package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/cespare/xxhash/v2"
)

const (
	bloomFilterMagic         = "FBLM"
	bloomFilterVersion       = 1
	bloomFilterHeaderSize    = 4 + 4 + 4 + 4 + 4
	bloomFilterChecksumSize  = 4
	minBloomFilterBits       = 64
	maxBloomFilterHashes     = 30
	defaultFalsePositiveRate = 0.01
)

var (
	ErrInvalidBloomFilterRate   = errors.New("segment: invalid bloom filter false positive rate")
	ErrTooManyBloomFilterKeys   = errors.New("segment: too many bloom filter keys")
	ErrShortBloomFilterBuffer   = errors.New("segment: short bloom filter buffer")
	ErrBloomFilterMagicMismatch = errors.New("segment: bloom filter magic mismatch")
	ErrUnsupportedBloomVersion  = errors.New("segment: unsupported bloom filter version")
	ErrCorruptBloomFilterData   = errors.New("segment: corrupt bloom filter data")
	ErrBloomFilterChecksum      = errors.New("segment: bloom filter checksum mismatch")
	ErrTrailingBloomFilterBytes = errors.New("segment: trailing bloom filter bytes")
)

// BloomFilter is a serialized-friendly probabilistic membership filter.
//
// It is suitable for per-block or per-segment usage. The current expected use in
// FuseDB is per-block negative lookup filtering.
type BloomFilter struct {
	numBits   uint32
	numHashes uint8
	bits      []byte
}

// NewBloomFilter allocates a bloom filter for expected key count and target
// false positive rate.
func NewBloomFilter(expectedKeys int, falsePositiveRate float64) (BloomFilter, error) {
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return BloomFilter{}, ErrInvalidBloomFilterRate
	}
	if expectedKeys < 0 || expectedKeys > math.MaxInt32 {
		return BloomFilter{}, ErrTooManyBloomFilterKeys
	}
	if expectedKeys == 0 {
		return BloomFilter{
			numBits:   minBloomFilterBits,
			numHashes: 1,
			bits:      make([]byte, minBloomFilterBits/8),
		}, nil
	}

	n := float64(expectedKeys)
	m := -n * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)
	if m < minBloomFilterBits {
		m = minBloomFilterBits
	}

	numBits := uint32(math.Ceil(m))
	byteLen := int((numBits + 7) / 8)
	numBits = uint32(byteLen * 8)

	k := int(math.Round((m / n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > maxBloomFilterHashes {
		k = maxBloomFilterHashes
	}

	return BloomFilter{
		numBits:   numBits,
		numHashes: uint8(k),
		bits:      make([]byte, byteLen),
	}, nil
}

// BuildBloomFilter constructs a bloom filter from a set of keys.
func BuildBloomFilter(keys [][]byte, falsePositiveRate float64) (BloomFilter, error) {
	if falsePositiveRate == 0 {
		falsePositiveRate = defaultFalsePositiveRate
	}

	filter, err := NewBloomFilter(len(keys), falsePositiveRate)
	if err != nil {
		return BloomFilter{}, err
	}
	for _, key := range keys {
		filter.Add(key)
	}
	return filter, nil
}

// BuildBloomFilterForBlock constructs a bloom filter over block keys.
func BuildBloomFilterForBlock(block Block, falsePositiveRate float64) (BloomFilter, error) {
	keys := make([][]byte, 0, block.Len())
	for _, entry := range block.Entries() {
		keys = append(keys, entry.Key)
	}
	return BuildBloomFilter(keys, falsePositiveRate)
}

// NumBits returns total number of addressable bits.
func (f *BloomFilter) NumBits() uint32 {
	if f == nil {
		return 0
	}
	return f.numBits
}

// NumHashes returns number of hash rounds.
func (f *BloomFilter) NumHashes() uint8 {
	if f == nil {
		return 0
	}
	return f.numHashes
}

// Empty reports whether filter has no bitset.
func (f *BloomFilter) Empty() bool {
	return f == nil || len(f.bits) == 0
}

// Add inserts one key into filter.
func (f *BloomFilter) Add(key []byte) {
	if f == nil || f.numBits == 0 || f.numHashes == 0 {
		return
	}
	h1, h2 := bloomHashes(key)
	mod := uint64(f.numBits)

	for i := uint8(0); i < f.numHashes; i++ {
		bit := (h1 + uint64(i)*h2) % mod
		byteIndex := bit / 8
		bitMask := byte(1 << (bit % 8))
		f.bits[byteIndex] |= bitMask
	}
}

// MayContain reports whether key may be present.
func (f *BloomFilter) MayContain(key []byte) bool {
	if f == nil || f.numBits == 0 || f.numHashes == 0 {
		return false
	}
	h1, h2 := bloomHashes(key)
	mod := uint64(f.numBits)

	for i := uint8(0); i < f.numHashes; i++ {
		bit := (h1 + uint64(i)*h2) % mod
		byteIndex := bit / 8
		bitMask := byte(1 << (bit % 8))
		if f.bits[byteIndex]&bitMask == 0 {
			return false
		}
	}
	return true
}

// MarshalBinary encodes bloom filter into stable on-disk bytes.
func (f *BloomFilter) MarshalBinary() ([]byte, error) {
	if f == nil {
		return nil, nil
	}
	if len(f.bits) > math.MaxUint32 {
		return nil, ErrCorruptBloomFilterData
	}
	if f.numBits != uint32(len(f.bits)*8) {
		return nil, ErrCorruptBloomFilterData
	}

	total := bloomFilterHeaderSize + len(f.bits) + bloomFilterChecksumSize
	buf := make([]byte, total)
	copy(buf[:4], bloomFilterMagic)
	binary.LittleEndian.PutUint32(buf[4:8], bloomFilterVersion)
	binary.LittleEndian.PutUint32(buf[8:12], f.numBits)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(f.numHashes))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(f.bits)))
	copy(buf[bloomFilterHeaderSize:], f.bits)

	checksumPos := bloomFilterHeaderSize + len(f.bits)
	checksum := crc32.ChecksumIEEE(buf[:checksumPos])
	binary.LittleEndian.PutUint32(buf[checksumPos:], checksum)
	return buf, nil
}

// UnmarshalBinary decodes bloom filter from stable on-disk bytes.
func (f *BloomFilter) UnmarshalBinary(data []byte) error {
	decoded, err := DecodeBloomFilter(data)
	if err != nil {
		return err
	}
	*f = decoded
	return nil
}

// EncodeBloomFilter encodes bloom filter into bytes.
func EncodeBloomFilter(filter BloomFilter) ([]byte, error) {
	return filter.MarshalBinary()
}

// DecodeBloomFilter decodes bloom filter from bytes.
func DecodeBloomFilter(data []byte) (BloomFilter, error) {
	if len(data) < bloomFilterHeaderSize+bloomFilterChecksumSize {
		return BloomFilter{}, ErrShortBloomFilterBuffer
	}
	if string(data[:4]) != bloomFilterMagic {
		return BloomFilter{}, ErrBloomFilterMagicMismatch
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version != bloomFilterVersion {
		return BloomFilter{}, fmt.Errorf("%w: %d", ErrUnsupportedBloomVersion, version)
	}

	numBits := binary.LittleEndian.Uint32(data[8:12])
	numHashes := uint8(binary.LittleEndian.Uint32(data[12:16]))
	bitsetLen := int(binary.LittleEndian.Uint32(data[16:20]))

	if bitsetLen <= 0 {
		return BloomFilter{}, ErrCorruptBloomFilterData
	}
	if len(data) < bloomFilterHeaderSize+bitsetLen+bloomFilterChecksumSize {
		return BloomFilter{}, ErrShortBloomFilterBuffer
	}
	if len(data) != bloomFilterHeaderSize+bitsetLen+bloomFilterChecksumSize {
		return BloomFilter{}, ErrTrailingBloomFilterBytes
	}
	if numBits != uint32(bitsetLen*8) || numHashes == 0 {
		return BloomFilter{}, ErrCorruptBloomFilterData
	}

	checksumPos := bloomFilterHeaderSize + bitsetLen
	wantChecksum := binary.LittleEndian.Uint32(data[checksumPos:])
	gotChecksum := crc32.ChecksumIEEE(data[:checksumPos])
	if gotChecksum != wantChecksum {
		return BloomFilter{}, ErrBloomFilterChecksum
	}

	filter := BloomFilter{
		numBits:   numBits,
		numHashes: numHashes,
		bits:      append([]byte(nil), data[bloomFilterHeaderSize:checksumPos]...),
	}
	return filter, nil
}

func bloomHashes(key []byte) (uint64, uint64) {
	h1 := xxhash.Sum64(key)
	h2 := splitmix64(h1 ^ 0x9e3779b97f4a7c15)
	if h2 == 0 {
		h2 = 0x9e3779b97f4a7c15
	}
	return h1, h2
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
