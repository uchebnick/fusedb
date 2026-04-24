package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	rawBlockMagic           = "FBLK"
	rawBlockVersion         = 1
	rawBlockHeaderSize      = 4 + 4 + 4
	rawBlockEntryHeaderSize = 4 + 4
	rawBlockChecksumSize    = 4
)

var (
	ErrTooManyBlockEntries     = errors.New("segment: too many block entries")
	ErrBlockTooLarge           = errors.New("segment: block too large")
	ErrShortBlockBuffer        = errors.New("segment: short block buffer")
	ErrBlockMagicMismatch      = errors.New("segment: block magic mismatch")
	ErrUnsupportedBlockVersion = errors.New("segment: unsupported block version")
	ErrBlockChecksumMismatch   = errors.New("segment: block checksum mismatch")
	ErrCorruptBlockOffsets     = errors.New("segment: corrupt block offsets")
	ErrBlockIndexRange         = errors.New("segment: block range out of bounds")
)

// EncodedLen returns encoded size of raw block.
func (b *Block) EncodedLen() (int, error) {
	if len(b.entries) > math.MaxUint32 {
		return 0, ErrTooManyBlockEntries
	}

	total := rawBlockHeaderSize + rawBlockChecksumSize + len(b.entries)*4
	for _, entry := range b.entries {
		if len(entry.Key) > math.MaxUint32 || len(entry.Value) > math.MaxUint32 {
			return 0, ErrBlockTooLarge
		}
		total += rawBlockEntryHeaderSize + len(entry.Key) + len(entry.Value)
		if total < 0 {
			return 0, ErrBlockTooLarge
		}
	}
	return total, nil
}

// MarshalBinary encodes one raw block.
func (b *Block) MarshalBinary() ([]byte, error) {
	size, err := b.EncodedLen()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	copy(buf[:4], rawBlockMagic)
	binary.LittleEndian.PutUint32(buf[4:8], rawBlockVersion)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(b.entries)))

	pos := rawBlockHeaderSize
	offsets := make([]uint32, 0, len(b.entries))
	for _, entry := range b.entries {
		offsets = append(offsets, uint32(pos))

		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(entry.Key)))
		pos += 4
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(entry.Value)))
		pos += 4

		copy(buf[pos:pos+len(entry.Key)], entry.Key)
		pos += len(entry.Key)
		copy(buf[pos:pos+len(entry.Value)], entry.Value)
		pos += len(entry.Value)
	}

	for _, offset := range offsets {
		binary.LittleEndian.PutUint32(buf[pos:pos+4], offset)
		pos += 4
	}

	checksum := crc32.ChecksumIEEE(buf[:pos])
	binary.LittleEndian.PutUint32(buf[pos:pos+4], checksum)
	return buf, nil
}

// UnmarshalBinary decodes one raw block.
func (b *Block) UnmarshalBinary(data []byte) error {
	decoded, err := DecodeBlock(data)
	if err != nil {
		return err
	}
	*b = decoded
	return nil
}

// EncodeBlock encodes one raw block.
func EncodeBlock(block Block) ([]byte, error) {
	return block.MarshalBinary()
}

// DecodeBlock decodes one raw block.
func DecodeBlock(data []byte) (Block, error) {
	if len(data) < rawBlockHeaderSize+rawBlockChecksumSize {
		return Block{}, ErrShortBlockBuffer
	}
	if string(data[:4]) != rawBlockMagic {
		return Block{}, ErrBlockMagicMismatch
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version != rawBlockVersion {
		return Block{}, fmt.Errorf("%w: %d", ErrUnsupportedBlockVersion, version)
	}

	entryCount := int(binary.LittleEndian.Uint32(data[8:12]))
	offsetsSize := entryCount * 4
	if len(data) < rawBlockHeaderSize+offsetsSize+rawBlockChecksumSize {
		return Block{}, ErrShortBlockBuffer
	}

	checksumPos := len(data) - rawBlockChecksumSize
	offsetsPos := checksumPos - offsetsSize
	if offsetsPos < rawBlockHeaderSize {
		return Block{}, ErrCorruptBlockOffsets
	}

	wantChecksum := binary.LittleEndian.Uint32(data[checksumPos:])
	gotChecksum := crc32.ChecksumIEEE(data[:checksumPos])
	if gotChecksum != wantChecksum {
		return Block{}, ErrBlockChecksumMismatch
	}

	block := Block{
		entries: make([]BlockEntry, 0, entryCount),
	}
	for i := 0; i < entryCount; i++ {
		offset := int(binary.LittleEndian.Uint32(data[offsetsPos+i*4 : offsetsPos+(i+1)*4]))
		next := offsetsPos
		if i+1 < entryCount {
			next = int(binary.LittleEndian.Uint32(data[offsetsPos+(i+1)*4 : offsetsPos+(i+2)*4]))
		}

		if offset < rawBlockHeaderSize || offset >= offsetsPos || next <= offset || next > offsetsPos {
			return Block{}, ErrCorruptBlockOffsets
		}
		if next-offset < rawBlockEntryHeaderSize {
			return Block{}, ErrShortBlockBuffer
		}

		pos := offset
		keyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		valueLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+keyLen+valueLen != next {
			return Block{}, ErrShortBlockBuffer
		}

		key := append([]byte(nil), data[pos:pos+keyLen]...)
		pos += keyLen
		value := append([]byte(nil), data[pos:pos+valueLen]...)

		if err := block.Add(BlockEntry{Key: key, Value: value}); err != nil {
			return Block{}, err
		}
	}

	return block, nil
}

// EncodeBlocks packs many raw blocks into one byte blob and returns an index
// that points at each block by separator.
func EncodeBlocks(blocks []Block) ([]byte, Index, error) {
	idx := Index{
		entries: make([]BlockIndexEntry, 0, len(blocks)),
	}
	out := make([]byte, 0)

	for _, block := range blocks {
		if block.Empty() {
			return nil, Index{}, ErrEmptySeparator
		}

		offset := uint64(len(out))
		encoded, err := block.MarshalBinary()
		if err != nil {
			return nil, Index{}, err
		}

		out = append(out, encoded...)
		if err := idx.AddBlock(block.Separator(), offset, uint32(len(encoded))); err != nil {
			return nil, Index{}, err
		}
	}

	return out, idx, nil
}

// DecodeIndexedBlock decodes one raw block slice referenced by index entry.
func DecodeIndexedBlock(data []byte, entry BlockIndexEntry) (Block, error) {
	start := int(entry.Offset)
	end := start + int(entry.Length)
	if start < 0 || end > len(data) || start > end {
		return Block{}, ErrBlockIndexRange
	}
	return DecodeBlock(data[start:end])
}
