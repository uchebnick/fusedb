package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

// On-disk layout, little endian throughout:
//
//	header (8 bytes)
//	  0..4    magic "FMAN"
//	  4..8    format version uint32
//	body (variable, checksummed)
//	  0..8    NextSegmentID uint64
//	  8..16   AppliedSeq    uint64
//	  16..20  leaf count    uint32
//	  then one record per leaf, in stored (sorted) order:
//	    0..8    LeafID         uint64
//	    8..16   SegmentID      uint64
//	    16..24  SegmentVersion uint64
//	    24..32  Keys           uint64
//	    32..36  low key length uint32
//	    36..    low key bytes
//	trailer (4 bytes)
//	  crc32 (IEEE) over the body only
//
// Low keys are length prefixed rather than delimited, so empty keys and keys
// containing arbitrary bytes, including NUL, round-trip unchanged.
const (
	manifestMagic        = "FMAN"
	manifestVersion      = 2
	manifestHeaderSize   = 4 + 4
	manifestChecksumSize = 4
	manifestBodyHeadSize = 8 + 8 + 4
	leafRecordFixedSize  = 8 + 8 + 8 + 8 + 4
)

var (
	// ErrShortManifestBuffer reports that the encoded manifest ended before a
	// field it declared could be read.
	ErrShortManifestBuffer = errors.New("manifest: short manifest buffer")
	// ErrManifestMagicMismatch reports a file that does not start with the
	// manifest magic, meaning it was never a manifest.
	ErrManifestMagicMismatch = errors.New("manifest: magic mismatch")

	// ErrUnsupportedVersion reports a manifest written by a newer format.
	ErrUnsupportedVersion = errors.New("manifest: unsupported manifest version")

	// ErrChecksumMismatch reports a manifest whose body does not match its
	// recorded checksum. The manifest is written whole and atomically, so this
	// is corruption rather than a torn write.
	ErrChecksumMismatch = errors.New("manifest: checksum mismatch")

	// ErrCorruptManifestData reports a manifest whose fields are internally
	// inconsistent even though the checksum matched.
	ErrCorruptManifestData = errors.New("manifest: corrupt manifest data")

	// ErrTrailingManifestBytes reports bytes left over after the declared leaf
	// records were decoded.
	ErrTrailingManifestBytes = errors.New("manifest: trailing manifest bytes")

	// ErrTooManyLeaves reports a leaf count that cannot be encoded.
	ErrTooManyLeaves = errors.New("manifest: too many leaves")

	// ErrLowKeyTooLarge reports a low key whose length cannot be encoded.
	ErrLowKeyTooLarge = errors.New("manifest: leaf low key too large")
)

// MarshalBinary encodes the manifest into stable on-disk bytes.
func (m *Manifest) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, ErrNilManifest
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if len(m.Leaves) > math.MaxUint32 {
		return nil, ErrTooManyLeaves
	}

	size := manifestHeaderSize + manifestBodyHeadSize + manifestChecksumSize
	for i := range m.Leaves {
		if len(m.Leaves[i].LowKey) > math.MaxUint32 {
			return nil, ErrLowKeyTooLarge
		}
		size += leafRecordFixedSize + len(m.Leaves[i].LowKey)
	}

	buf := make([]byte, size)
	copy(buf[:4], manifestMagic)
	binary.LittleEndian.PutUint32(buf[4:8], manifestVersion)

	pos := manifestHeaderSize
	binary.LittleEndian.PutUint64(buf[pos:pos+8], m.NextSegmentID)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], m.AppliedSeq)
	pos += 8
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(m.Leaves)))
	pos += 4

	for i := range m.Leaves {
		leaf := &m.Leaves[i]
		binary.LittleEndian.PutUint64(buf[pos:pos+8], leaf.LeafID)
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:pos+8], leaf.SegmentID)
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:pos+8], leaf.SegmentVersion)
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:pos+8], leaf.Keys)
		pos += 8
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(leaf.LowKey)))
		pos += 4
		pos += copy(buf[pos:], leaf.LowKey)
	}

	checksum := crc32.ChecksumIEEE(buf[manifestHeaderSize:pos])
	binary.LittleEndian.PutUint32(buf[pos:pos+4], checksum)
	return buf, nil
}

// UnmarshalBinary decodes the manifest from stable on-disk bytes.
func (m *Manifest) UnmarshalBinary(data []byte) error {
	decoded, err := DecodeManifest(data)
	if err != nil {
		return err
	}
	*m = *decoded
	return nil
}

// EncodeManifest encodes the manifest into bytes.
func EncodeManifest(m *Manifest) ([]byte, error) {
	return m.MarshalBinary()
}

// DecodeManifest decodes and validates a manifest from bytes.
func DecodeManifest(data []byte) (*Manifest, error) {
	if len(data) < manifestHeaderSize+manifestBodyHeadSize+manifestChecksumSize {
		return nil, ErrShortManifestBuffer
	}
	if string(data[:4]) != manifestMagic {
		return nil, ErrManifestMagicMismatch
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version != manifestVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	checksumPos := len(data) - manifestChecksumSize
	wantChecksum := binary.LittleEndian.Uint32(data[checksumPos:])
	gotChecksum := crc32.ChecksumIEEE(data[manifestHeaderSize:checksumPos])
	if wantChecksum != gotChecksum {
		return nil, ErrChecksumMismatch
	}

	pos := manifestHeaderSize
	m := &Manifest{
		NextSegmentID: binary.LittleEndian.Uint64(data[pos : pos+8]),
		AppliedSeq:    binary.LittleEndian.Uint64(data[pos+8 : pos+16]),
	}
	leafCount := int(binary.LittleEndian.Uint32(data[pos+16 : pos+20]))
	pos += manifestBodyHeadSize

	// Reject impossible counts before allocating for them.
	if leafCount < 0 || leafCount > (checksumPos-pos)/leafRecordFixedSize {
		return nil, ErrCorruptManifestData
	}
	if leafCount > 0 {
		m.Leaves = make([]LeafRecord, leafCount)
	}

	for i := 0; i < leafCount; i++ {
		if checksumPos-pos < leafRecordFixedSize {
			return nil, ErrCorruptManifestData
		}
		leaf := &m.Leaves[i]
		leaf.LeafID = binary.LittleEndian.Uint64(data[pos : pos+8])
		leaf.SegmentID = binary.LittleEndian.Uint64(data[pos+8 : pos+16])
		leaf.SegmentVersion = binary.LittleEndian.Uint64(data[pos+16 : pos+24])
		leaf.Keys = binary.LittleEndian.Uint64(data[pos+24 : pos+32])
		keyLen := int(binary.LittleEndian.Uint32(data[pos+32 : pos+36]))
		pos += leafRecordFixedSize

		if keyLen < 0 || keyLen > checksumPos-pos {
			return nil, ErrCorruptManifestData
		}
		if keyLen > 0 {
			leaf.LowKey = make([]byte, keyLen)
			copy(leaf.LowKey, data[pos:pos+keyLen])
		}
		pos += keyLen
	}

	if pos != checksumPos {
		return nil, ErrTrailingManifestBytes
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}
