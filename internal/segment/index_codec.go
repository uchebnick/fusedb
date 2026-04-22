package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	indexEntryHeaderSize = 4 + 8 + 4
	indexHeaderSize      = 4
)

var (
	ErrIndexTooLarge        = errors.New("segment: index too large")
	ErrCorruptIndex         = errors.New("segment: corrupt index")
	ErrShortIndexBuffer     = errors.New("segment: short index buffer")
	ErrTrailingIndexBytes   = errors.New("segment: trailing index bytes")
	ErrSeparatorTooLarge    = errors.New("segment: separator too large")
	ErrEncodedIndexTooLarge = errors.New("segment: encoded index too large")
)

// EncodedLen returns encoded size of index in bytes.
func (idx *Index) EncodedLen() (int, error) {
	if len(idx.entries) > math.MaxUint32 {
		return 0, ErrIndexTooLarge
	}

	total := indexHeaderSize
	for _, entry := range idx.entries {
		if len(entry.Separator) > math.MaxUint32 {
			return 0, ErrSeparatorTooLarge
		}
		total += indexEntryHeaderSize + len(entry.Separator)
		if total < 0 {
			return 0, ErrEncodedIndexTooLarge
		}
	}
	return total, nil
}

// MarshalBinary encodes index into stable on-disk byte format.
func (idx *Index) MarshalBinary() ([]byte, error) {
	size, err := idx.EncodedLen()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(idx.entries)))

	pos := indexHeaderSize
	for _, entry := range idx.entries {
		sepLen := len(entry.Separator)

		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(sepLen))
		pos += 4

		copy(buf[pos:pos+sepLen], entry.Separator)
		pos += sepLen

		binary.LittleEndian.PutUint64(buf[pos:pos+8], entry.Offset)
		pos += 8

		binary.LittleEndian.PutUint32(buf[pos:pos+4], entry.Length)
		pos += 4
	}

	return buf, nil
}

// UnmarshalBinary decodes on-disk bytes into validated in-memory index.
func (idx *Index) UnmarshalBinary(data []byte) error {
	decoded, err := DecodeIndex(data)
	if err != nil {
		return err
	}
	*idx = decoded
	return nil
}

// EncodeIndex encodes index into stable on-disk byte format.
func EncodeIndex(idx Index) ([]byte, error) {
	return idx.MarshalBinary()
}

// DecodeIndex decodes stable on-disk bytes into validated index.
func DecodeIndex(data []byte) (Index, error) {
	if len(data) < indexHeaderSize {
		return Index{}, fmt.Errorf("%w: missing entry count", ErrShortIndexBuffer)
	}

	count := int(binary.LittleEndian.Uint32(data[:4]))
	pos := indexHeaderSize
	entries := make([]BlockIndexEntry, 0, count)

	for i := 0; i < count; i++ {
		if len(data)-pos < 4 {
			return Index{}, fmt.Errorf("%w: entry %d missing separator length", ErrShortIndexBuffer, i)
		}
		sepLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if len(data)-pos < sepLen {
			return Index{}, fmt.Errorf("%w: entry %d truncated separator", ErrShortIndexBuffer, i)
		}
		separator := append([]byte(nil), data[pos:pos+sepLen]...)
		pos += sepLen

		if len(data)-pos < 8 {
			return Index{}, fmt.Errorf("%w: entry %d missing offset", ErrShortIndexBuffer, i)
		}
		offset := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8

		if len(data)-pos < 4 {
			return Index{}, fmt.Errorf("%w: entry %d missing length", ErrShortIndexBuffer, i)
		}
		length := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		entries = append(entries, BlockIndexEntry{
			Separator: separator,
			Offset:    offset,
			Length:    length,
		})
	}

	if pos != len(data) {
		return Index{}, fmt.Errorf("%w: %d extra bytes", ErrTrailingIndexBytes, len(data)-pos)
	}

	idx, err := NewIndex(entries)
	if err != nil {
		return Index{}, fmt.Errorf("%w: %v", ErrCorruptIndex, err)
	}
	return idx, nil
}
