package segment

import (
	"bytes"
	"encoding/binary"
)

// BlockView is a zero-copy random-access view over one encoded block.
//
// It keeps encoded block bytes and uses the block offset table to decode only
// entries that are needed by lookup or iteration.
type BlockView struct {
	data        []byte
	entryCount  int
	offsetsPos  int
	checksumPos int
}

// Len returns number of entries inside encoded block view.
func (v BlockView) Len() int {
	return v.entryCount
}

// Entry decodes one entry from encoded block view by position.
func (v BlockView) Entry(i int) (BlockEntry, error) {
	if i < 0 || i >= v.entryCount {
		return BlockEntry{}, ErrBlockIndexRange
	}

	offset := int(binary.LittleEndian.Uint32(v.data[v.offsetsPos+i*4 : v.offsetsPos+(i+1)*4]))
	next := v.offsetsPos
	if i+1 < v.entryCount {
		next = int(binary.LittleEndian.Uint32(v.data[v.offsetsPos+(i+1)*4 : v.offsetsPos+(i+2)*4]))
	}
	return decodeBlockEntryAt(v.data, offset, next, v.offsetsPos)
}

// Find returns value for exact key lookup without decoding the whole block.
//
// Returned value borrows memory from the encoded block bytes. Callers that need
// ownership must clone it before keeping it after the block payload is released.
func (v BlockView) Find(key []byte) ([]byte, bool, error) {
	lo := 0
	hi := v.entryCount

	for lo < hi {
		mid := (lo + hi) / 2
		entry, err := v.Entry(mid)
		if err != nil {
			return nil, false, err
		}

		cmp := bytes.Compare(entry.Key, key)
		if cmp < 0 {
			lo = mid + 1
			continue
		}
		if cmp > 0 {
			hi = mid
			continue
		}
		return entry.Value, true, nil
	}
	return nil, false, nil
}
