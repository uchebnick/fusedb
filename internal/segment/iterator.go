package segment

import "bytes"

// Iterator walks materialized entries in one frozen segment in key order.
type Iterator struct {
	reader     *Reader
	blockIndex int
	entryIndex int
	block      Block
	valid      bool
	closed     bool
	err        error
}

// NewIterator creates an ordered iterator over this reader's segment.
func (r *Reader) NewIterator() *Iterator {
	it := &Iterator{
		reader:     r,
		blockIndex: -1,
		entryIndex: -1,
	}
	if r == nil || r.segment == nil {
		it.err = ErrNilSegment
	}
	return it
}

// First positions iterator at the first key.
func (it *Iterator) First() bool {
	if !it.ready() {
		return false
	}
	return it.loadBlock(0, 0)
}

// Seek positions iterator at the first key greater than or equal to target.
func (it *Iterator) Seek(target []byte) bool {
	if !it.ready() {
		return false
	}

	blockIndex := it.reader.segment.Index.FindBlockIndex(target)
	if blockIndex == -1 {
		it.invalidate()
		return false
	}

	entry, ok := it.reader.segment.Index.Entry(blockIndex)
	if !ok {
		it.invalidate()
		return false
	}

	block, err := it.reader.readBlock(entry)
	if err != nil {
		it.err = err
		it.invalidate()
		return false
	}

	entryIndex := blockLowerBound(block, target)
	if entryIndex >= block.Len() {
		return it.loadBlock(blockIndex+1, 0)
	}

	it.block = block
	it.blockIndex = blockIndex
	it.entryIndex = entryIndex
	it.valid = true
	return true
}

// Next moves iterator to the next key.
func (it *Iterator) Next() bool {
	if !it.ready() || !it.valid {
		return false
	}
	nextEntry := it.entryIndex + 1
	if nextEntry < it.block.Len() {
		it.entryIndex = nextEntry
		return true
	}
	return it.loadBlock(it.blockIndex+1, 0)
}

// Valid reports whether iterator currently points at an entry.
func (it *Iterator) Valid() bool {
	return it != nil && it.valid && it.err == nil && !it.closed
}

// Key returns detached current key.
func (it *Iterator) Key() []byte {
	entry, ok := it.currentEntry()
	if !ok {
		return nil
	}
	return bytes.Clone(entry.Key)
}

// Value returns detached current value.
func (it *Iterator) Value() []byte {
	entry, ok := it.currentEntry()
	if !ok {
		return nil
	}
	return bytes.Clone(entry.Value)
}

// Entry returns detached current key/value pair.
func (it *Iterator) Entry() (BlockEntry, bool) {
	entry, ok := it.currentEntry()
	if !ok {
		return BlockEntry{}, false
	}
	return entry.Clone(), true
}

// Err returns first IO/decode error observed by iterator.
func (it *Iterator) Err() error {
	if it == nil {
		return nil
	}
	return it.err
}

// Close releases iterator state.
func (it *Iterator) Close() error {
	if it == nil {
		return nil
	}
	it.closed = true
	it.invalidate()
	it.reader = nil
	return nil
}

func (it *Iterator) ready() bool {
	if it == nil || it.closed || it.err != nil {
		return false
	}
	if it.reader == nil || it.reader.segment == nil {
		it.err = ErrNilSegment
		return false
	}
	return true
}

func (it *Iterator) loadBlock(blockIndex, entryIndex int) bool {
	for blockIndex < it.reader.segment.Index.Len() {
		entry, ok := it.reader.segment.Index.Entry(blockIndex)
		if !ok {
			it.invalidate()
			return false
		}

		block, err := it.reader.readBlock(entry)
		if err != nil {
			it.err = err
			it.invalidate()
			return false
		}
		if entryIndex < block.Len() {
			it.block = block
			it.blockIndex = blockIndex
			it.entryIndex = entryIndex
			it.valid = true
			return true
		}

		blockIndex++
		entryIndex = 0
	}

	it.invalidate()
	return false
}

func (it *Iterator) currentEntry() (BlockEntry, bool) {
	if !it.Valid() {
		return BlockEntry{}, false
	}
	return it.block.Entry(it.entryIndex)
}

func (it *Iterator) invalidate() {
	it.valid = false
	it.blockIndex = -1
	it.entryIndex = -1
	it.block = Block{}
}

func blockLowerBound(block Block, target []byte) int {
	lo := 0
	hi := block.Len()
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(block.entries[mid].Key, target) < 0 {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	return lo
}
