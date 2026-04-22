package segment

import (
	"bytes"
	"errors"
)

var (
	ErrEmptySeparator    = errors.New("segment: empty block separator")
	ErrZeroBlockLength   = errors.New("segment: zero block length")
	ErrUnsortedIndex     = errors.New("segment: block index out of order")
	ErrOverlappingBlocks = errors.New("segment: block index has overlapping blocks")
)

// BlockIndexEntry maps a key range to a block inside a segment file.
//
// Separator is an inclusive upper bound for block keys. A point lookup finds
// the first entry whose separator is greater than or equal to the target key.
type BlockIndexEntry struct {
	Separator []byte
	Offset    uint64
	Length    uint32
}

// EndOffset returns the first byte after block payload.
func (e BlockIndexEntry) EndOffset() uint64 {
	return e.Offset + uint64(e.Length)
}

// Clone returns a detached copy safe for long-term ownership.
func (e BlockIndexEntry) Clone() BlockIndexEntry {
	return BlockIndexEntry{
		Separator: bytes.Clone(e.Separator),
		Offset:    e.Offset,
		Length:    e.Length,
	}
}

// Index is an in-memory block index for a single immutable segment.
type Index struct {
	entries []BlockIndexEntry
}

// NewIndex builds validated index from provided entries.
func NewIndex(entries []BlockIndexEntry) (Index, error) {
	idx := Index{
		entries: make([]BlockIndexEntry, 0, len(entries)),
	}
	for _, entry := range entries {
		if err := idx.Add(entry); err != nil {
			return Index{}, err
		}
	}
	return idx, nil
}

// Len returns number of block index entries.
func (idx *Index) Len() int {
	return len(idx.entries)
}

// Empty reports whether index has no entries.
func (idx *Index) Empty() bool {
	return len(idx.entries) == 0
}

// Reset clears index while preserving storage.
func (idx *Index) Reset() {
	idx.entries = idx.entries[:0]
}

// Entries returns read-only view over index entries.
func (idx *Index) Entries() []BlockIndexEntry {
	return idx.entries
}

// Entry returns entry by position.
func (idx *Index) Entry(i int) (BlockIndexEntry, bool) {
	if i < 0 || i >= len(idx.entries) {
		return BlockIndexEntry{}, false
	}
	return idx.entries[i], true
}

// Add appends validated block entry.
func (idx *Index) Add(entry BlockIndexEntry) error {
	if len(entry.Separator) == 0 {
		return ErrEmptySeparator
	}
	if entry.Length == 0 {
		return ErrZeroBlockLength
	}
	if n := len(idx.entries); n > 0 {
		prev := idx.entries[n-1]
		if bytes.Compare(prev.Separator, entry.Separator) > 0 {
			return ErrUnsortedIndex
		}
		if prev.EndOffset() > entry.Offset {
			return ErrOverlappingBlocks
		}
	}
	idx.entries = append(idx.entries, entry.Clone())
	return nil
}

// AddBlock appends block metadata to index.
func (idx *Index) AddBlock(separator []byte, offset uint64, length uint32) error {
	return idx.Add(BlockIndexEntry{
		Separator: separator,
		Offset:    offset,
		Length:    length,
	})
}

// FindBlock returns first block whose separator is greater than or equal to
// target key. Returned bool is false when key is beyond right edge of segment.
func (idx *Index) FindBlock(key []byte) (BlockIndexEntry, int, bool) {
	pos := idx.FindBlockIndex(key)
	if pos == -1 {
		return BlockIndexEntry{}, -1, false
	}
	return idx.entries[pos], pos, true
}

// FindBlockIndex returns index of first block whose separator is greater than
// or equal to target key. Returns -1 when no block can contain key.
func (idx *Index) FindBlockIndex(key []byte) int {
	lo := 0
	hi := len(idx.entries)

	for lo < hi {
		mid := (hi + lo) / 2
		if bytes.Compare(idx.entries[mid].Separator, key) < 0 {
			lo = mid + 1
			continue
		}
		hi = mid
	}

	if lo >= len(idx.entries) {
		return -1
	}
	return lo
}
