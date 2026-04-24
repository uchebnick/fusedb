package segment

import (
	"bytes"
	"errors"
)

var (
	ErrEmptyBlockKey        = errors.New("segment: empty block key")
	ErrUnsortedBlockEntries = errors.New("segment: block entries out of order")
)

// BlockEntry is one materialized key/value pair inside a segment block.
type BlockEntry struct {
	Key   []byte
	Value []byte
}

// Clone returns a detached copy safe for long-term ownership.
func (e BlockEntry) Clone() BlockEntry {
	return BlockEntry{
		Key:   bytes.Clone(e.Key),
		Value: bytes.Clone(e.Value),
	}
}

// Block is an immutable sorted collection of materialized entries.
type Block struct {
	entries []BlockEntry
}

// NewBlock builds validated block from ordered entries.
func NewBlock(entries []BlockEntry) (Block, error) {
	block := Block{
		entries: make([]BlockEntry, 0, len(entries)),
	}
	for _, entry := range entries {
		if err := block.Add(entry); err != nil {
			return Block{}, err
		}
	}
	return block, nil
}

// Len returns number of entries inside block.
func (b *Block) Len() int {
	return len(b.entries)
}

// Empty reports whether block has no entries.
func (b *Block) Empty() bool {
	return len(b.entries) == 0
}

// Entries returns read-only view over block entries.
func (b *Block) Entries() []BlockEntry {
	return b.entries
}

// Entry returns entry by position.
func (b *Block) Entry(i int) (BlockEntry, bool) {
	if i < 0 || i >= len(b.entries) {
		return BlockEntry{}, false
	}
	return b.entries[i], true
}

// Add appends one validated entry to block.
func (b *Block) Add(entry BlockEntry) error {
	if len(entry.Key) == 0 {
		return ErrEmptyBlockKey
	}
	if n := len(b.entries); n > 0 {
		prev := b.entries[n-1]
		if bytes.Compare(prev.Key, entry.Key) >= 0 {
			return ErrUnsortedBlockEntries
		}
	}
	b.entries = append(b.entries, entry.Clone())
	return nil
}

// AddKV appends one validated key/value pair to block.
func (b *Block) AddKV(key, value []byte) error {
	return b.Add(BlockEntry{Key: key, Value: value})
}

// Separator returns inclusive upper bound key for block.
func (b *Block) Separator() []byte {
	if len(b.entries) == 0 {
		return nil
	}
	return bytes.Clone(b.entries[len(b.entries)-1].Key)
}

// Find returns value for exact key lookup.
func (b *Block) Find(key []byte) ([]byte, bool) {
	lo := 0
	hi := len(b.entries)

	for lo < hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(b.entries[mid].Key, key)
		if cmp < 0 {
			lo = mid + 1
			continue
		}
		if cmp > 0 {
			hi = mid
			continue
		}
		return bytes.Clone(b.entries[mid].Value), true
	}
	return nil, false
}
