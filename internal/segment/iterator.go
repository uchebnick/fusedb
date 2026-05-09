package segment

import (
	"bytes"
	"iter"
)

// Iter returns an ordered stream over segment entries.
//
// Yielded key/value slices are borrowed from decoded segment blocks. Callers
// must not mutate or retain them after yield returns.
func (r *Reader) Iter() iter.Seq2[[]byte, []byte] {
	return r.IterFrom(nil)
}

// IterFrom returns an ordered stream starting at the first key >= target.
//
// Passing nil starts from the first key in the segment. Yielded key/value
// slices follow the same borrowed-slice contract as Iter.
func (r *Reader) IterFrom(target []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		if r == nil || r.segment == nil || r.closed {
			return
		}

		blockIndex := 0
		seek := target != nil
		if seek {
			blockIndex = r.segment.Index.FindBlockIndex(target)
			if blockIndex == -1 {
				return
			}
		}

		for blockIndex < r.segment.Index.Len() {
			indexEntry, ok := r.segment.Index.Entry(blockIndex)
			if !ok {
				return
			}

			block, err := r.readBlock(indexEntry)
			if err != nil {
				return
			}

			entryIndex := 0
			if seek {
				entryIndex = blockLowerBound(block, target)
				seek = false
			}

			for entryIndex < block.Len() {
				entry, ok := block.Entry(entryIndex)
				if !ok {
					return
				}
				if !yield(entry.Key, entry.Value) {
					return
				}
				entryIndex++
			}

			blockIndex++
		}
	}
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
