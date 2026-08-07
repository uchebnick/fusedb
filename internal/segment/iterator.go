package segment

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
)

// ErrCorruptSegmentIndex reports an index entry that does not resolve to a
// readable block.
var ErrCorruptSegmentIndex = errors.New("segment: corrupt segment index entry")

// Iter returns an ordered stream over segment entries.
//
// Yielded key/value slices are borrowed from decoded segment blocks. Callers
// must not mutate or retain them after yield returns.
//
// Iter silently stops on a read error. Merge paths, where a truncated stream
// would silently drop keys, must use IterWithErr instead.
func (r *Reader) Iter() iter.Seq2[[]byte, []byte] {
	return r.IterFrom(nil)
}

// IterFrom returns an ordered stream starting at the first key >= target.
//
// Passing nil starts from the first key in the segment. Yielded key/value
// slices follow the same borrowed-slice contract as Iter.
func (r *Reader) IterFrom(target []byte) iter.Seq2[[]byte, []byte] {
	seq, _ := r.IterFromWithErr(target)
	return seq
}

// IterWithErr returns an ordered stream plus an error accessor.
//
// The accessor must be consulted after the range loop finishes. A non-nil
// error means the stream stopped early and the yielded entries are only a
// prefix of the segment, which callers that rewrite segments must treat as a
// failure rather than as end of data.
func (r *Reader) IterWithErr() (iter.Seq2[[]byte, []byte], func() error) {
	return r.IterFromWithErr(nil)
}

// IterFromWithErr returns a seeking ordered stream plus an error accessor.
//
// See IterWithErr for the error contract.
func (r *Reader) IterFromWithErr(target []byte) (iter.Seq2[[]byte, []byte], func() error) {
	var iterErr error

	seq := func(yield func([]byte, []byte) bool) {
		if r == nil || r.segment == nil || r.closed.Load() {
			iterErr = ErrNilSegment
			return
		}

		blockIndex := 0
		seek := target != nil
		if seek {
			blockIndex = r.segment.Index.FindBlockIndex(target)
			if blockIndex == -1 {
				// Target sorts past the last block: an empty stream is the
				// correct answer, not a failure.
				return
			}
		}

		for blockIndex < r.segment.Index.Len() {
			indexEntry, ok := r.segment.Index.Entry(blockIndex)
			if !ok {
				iterErr = fmt.Errorf("%w: block %d", ErrCorruptSegmentIndex, blockIndex)
				return
			}

			block, err := r.readBlock(indexEntry)
			if err != nil {
				iterErr = fmt.Errorf("segment: read block %d: %w", blockIndex, err)
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
					iterErr = fmt.Errorf("%w: block %d entry %d", ErrCorruptSegmentIndex, blockIndex, entryIndex)
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

	return seq, func() error { return iterErr }
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
