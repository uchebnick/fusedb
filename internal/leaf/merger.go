package leaf

import (
	"bytes"
	"iter"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/value"
)

type Merger struct {
	TargetBlockSize    int
	BloomFalsePositive float64
	Compression        *compression.Registry
}

func (m *Merger) resolve(segmentValue []byte, op *ops.Op) ([]byte, bool) {
	if op == nil {
		return segmentValue, true
	}

	switch op.Kind {
	case ops.OpDelete:
		return nil, false
	case ops.OpPut:
		return op.Data, true
	case ops.OpInc:
		if len(segmentValue) == 0 {
			return value.EncodeInt64(ops.DecodeInc(*op)), true
		}
		old, err := value.DecodeInt64(segmentValue)
		if err != nil {
			return nil, false
		}

		return value.EncodeInt64(old + ops.DecodeInc(*op)), true
	default:
		return segmentValue, true
	}
}

func (m *Merger) mergeIter(
	segmentIter iter.Seq2[[]byte, []byte],
	skipListIter iter.Seq2[[]byte, ops.Op],
) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		segmentNext, segmentStop := iter.Pull2(segmentIter)
		defer segmentStop()

		skipNext, skipStop := iter.Pull2(skipListIter)
		defer skipStop()

		segmentKey, segmentValue, segmentOk := segmentNext()
		skipKey, skipOp, skipOk := skipNext()

		for segmentOk || skipOk {
			switch {
			case !segmentOk:
				value, exists := m.resolve(nil, &skipOp)
				if exists && !yield(skipKey, value) {
					return
				}
				skipKey, skipOp, skipOk = skipNext()

			case !skipOk:
				if !yield(segmentKey, segmentValue) {
					return
				}
				segmentKey, segmentValue, segmentOk = segmentNext()

			default:
				cmp := bytes.Compare(segmentKey, skipKey)

				switch {
				case cmp < 0:
					if !yield(segmentKey, segmentValue) {
						return
					}
					segmentKey, segmentValue, segmentOk = segmentNext()

				case cmp == 0:
					value, exists := m.resolve(segmentValue, &skipOp)
					if exists && !yield(segmentKey, value) {
						return
					}
					segmentKey, segmentValue, segmentOk = segmentNext()
					skipKey, skipOp, skipOk = skipNext()

				case cmp > 0:
					value, exists := m.resolve(nil, &skipOp)
					if exists && !yield(skipKey, value) {
						return
					}
					skipKey, skipOp, skipOk = skipNext()
				}
			}
		}
	}
}

func (m *Merger) merge(
	segmentIter iter.Seq2[[]byte, []byte],
	opsIter iter.Seq2[[]byte, ops.Op],
	opts segment.Options,
) (*segment.Segment, error) {
	newSegment, err := segment.NewSegment(opts)
	if err != nil {
		return nil, err
	}

	for key, value := range m.mergeIter(segmentIter, opsIter) {
		if err := newSegment.AppendKVUnsafe(key, value); err != nil {
			_ = newSegment.Abort()
			return nil, err
		}
	}

	if err := newSegment.Freeze(); err != nil {
		_ = newSegment.Abort()
		return nil, err
	}

	return newSegment, nil
}
