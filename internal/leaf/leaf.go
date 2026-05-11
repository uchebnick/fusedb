package leaf

import (
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/value"
)

// Leaf owns one key-range local state: mutable buffer plus immutable segment.
type Leaf struct {
	id   uint64
	seed uint64

	buffer *Buffer
	reader atomic.Pointer[segment.Reader]

	merger *Merger

	retired         chan retiredReader
	readerCloseStop chan struct{}
	readerCloseDone chan struct{}
	closeOnce       sync.Once
}

type retiredReader struct {
	createdTime time.Time
	reader      *segment.Reader
}

const (
	defaultReaderRetireTTL      = 2 * time.Second
	defaultReaderCleanupTick    = 100 * time.Millisecond
	defaultRetiredReaderBufSize = 64
)

func (l *Leaf) spawnReaderCloseWorker(ttl time.Duration) {
	go func() {
		defer close(l.readerCloseDone)

		ticker := time.NewTicker(defaultReaderCleanupTick)
		defer ticker.Stop()

		queue := make([]retiredReader, 0)
		for {
			select {
			case <-l.readerCloseStop:
				for _, retired := range queue {
					closeRetiredReader(retired)
				}
				for {
					select {
					case retired := <-l.retired:
						closeRetiredReader(retired)
					default:
						return
					}
				}
			case retired := <-l.retired:
				queue = append(queue, retired)
			case now := <-ticker.C:
				queue = closeExpiredReaders(queue, now, ttl)
			}
		}
	}()
}

// NewLeaf creates a leaf with an empty buffer and optional segment reader.
func NewLeaf(id, seed uint64, reader *segment.Reader, merger *Merger) *Leaf {
	leaf := &Leaf{
		id:              id,
		seed:            seed,
		buffer:          NewBuffer(seed),
		merger:          merger,
		retired:         make(chan retiredReader, defaultRetiredReaderBufSize),
		readerCloseStop: make(chan struct{}),
		readerCloseDone: make(chan struct{}),
	}
	leaf.reader.Store(reader)
	leaf.spawnReaderCloseWorker(defaultReaderRetireTTL)
	return leaf
}

// Get returns the materialized user value for key.
func (l *Leaf) Get(key []byte) ([]byte, bool, error) {
	if l == nil {
		return nil, false, nil
	}

	if op, ok := l.buffer.ReadOp(key); ok {
		if op.Kind != ops.OpInc {
			encoded, ok, err := l.resolveOp(nil, op)
			if err != nil || !ok {
				return nil, ok, err
			}
			return decodeUserValue(encoded)
		}

		segmentValue, segmentOK, err := l.readSegmentValue(key)
		if err != nil {
			return nil, false, err
		}
		encoded, ok, err := l.resolveOp(optionalValue(segmentValue, segmentOK), op)
		if err != nil || !ok {
			return nil, ok, err
		}
		return decodeUserValue(encoded)
	}

	segmentValue, ok, err := l.readSegmentValue(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	return decodeUserValue(segmentValue)
}

func (l *Leaf) readSegmentValue(key []byte) ([]byte, bool, error) {
	reader := l.reader.Load()
	if reader == nil {
		return nil, false, nil
	}
	return reader.Get(key)
}

func optionalValue(value []byte, ok bool) []byte {
	if !ok {
		return nil
	}
	return value
}

func decodeUserValue(encoded []byte) ([]byte, bool, error) {
	kind, err := value.KindOf(encoded)
	if err != nil {
		return nil, false, err
	}
	switch kind {
	case value.KindBytes:
		raw, err := value.DecodeBytes(encoded)
		return raw, err == nil, err
	case value.KindInt64:
		return encoded, true, nil
	default:
		return nil, false, value.ErrUnknownKind
	}
}

// Put buffers a byte value replacement for key.
func (l *Leaf) Put(key, raw []byte) {
	l.buffer.Put(key, value.EncodeBytes(raw))
}

// Delete buffers a delete tombstone for key.
func (l *Leaf) Delete(key []byte) {
	l.buffer.Delete(key)
}

// Inc buffers a signed integer increment for key.
func (l *Leaf) Inc(key []byte, delta int64) {
	l.buffer.Inc(key, delta)
}

// BufferedLen returns number of keys currently represented in the active buffer.
func (l *Leaf) BufferedLen() int64 {
	if l == nil {
		return 0
	}
	return l.buffer.Len()
}

// BufferedBytes returns active buffered operation payload bytes.
func (l *Leaf) BufferedBytes() int64 {
	if l == nil {
		return 0
	}
	return l.buffer.EstimatedBytes()
}

func (l *Leaf) Merge(opts segment.Options) error {
	if l == nil || l.merger == nil {
		return nil
	}
	if !l.buffer.Freeze() {
		return nil
	}

	segmentIter := emptySegmentIter
	if reader := l.reader.Load(); reader != nil {
		segmentIter = reader.Iter()
	}

	newSegment, err := l.merger.merge(segmentIter, l.buffer.IterFrozen(), opts)
	if err != nil {
		return err
	}

	reader, err := segment.NewReader(newSegment, l.merger.Compression)
	if err != nil {
		return err
	}

	oldReader := l.reader.Swap(reader)
	if oldReader != nil {
		l.retireReader(oldReader)
	}
	l.buffer.ClearFrozen()
	return nil
}

// Close releases readers owned by this leaf.
func (l *Leaf) Close() error {
	if l == nil {
		return nil
	}

	var err error
	l.closeOnce.Do(func() {
		close(l.readerCloseStop)
		<-l.readerCloseDone

		if reader := l.reader.Swap(nil); reader != nil {
			err = reader.Close()
		}
	})
	return err
}

func (l *Leaf) retireReader(reader *segment.Reader) {
	if reader == nil {
		return
	}
	l.retired <- retiredReader{
		createdTime: time.Now(),
		reader:      reader,
	}
}

func closeExpiredReaders(queue []retiredReader, now time.Time, ttl time.Duration) []retiredReader {
	keep := queue[:0]
	for _, retired := range queue {
		if now.Sub(retired.createdTime) >= ttl {
			closeRetiredReader(retired)
			continue
		}
		keep = append(keep, retired)
	}
	return keep
}

func closeRetiredReader(retired retiredReader) {
	if retired.reader == nil {
		return
	}
	oldSegment := retired.reader.Segment()
	_ = retired.reader.Close()
	if oldSegment != nil {
		_ = oldSegment.Remove()
	}
}

func (l *Leaf) resolveOp(segmentValue []byte, op ops.Op) ([]byte, bool, error) {
	switch op.Kind {
	case ops.OpDelete:
		return nil, false, nil
	case ops.OpPut:
		return op.Data, true, nil
	case ops.OpInc:
		base := int64(0)
		if len(segmentValue) > 0 {
			var err error
			base, err = value.DecodeInt64(segmentValue)
			if err != nil {
				return nil, false, err
			}
		}
		return value.EncodeInt64(base + ops.DecodeInc(op)), true, nil
	default:
		return segmentValue, len(segmentValue) > 0, nil
	}
}

func emptySegmentIter(yield func([]byte, []byte) bool) {
}

var _ iter.Seq2[[]byte, []byte] = emptySegmentIter
