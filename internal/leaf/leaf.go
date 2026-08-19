package leaf

import (
	"bytes"
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

	// lowKey is the inclusive lower bound of the leaf range. It is immutable
	// for the life of the leaf; a leaf never widens or narrows, it is replaced
	// by new leaves when it splits. An empty lowKey marks the leftmost leaf.
	lowKey []byte

	// segmentKeys tracks how many keys the current segment holds so the next
	// merge can size its bloom filter for the merged result rather than for
	// the buffer alone.
	segmentKeys atomic.Int64

	// writeMu keeps buffered writes from racing a split. Writers take the read
	// side, a split takes the write side and marks the leaf detached, which
	// tells writers that were waiting to retry against the new tree.
	writeMu  sync.RWMutex
	detached bool

	buffer *Buffer
	reader atomic.Pointer[segment.Reader]

	merger *Merger

	// retireHook, when set, takes over disposal of replaced segment readers.
	retireHook func(*segment.Reader)

	retired         chan retiredReader
	readerCloseStop chan struct{}
	readerCloseDone chan struct{}
	closeOnce       sync.Once

	// The cleanup goroutine starts on first use rather than at construction.
	// A tree holds one leaf per key range, so starting it eagerly would leave
	// one sleeping goroutine per leaf; with a retire hook installed the leaf
	// never needs its own worker at all.
	retireWorkerOnce    sync.Once
	retireWorkerStarted atomic.Bool
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
//
// The leaf covers the whole keyspace. Use NewRangeLeaf for a leaf that owns
// only part of it.
func NewLeaf(id, seed uint64, reader *segment.Reader, merger *Merger) *Leaf {
	return NewRangeLeaf(id, seed, nil, reader, merger, 0)
}

// NewRangeLeaf creates a leaf that owns keys from lowKey onwards.
//
// segmentKeys is the key count of reader's segment, used to size bloom filters
// on the next merge. Zero is safe but makes the next filter smaller than ideal.
func NewRangeLeaf(
	id, seed uint64,
	lowKey []byte,
	reader *segment.Reader,
	merger *Merger,
	segmentKeys int64,
) *Leaf {
	leaf := &Leaf{
		id:              id,
		seed:            seed,
		lowKey:          bytes.Clone(lowKey),
		buffer:          NewBuffer(seed),
		merger:          merger,
		retired:         make(chan retiredReader, defaultRetiredReaderBufSize),
		readerCloseStop: make(chan struct{}),
		readerCloseDone: make(chan struct{}),
	}
	leaf.segmentKeys.Store(segmentKeys)
	leaf.reader.Store(reader)
	return leaf
}

// ID returns the leaf identifier.
func (l *Leaf) ID() uint64 {
	if l == nil {
		return 0
	}
	return l.id
}

// LowKey returns the inclusive lower bound of the leaf range.
//
// The returned slice is leaf-owned and must not be mutated.
func (l *Leaf) LowKey() []byte {
	if l == nil {
		return nil
	}
	return l.lowKey
}

// SegmentKeys returns the key count of the current segment.
func (l *Leaf) SegmentKeys() int64 {
	if l == nil {
		return 0
	}
	return l.segmentKeys.Load()
}

// Reader returns the current segment reader, which may be nil.
func (l *Leaf) Reader() *segment.Reader {
	if l == nil {
		return nil
	}
	return l.reader.Load()
}

// Get returns the materialized user value for key.
func (l *Leaf) Get(key []byte) ([]byte, bool, error) {
	encoded, ok, err := l.GetEncoded(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	return decodeUserValue(encoded)
}

// GetEncoded returns the materialized value including its internal kind tag.
// The returned slice borrows leaf-owned memory. It is used by the DB mutation
// admission path to reject an Inc over a byte value before writing that
// operation to the WAL.
func (l *Leaf) GetEncoded(key []byte) ([]byte, bool, error) {
	if l == nil {
		return nil, false, nil
	}

	if op, ok := l.buffer.ReadOp(key); ok {
		if op.Kind != ops.OpInc {
			return l.resolveOp(nil, op)
		}

		segmentValue, segmentOK, err := l.readSegmentValue(key)
		if err != nil {
			return nil, false, err
		}
		return l.resolveOp(optionalValue(segmentValue, segmentOK), op)
	}

	return l.readSegmentValue(key)
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
//
// EncodeBytes already returns a freshly allocated buffer, so ownership is
// handed straight to the buffer instead of paying for a second copy.
func (l *Leaf) Put(key, raw []byte) {
	l.buffer.PutOwned(key, value.EncodeBytes(raw))
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

// PendingLen returns the number of buffered keys across both layers.
//
// BufferedLen only counts the active layer, so it reads as zero right after a
// freeze even though the frozen operations still have to reach a segment. Merge
// decisions must use this instead, or a checkpoint would freeze every buffer,
// conclude there was nothing to do, and drop the log records covering it.
func (l *Leaf) PendingLen() int64 {
	if l == nil {
		return 0
	}
	return l.buffer.Len() + l.buffer.FrozenLen()
}

// BufferedBytes returns active buffered operation payload bytes.
func (l *Leaf) BufferedBytes() int64 {
	if l == nil {
		return 0
	}
	return l.buffer.RetainedBytes()
}

func (l *Leaf) Merge(opts segment.Options) error {
	if l == nil || l.merger == nil {
		return nil
	}
	// A false result means a previous merge failed after freezing. Continuing
	// with that frozen layer is required for progress: bailing out here would
	// make every later merge a silent no-op and strand the buffered writes.
	l.FreezeBuffer()

	segmentIter := emptySegmentIter
	segmentErr := func() error { return nil }
	if reader := l.reader.Load(); reader != nil {
		segmentIter, segmentErr = reader.IterWithErr()
	}

	newSegment, err := l.merger.merge(segmentIter, segmentErr, l.buffer.IterFrozen(), opts)
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
		// Only stop the worker if something ever retired a reader through it.
		if l.retireWorkerStarted.Load() {
			close(l.readerCloseStop)
			<-l.readerCloseDone
		}

		if reader := l.reader.Swap(nil); reader != nil {
			err = reader.Close()
		}
	})
	return err
}

// SetRetireHook redirects retired readers to an external owner.
//
// A tree holds many leaves, so a per-leaf cleanup goroutine would scale with
// the leaf count for work that is naturally shared. With a hook installed the
// leaf stops using its own queue and the owner decides when a replaced segment
// is closed and deleted.
func (l *Leaf) SetRetireHook(hook func(*segment.Reader)) {
	if l == nil {
		return
	}
	l.retireHook = hook
}

// RetireSegment hands the current segment reader to the retire path and leaves
// the leaf without a segment.
//
// It is used when a leaf is replaced by a split: the segment it was built from
// is already superseded, but in-flight lookups may still be reading it.
func (l *Leaf) RetireSegment() {
	if l == nil {
		return
	}
	if reader := l.reader.Swap(nil); reader != nil {
		l.retireReader(reader)
	}
}

func (l *Leaf) retireReader(reader *segment.Reader) {
	if reader == nil {
		return
	}
	if l.retireHook != nil {
		l.retireHook(reader)
		return
	}

	l.ensureRetireWorker()
	l.retired <- retiredReader{
		createdTime: time.Now(),
		reader:      reader,
	}
}

// ensureRetireWorker starts the cleanup goroutine on first retirement.
func (l *Leaf) ensureRetireWorker() {
	l.retireWorkerOnce.Do(func() {
		l.retireWorkerStarted.Store(true)
		l.spawnReaderCloseWorker(defaultReaderRetireTTL)
	})
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
