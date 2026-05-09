package leaf

import (
	"iter"
	"sync"
	"sync/atomic"

	"fusedb/internal/ops"
	"fusedb/internal/skiplist"
)

// Buffer is a leaf-local mutable operation buffer.
//
// Buffer is intentionally thin: it owns the ergonomic mutation API for a leaf,
// while skiplist owns the ordered in-memory operation index.
type Buffer struct {
	freezeMu sync.Mutex
	seed     uint64
	active   atomic.Pointer[skiplist.SkipList]
	frozen   atomic.Pointer[skiplist.SkipList]
}

// NewBuffer creates an empty leaf mutation buffer.
//
// The seed is passed through to the underlying skiplist height selection.
func NewBuffer(seed uint64) *Buffer {
	buffer := &Buffer{
		seed: seed,
	}
	buffer.active.Store(skiplist.NewSkipList(seed))
	return buffer
}

// Len returns the number of unique keys currently represented in the buffer.
//
// Delete tombstones are counted because they are still buffered operations.
func (b *Buffer) Len() int64 {
	return b.activeList().Len()
}

// DataBytes returns the total byte length of Op.Data payloads in the buffer.
//
// Delete tombstones contribute zero data bytes.
func (b *Buffer) DataBytes() int64 {
	return b.activeList().DataBytes()
}

// EstimatedBytes returns the current buffered data payload size.
//
// This is exact for Op.Data bytes and intentionally does not include key bytes,
// node overhead, or skiplist next-pointer overhead.
func (b *Buffer) EstimatedBytes() int64 {
	return b.DataBytes()
}

// Apply publishes op for key.
//
// Callers must not mutate key while Apply is running and must treat op.Data as
// immutable after Apply returns.
func (b *Buffer) Apply(key []byte, op ops.Op) {
	b.activeList().Apply(key, op)
}

// Put buffers a value replacement for key.
//
// Put copies value before publishing it.
func (b *Buffer) Put(key []byte, value []byte) {
	b.Apply(key, ops.NewPut(value))
}

// Delete buffers a delete tombstone for key.
func (b *Buffer) Delete(key []byte) {
	b.Apply(key, ops.NewDelete())
}

// Inc buffers a signed counter increment for key.
func (b *Buffer) Inc(key []byte, delta int64) {
	b.Apply(key, ops.NewInc(delta))
}

// ReadOp returns the buffered operation for key without copying Op.Data.
//
// The returned Op is a zero-copy view of immutable buffer-owned data. Callers
// must not mutate returned Op.Data. Use SafeReadOp when an owned copy is needed.
func (b *Buffer) ReadOp(key []byte) (ops.Op, bool) {
	active, hasActive := b.activeList().Read(key)
	if frozen := b.frozen.Load(); frozen != nil {
		frozenOp, hasFrozen := frozen.Read(key)
		return mergeLayeredOps(frozenOp, hasFrozen, active, hasActive)
	}
	if hasActive {
		return active, true
	}
	return ops.Op{}, false
}

// SafeReadOp returns the buffered operation for key with an owned Op.Data copy.
func (b *Buffer) SafeReadOp(key []byte) (ops.Op, bool) {
	op, ok := b.ReadOp(key)
	if !ok {
		return ops.Op{}, false
	}
	return op.Clone(), true
}

// IterOps returns a zero-copy ordered iterator over buffered operations.
//
// This is the intended iterator for leaf merge code. Callers must not mutate
// returned Op.Data.
func (b *Buffer) IterOps() iter.Seq2[[]byte, ops.Op] {
	return b.activeList().Iter()
}

// SafeIterOps returns an ordered iterator that copies Op.Data for every entry.
func (b *Buffer) SafeIterOps() iter.Seq2[[]byte, ops.Op] {
	return b.activeList().SafeIter()
}

// Freeze moves current active operations into the frozen slot.
//
// Writes after Freeze go into a fresh active skiplist. If frozen operations are
// already present, Freeze returns false and leaves the buffer unchanged.
func (b *Buffer) Freeze() bool {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	if b.frozen.Load() != nil {
		return false
	}
	oldActive := b.active.Swap(skiplist.NewSkipList(b.seed))
	b.frozen.Store(oldActive)
	return true
}

// FrozenLen returns the number of unique keys in the frozen operation layer.
func (b *Buffer) FrozenLen() int64 {
	frozen := b.frozen.Load()
	if frozen == nil {
		return 0
	}
	return frozen.Len()
}

// ReadFrozen returns an operation from the frozen layer without copying data.
func (b *Buffer) ReadFrozen(key []byte) (ops.Op, bool) {
	frozen := b.frozen.Load()
	if frozen == nil {
		return ops.Op{}, false
	}
	return frozen.Read(key)
}

// IterFrozen returns a zero-copy ordered iterator over frozen operations.
func (b *Buffer) IterFrozen() iter.Seq2[[]byte, ops.Op] {
	frozen := b.frozen.Load()
	if frozen == nil {
		return emptyOpsIter
	}
	return frozen.Iter()
}

// ClearFrozen drops the frozen operation layer after a successful merge.
func (b *Buffer) ClearFrozen() {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	b.frozen.Store(nil)
}

func emptyOpsIter(yield func([]byte, ops.Op) bool) {
}

func mergeLayeredOps(lower ops.Op, hasLower bool, upper ops.Op, hasUpper bool) (ops.Op, bool) {
	if !hasUpper {
		return lower, hasLower
	}
	if !hasLower {
		return upper, true
	}
	if upper.Kind != ops.OpInc {
		return upper, true
	}
	if lower.Kind != ops.OpInc {
		return upper, true
	}

	merged := lower.Clone()
	ops.MergeIncInto(&merged, upper)
	return merged, true
}

func (b *Buffer) activeList() *skiplist.SkipList {
	active := b.active.Load()
	if active != nil {
		return active
	}
	created := skiplist.NewSkipList(b.seed)
	if b.active.CompareAndSwap(nil, created) {
		return created
	}
	return b.active.Load()
}
