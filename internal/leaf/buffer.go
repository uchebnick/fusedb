package leaf

import (
	"iter"
	"sync"
	"sync/atomic"

	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/skiplist"
)

// layers is the immutable pair of operation lists a buffer exposes.
//
// Holding both in one value is what makes freezing atomic for readers. With
// separate pointers for active and frozen, a reader could observe the moment
// between "active replaced" and "frozen published" and find a key in neither
// layer, which reads as a missing key and, for a counter, as a lost increment.
type layers struct {
	active *skiplist.SkipList
	frozen *skiplist.SkipList
}

// Buffer is a leaf-local mutable operation buffer.
//
// Buffer is intentionally thin: it owns the ergonomic mutation API for a leaf,
// while skiplist owns the ordered in-memory operation index.
type Buffer struct {
	freezeMu sync.Mutex
	seed     uint64
	state    atomic.Pointer[layers]
}

// NewBuffer creates an empty leaf mutation buffer.
//
// The seed is passed through to the underlying skiplist height selection.
func NewBuffer(seed uint64) *Buffer {
	buffer := &Buffer{
		seed: seed,
	}
	buffer.state.Store(&layers{active: skiplist.NewSkipList(seed)})
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

// RetainedBytes estimates active-buffer heap for maintenance admission. It
// includes key and node overhead so workloads made of tombstones, counters, or
// tiny values still create merge debt.
func (b *Buffer) RetainedBytes() int64 {
	return b.activeList().EstimatedBytes()
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

// PutOwned buffers a value replacement for key without copying value.
//
// The caller transfers ownership of value and must never touch it again. Leaf
// uses this for freshly encoded values so a Put costs one allocation instead of
// an encode followed by a defensive clone.
func (b *Buffer) PutOwned(key []byte, value []byte) {
	b.Apply(key, ops.NewPutOwned(value))
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
// Both layers are read from one snapshot, so a concurrent freeze cannot hide a
// key that is present in either of them.
//
// The returned Op is a zero-copy view of immutable buffer-owned data. Callers
// must not mutate returned Op.Data. Use SafeReadOp when an owned copy is needed.
func (b *Buffer) ReadOp(key []byte) (ops.Op, bool) {
	state := b.load()

	active, hasActive := state.active.Read(key)
	if state.frozen != nil {
		frozenOp, hasFrozen := state.frozen.Read(key)
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
//
// Callers must exclude writers around this call: a write that lands in the old
// active list after a merge has already walked it would be dropped by the
// following ClearFrozen.
func (b *Buffer) Freeze() bool {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	current := b.load()
	if current.frozen != nil {
		return false
	}
	b.state.Store(&layers{
		active: skiplist.NewSkipList(b.seed),
		frozen: current.active,
	})
	return true
}

// FrozenLen returns the number of unique keys in the frozen operation layer.
func (b *Buffer) FrozenLen() int64 {
	frozen := b.load().frozen
	if frozen == nil {
		return 0
	}
	return frozen.Len()
}

// ReadFrozen returns an operation from the frozen layer without copying data.
func (b *Buffer) ReadFrozen(key []byte) (ops.Op, bool) {
	frozen := b.load().frozen
	if frozen == nil {
		return ops.Op{}, false
	}
	return frozen.Read(key)
}

// IterFrozen returns a zero-copy ordered iterator over frozen operations.
func (b *Buffer) IterFrozen() iter.Seq2[[]byte, ops.Op] {
	frozen := b.load().frozen
	if frozen == nil {
		return emptyOpsIter
	}
	return frozen.Iter()
}

// ClearFrozen drops the frozen operation layer after a successful merge.
func (b *Buffer) ClearFrozen() {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	current := b.load()
	if current.frozen == nil {
		return
	}
	b.state.Store(&layers{active: current.active})
}

// RollbackFrozen folds a failed merge's frozen layer back under the active
// layer. The frozen operations are applied first and the newer active
// operations second, preserving the same ordering as ReadOp.
//
// Callers must exclude writers around this call. It is intentionally used only
// on a failed merge path so the next freeze can capture one exact generation
// and its corresponding WAL watermark.
func (b *Buffer) RollbackFrozen() bool {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	current := b.load()
	if current.frozen == nil {
		return false
	}

	merged := skiplist.NewSkipList(b.seed)
	for key, op := range current.frozen.Iter() {
		merged.Apply(key, op)
	}
	for key, op := range current.active.Iter() {
		merged.Apply(key, op)
	}
	b.state.Store(&layers{active: merged})
	return true
}

// TakeActive swaps in a fresh active list and returns the previous one.
//
// Callers must hold whatever exclusion keeps writers out; Buffer itself only
// guarantees the swap is atomic with respect to readers and to Freeze.
func (b *Buffer) TakeActive() *skiplist.SkipList {
	b.freezeMu.Lock()
	defer b.freezeMu.Unlock()

	current := b.load()
	b.state.Store(&layers{
		active: skiplist.NewSkipList(b.seed),
		frozen: current.frozen,
	})
	return current.active
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

func (b *Buffer) load() *layers {
	current := b.state.Load()
	if current != nil {
		return current
	}

	created := &layers{active: skiplist.NewSkipList(b.seed)}
	if b.state.CompareAndSwap(nil, created) {
		return created
	}
	return b.state.Load()
}

func (b *Buffer) activeList() *skiplist.SkipList {
	return b.load().active
}
