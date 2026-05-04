package leaf

import (
	"iter"
	"sync"

	"fusedb/internal/skiplist"
)

// Buffer is a leaf-local mutable operation buffer.
//
// Buffer is intentionally thin: it owns the ergonomic mutation API for a leaf,
// while skiplist owns the ordered in-memory operation index.
type Buffer struct {
	mu   sync.RWMutex
	seed uint64
	ops  *skiplist.SkipList
}

// ImmutableOps is a read-only snapshot of buffer operations.
//
// It is returned by FreezeOps and is intended for merge code. The underlying
// skiplist is no longer reachable by Buffer writes after FreezeOps returns.
type ImmutableOps struct {
	ops *skiplist.SkipList
}

// NewBuffer creates an empty leaf mutation buffer.
//
// The seed is passed through to the underlying skiplist height selection.
func NewBuffer(seed uint64) *Buffer {
	return &Buffer{
		seed: seed,
		ops:  skiplist.NewSkipList(seed),
	}
}

// Len returns the number of unique keys currently represented in the buffer.
//
// Delete tombstones are counted because they are still buffered operations.
func (b *Buffer) Len() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ops.Len()
}

// DataBytes returns the total byte length of Op.Data payloads in the buffer.
//
// Delete tombstones contribute zero data bytes.
func (b *Buffer) DataBytes() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ops.DataBytes()
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
// Callers must treat op.Data as immutable after Apply returns.
func (b *Buffer) Apply(key string, op skiplist.Op) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.ops.Apply(key, op)
}

// Put buffers a value replacement for key.
//
// Put copies value before publishing it.
func (b *Buffer) Put(key string, value []byte) {
	b.Apply(key, skiplist.NewPut(value))
}

// Delete buffers a delete tombstone for key.
func (b *Buffer) Delete(key string) {
	b.Apply(key, skiplist.NewDelete())
}

// Inc buffers a signed counter increment for key.
func (b *Buffer) Inc(key string, delta int64) {
	b.Apply(key, skiplist.NewInc(delta))
}

// ReadOp returns the buffered operation for key without copying Op.Data.
//
// The returned Op is a zero-copy view of immutable buffer-owned data. Callers
// must not mutate returned Op.Data. Use SafeReadOp when an owned copy is needed.
func (b *Buffer) ReadOp(key string) (skiplist.Op, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ops.Read(key)
}

// SafeReadOp returns the buffered operation for key with an owned Op.Data copy.
func (b *Buffer) SafeReadOp(key string) (skiplist.Op, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ops.SafeRead(key)
}

// IterOps returns a zero-copy ordered iterator over buffered operations.
//
// This is the intended iterator for leaf merge code. Callers must not mutate
// returned Op.Data.
func (b *Buffer) IterOps() iter.Seq2[string, skiplist.Op] {
	b.mu.RLock()
	ops := b.ops
	b.mu.RUnlock()

	return ops.Iter()
}

// SafeIterOps returns an ordered iterator that copies Op.Data for every entry.
func (b *Buffer) SafeIterOps() iter.Seq2[string, skiplist.Op] {
	b.mu.RLock()
	ops := b.ops
	b.mu.RUnlock()

	return ops.SafeIter()
}

// FreezeOps detaches current operations and returns them as an immutable view.
//
// Writes after FreezeOps go into a fresh active skiplist. Existing in-flight
// Apply calls complete before the snapshot is detached, so the returned
// ImmutableOps will not receive future Buffer writes.
func (b *Buffer) FreezeOps() *ImmutableOps {
	b.mu.Lock()
	defer b.mu.Unlock()

	ops := b.ops
	b.ops = skiplist.NewSkipList(b.seed)

	return &ImmutableOps{
		ops: ops,
	}
}

// Len returns the number of unique keys represented in the immutable snapshot.
func (i *ImmutableOps) Len() int64 {
	if i == nil || i.ops == nil {
		return 0
	}

	return i.ops.Len()
}

// DataBytes returns the total byte length of Op.Data payloads in the snapshot.
func (i *ImmutableOps) DataBytes() int64 {
	if i == nil || i.ops == nil {
		return 0
	}

	return i.ops.DataBytes()
}

// ReadOp returns an operation from the immutable snapshot without copying data.
func (i *ImmutableOps) ReadOp(key string) (skiplist.Op, bool) {
	if i == nil || i.ops == nil {
		return skiplist.Op{}, false
	}

	return i.ops.Read(key)
}

// SafeReadOp returns an operation from the immutable snapshot with owned data.
func (i *ImmutableOps) SafeReadOp(key string) (skiplist.Op, bool) {
	if i == nil || i.ops == nil {
		return skiplist.Op{}, false
	}

	return i.ops.SafeRead(key)
}

// IterOps returns a zero-copy ordered iterator over immutable operations.
func (i *ImmutableOps) IterOps() iter.Seq2[string, skiplist.Op] {
	if i == nil || i.ops == nil {
		return emptyOpsIter
	}

	return i.ops.Iter()
}

// SafeIterOps returns an ordered iterator that copies every operation payload.
func (i *ImmutableOps) SafeIterOps() iter.Seq2[string, skiplist.Op] {
	if i == nil || i.ops == nil {
		return emptyOpsIter
	}

	return i.ops.SafeIter()
}

func emptyOpsIter(yield func(string, skiplist.Op) bool) {
}
