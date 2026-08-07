package leaf

import (
	"iter"

	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/value"
)

// TryPut buffers a value replacement for key.
//
// It reports false when the leaf has been detached by a split, which tells the
// caller to look the key up again in the updated tree. Writes take the read
// side of writeMu, so concurrent writers never block each other; only a split
// excludes them, and only for as long as it takes to hand the remaining
// operations to the new leaves.
func (l *Leaf) TryPut(key, raw []byte) bool {
	l.writeMu.RLock()
	defer l.writeMu.RUnlock()

	if l.detached {
		return false
	}
	l.buffer.PutOwned(key, value.EncodeBytes(raw))
	return true
}

// TryDelete buffers a delete tombstone for key. See TryPut for the contract.
func (l *Leaf) TryDelete(key []byte) bool {
	l.writeMu.RLock()
	defer l.writeMu.RUnlock()

	if l.detached {
		return false
	}
	l.buffer.Delete(key)
	return true
}

// TryInc buffers a signed counter increment for key. See TryPut for the
// contract.
//
// Retrying a rejected increment is safe precisely because the operation was
// rejected: a detached leaf never applied it, so the retry cannot double count.
func (l *Leaf) TryInc(key []byte, delta int64) bool {
	l.writeMu.RLock()
	defer l.writeMu.RUnlock()

	if l.detached {
		return false
	}
	l.buffer.Inc(key, delta)
	return true
}

// Detached reports whether the leaf has been replaced by a split.
func (l *Leaf) Detached() bool {
	l.writeMu.RLock()
	defer l.writeMu.RUnlock()

	return l.detached
}

// DetachUnder marks the leaf as replaced and runs handover while writers are
// excluded.
//
// handover receives the operations that arrived after the merge froze the
// buffer, and must both route them into the new leaves and publish those leaves
// so that writers see them the moment this returns. Doing both under the write
// lock is what preserves write ordering: if the new leaves became visible
// first, a fresh write could land under an older operation replayed here.
func (l *Leaf) DetachUnder(handover func(pending iter.Seq2[[]byte, ops.Op])) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	l.detached = true

	pending := l.buffer.TakeActive()
	if handover != nil {
		handover(pending.Iter())
	}
}

// FreezeBuffer moves the active operations into the frozen layer.
//
// A checkpoint calls this on every leaf while writers are held off, so the set
// of frozen operations corresponds exactly to a known log position. A false
// result means a frozen layer was already present from an earlier merge, which
// is harmless: those operations are merged too.
func (l *Leaf) FreezeBuffer() bool {
	if l == nil {
		return false
	}

	// Writers are excluded for the swap itself: an operation published into the
	// old active list after a merge has walked it would be dropped by the
	// ClearFrozen that follows the merge.
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	return l.buffer.Freeze()
}

// ApplyOp publishes op for key directly into the active buffer.
//
// It bypasses the detached check and is meant for split handover, where the
// target leaf is not yet visible to writers.
func (l *Leaf) ApplyOp(key []byte, op ops.Op) {
	l.buffer.Apply(key, op)
}

// InstallMerge adopts a merged segment that covers the whole leaf range.
//
// This is the non-splitting path: the leaf keeps its identity and range, and
// only exchanges its segment. The previous reader is retired rather than closed
// immediately so in-flight lookups can finish.
func (l *Leaf) InstallMerge(result MergeResult) {
	if l == nil {
		return
	}

	oldReader := l.reader.Swap(result.Reader)
	l.segmentKeys.Store(result.Keys)
	if oldReader != nil {
		l.retireReader(oldReader)
	}
	l.buffer.ClearFrozen()
}

// DropFrozen releases the frozen layer after a merge that produced no segment,
// which happens when every buffered key was deleted.
func (l *Leaf) DropFrozen() {
	if l == nil {
		return
	}

	oldReader := l.reader.Swap(nil)
	l.segmentKeys.Store(0)
	if oldReader != nil {
		l.retireReader(oldReader)
	}
	l.buffer.ClearFrozen()
}
