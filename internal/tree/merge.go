package tree

import (
	"fmt"
	"iter"

	"github.com/uchebnick/fusedb/internal/leaf"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/segment"
)

// maxMergeAllRounds bounds how often MergeAll revisits the tree. A split moves
// buffered writes into leaves the pass has already visited, so one extra round
// flushes them, and the bound keeps concurrent writers from making MergeAll run
// forever.
const maxMergeAllRounds = 4

// PendingMerge returns the leaves whose buffer crossed the merge threshold.
func (t *Tree) PendingMerge() []*leaf.Leaf {
	retry := t.retrySnapshot()
	pending := make([]*leaf.Leaf, 0, 8)
	for _, l := range t.snapshot() {
		if l.BufferedBytes() >= t.mergeThresholdBytes || contains(retry, l.ID()) {
			pending = append(pending, l)
		}
	}
	return pending
}

// MergeAll merges every leaf that holds buffered writes.
func (t *Tree) MergeAll() error {
	if t.closed.Load() {
		return ErrClosed
	}

	for round := 0; round < maxMergeAllRounds; round++ {
		retry := t.retrySnapshot()
		worked := false
		for _, l := range t.snapshot() {
			if l.PendingLen() == 0 && !contains(retry, l.ID()) {
				continue
			}
			worked = true
			if err := t.MergeLeaf(l); err != nil {
				return err
			}
		}
		if !worked {
			return nil
		}
	}
	return nil
}

// MergeLeaf rewrites one leaf segment from its buffered operations.
//
// The merge is local: only this leaf is read and written, which is what bounds
// write amplification to the leaf size instead of the database size. When the
// merged result outgrows MaxLeafBytes it is cut into several segments and the
// leaf is replaced by one leaf per segment.
//
// A leaf that is no longer part of the published tree is ignored, so a caller
// may hand back a leaf list it collected earlier.
func (t *Tree) MergeLeaf(l *leaf.Leaf) error {
	if t == nil || l == nil {
		return nil
	}
	if t.closed.Load() {
		return ErrClosed
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	index, ok := t.currentIndexLocked(l)
	if !ok {
		return nil
	}
	// PendingLen, not BufferedLen: a checkpoint freezes every buffer first, so
	// the active layer is empty while the frozen operations still need writing.
	if l.PendingLen() == 0 {
		if _, retry := t.retryMerge[l.ID()]; !retry {
			return nil
		}
	}

	next := t.manifest.Clone()
	recordIndex := next.IndexOfLeafID(l.ID())
	if recordIndex < 0 {
		return fmt.Errorf("%w: %d", ErrLeafNotInManifest, l.ID())
	}
	record := next.Leaves[recordIndex]

	// Allocation order matches output order, which is how each result learns
	// the segment identity it was written with: MergeResult only carries the
	// file, not the id it was given.
	allocated := make([]segmentIdentity, 0, 4)
	results, err := l.MergeSplit(leaf.SplitOptions{
		Base:     t.segmentOptions(),
		MaxBytes: t.maxLeafBytes,
		Allocate: func(outputIndex int) (uint64, uint64, error) {
			identity := segmentIdentity{segmentID: record.SegmentID, version: record.SegmentVersion + 1}
			// The first output continues the range the leaf already owns, so it
			// keeps the segment id and only moves to the next version.
			if outputIndex > 0 || record.SegmentID == 0 {
				identity = segmentIdentity{segmentID: next.AllocateSegmentID(), version: 1}
			}
			allocated = append(allocated, identity)
			return identity.segmentID, identity.version, nil
		},
	})
	if err != nil {
		t.failMergeLocked(l.ID(), next, nil)
		return fmt.Errorf("tree: merge leaf %d: %w", l.ID(), err)
	}
	if len(allocated) != len(results) {
		t.failMergeLocked(l.ID(), next, results)
		return fmt.Errorf("%w: %d allocated, %d produced", ErrAllocationMismatch, len(allocated), len(results))
	}

	switch len(results) {
	case 0:
		return t.installEmptyLocked(l, next)
	case 1:
		return t.installMergeLocked(l, next, results[0], allocated[0])
	default:
		return t.installSplitLocked(l, index, next, results, allocated)
	}
}

type segmentIdentity struct {
	segmentID uint64
	version   uint64
}

// installEmptyLocked handles a merge whose every key resolved to a tombstone.
func (t *Tree) installEmptyLocked(l *leaf.Leaf, next *manifest.Manifest) error {
	if err := next.SetLeafSegment(l.ID(), 0, 0); err != nil {
		t.failMergeLocked(l.ID(), next, nil)
		return err
	}
	if err := t.saveManifestLocked(next); err != nil {
		t.failMergeLocked(l.ID(), next, nil)
		return fmt.Errorf("tree: save manifest: %w", err)
	}

	t.manifest = next
	t.gen.Add(1)
	l.DropFrozen()
	t.gen.Add(1)
	delete(t.retryMerge, l.ID())
	return nil
}

// installMergeLocked handles a merge that fit into a single segment.
//
// The leaf keeps its identity, its buffer, and its place in the tree; only the
// segment underneath it is exchanged. Recreating the leaf here would throw away
// writes that arrived while the merge was running.
func (t *Tree) installMergeLocked(
	l *leaf.Leaf,
	next *manifest.Manifest,
	result leaf.MergeResult,
	identity segmentIdentity,
) error {
	if err := next.SetLeafSegment(l.ID(), identity.segmentID, identity.version); err != nil {
		t.failMergeLocked(l.ID(), next, []leaf.MergeResult{result})
		return err
	}
	if recordIndex := next.IndexOfLeafID(l.ID()); recordIndex >= 0 {
		next.Leaves[recordIndex].Keys = uint64(result.Keys)
	}
	// The manifest has to name the new segment before the old one may be
	// deleted, and the old one is only queued for deletion by InstallMerge.
	if err := t.saveManifestLocked(next); err != nil {
		t.failMergeLocked(l.ID(), next, []leaf.MergeResult{result})
		return fmt.Errorf("tree: save manifest: %w", err)
	}

	t.manifest = next
	t.gen.Add(1)
	l.InstallMerge(result)
	t.gen.Add(1)
	delete(t.retryMerge, l.ID())
	return nil
}

// installSplitLocked replaces one leaf by the leaves its merge produced.
//
// The publication order here is the reason splits do not lose writes. The new
// leaves are built and the manifest is persisted first, while the old leaf is
// still the one writers use. Then, under the old leaf write lock, the
// operations buffered after the merge froze are routed into the new leaves and
// only after that are the new leaves published. Publishing first would let a
// fresh write land in a new leaf and then be overwritten by an older replayed
// operation for the same key.
func (t *Tree) installSplitLocked(
	l *leaf.Leaf,
	index int,
	next *manifest.Manifest,
	results []leaf.MergeResult,
	allocated []segmentIdentity,
) error {
	created := make([]*leaf.Leaf, len(results))
	records := make([]manifest.LeafRecord, len(results))
	for i, result := range results {
		// The first output keeps the leaf id: it covers the same lower bound
		// and continues the same segment lineage.
		leafID := l.ID()
		if i > 0 {
			leafID = t.allocateLeafID()
		}
		created[i] = t.newLeaf(leafID, result.LowKey, result.Reader, result.Keys)
		records[i] = manifest.LeafRecord{
			LeafID:         leafID,
			LowKey:         result.LowKey,
			SegmentID:      allocated[i].segmentID,
			SegmentVersion: allocated[i].version,
			Keys:           uint64(result.Keys),
		}
	}

	recordIndex := next.IndexOfLeafID(l.ID())
	if recordIndex < 0 {
		t.abortSplitLocked(l.ID(), next, created, results)
		return fmt.Errorf("%w: %d", ErrLeafNotInManifest, l.ID())
	}
	leaves := make([]manifest.LeafRecord, 0, len(next.Leaves)+len(records)-1)
	leaves = append(leaves, next.Leaves[:recordIndex]...)
	leaves = append(leaves, records...)
	leaves = append(leaves, next.Leaves[recordIndex+1:]...)
	next.Leaves = leaves

	if err := t.saveManifestLocked(next); err != nil {
		t.abortSplitLocked(l.ID(), next, created, results)
		return fmt.Errorf("tree: save manifest: %w", err)
	}

	current := t.snapshot()
	published := make([]*leaf.Leaf, 0, len(current)+len(created)-1)
	published = append(published, current[:index]...)
	published = append(published, created...)
	published = append(published, current[index+1:]...)

	t.manifest = next
	t.gen.Add(1)
	l.DetachUnder(func(pending iter.Seq2[[]byte, ops.Op]) {
		for key, op := range pending {
			target := findLeaf(created, key)
			if target == nil {
				// The first output starts at the lower bound of the leaf being
				// split, so this is unreachable for keys the leaf owned.
				target = created[0]
			}
			target.ApplyOp(key, op)
		}
		t.publish(published)
	})
	// The replaced segment is superseded but lookups that started before the
	// split may still be inside it, so it goes through the retire queue.
	l.RetireSegment()
	t.gen.Add(1)

	_ = l.Close()
	delete(t.retryMerge, l.ID())
	return nil
}

// failMergeLocked rolls a failed merge back to the state on disk.
//
// Segment ids consumed by the failed attempt are kept: the outputs are removed
// here, but a leftover file from a partially failed removal must never be hit
// by a later merge reusing the id.
func (t *Tree) failMergeLocked(leafID uint64, next *manifest.Manifest, results []leaf.MergeResult) {
	discardResults(results)
	if next != nil && next.NextSegmentID > t.manifest.NextSegmentID {
		t.manifest.NextSegmentID = next.NextSegmentID
	}
	// The merge froze the leaf buffer before it failed. That data is invisible
	// to BufferedLen, so the leaf has to be marked as still needing a merge.
	t.retryMerge[leafID] = struct{}{}
}

func (t *Tree) abortSplitLocked(
	leafID uint64,
	next *manifest.Manifest,
	created []*leaf.Leaf,
	results []leaf.MergeResult,
) {
	// The new leaves were never published, so closing them only releases the
	// readers they hold; the files themselves are removed with the results.
	for _, l := range created {
		if l != nil {
			_ = l.Close()
		}
	}
	t.failMergeLocked(leafID, next, results)
}

func discardResults(results []leaf.MergeResult) {
	for _, result := range results {
		if result.Reader != nil {
			_ = result.Reader.Close()
		}
		if result.Segment != nil {
			_ = result.Segment.Remove()
		}
	}
}

// retrySnapshot copies the set of leaves whose last merge failed after it had
// already frozen their buffer.
//
// The set is almost always empty, so scanning a large tree does not pay for a
// lock per leaf.
func (t *Tree) retrySnapshot() map[uint64]struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.retryMerge) == 0 {
		return nil
	}
	retry := make(map[uint64]struct{}, len(t.retryMerge))
	for leafID := range t.retryMerge {
		retry[leafID] = struct{}{}
	}
	return retry
}

func contains(set map[uint64]struct{}, leafID uint64) bool {
	if set == nil {
		return false
	}
	_, ok := set[leafID]
	return ok
}

// currentIndexLocked reports where l sits in the published tree.
//
// Low keys are unique, so a leaf that was replaced by a split is recognized by
// the pointer at its position no longer being the leaf itself.
func (t *Tree) currentIndexLocked(l *leaf.Leaf) (int, bool) {
	current := t.snapshot()
	index := searchLeaf(current, l.LowKey())
	if index < 0 || current[index] != l {
		return 0, false
	}
	return index, true
}

func (t *Tree) segmentOptions() segment.Options {
	return segment.Options{
		FS:                    t.fs,
		Dir:                   t.dir,
		TargetBlockSize:       t.targetBlockSize,
		BloomFalsePositive:    t.bloomFalsePositive,
		Compression:           t.compression,
		CompressionDictionary: t.dictionary,
	}
}
