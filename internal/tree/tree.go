// Package tree owns the ordered set of leaves that partitions the keyspace.
//
// A single leaf makes every merge rewrite the whole database, so write
// amplification grows with the database size. The tree keeps the merge local:
// only the leaf that crossed its buffer threshold is rewritten, and a leaf that
// outgrows MaxLeafBytes is cut into several leaves by the same merge pass.
//
// The leaf set is small even for very large databases: at 64 MiB per leaf a
// terabyte of data is about sixteen thousand pointers. That is why the tree is
// a sorted slice published copy-on-write through an atomic pointer instead of a
// concurrent B-tree. Lookups take no lock at all: they load the snapshot,
// binary search it, and read the leaf. Structural changes are serialized by one
// mutex that the read and the ordinary write path never touch.
package tree

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/leaf"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/segment"
)

const (
	// DefaultMaxLeafBytes is the merged payload size at which a merge starts a
	// new leaf. It bounds the cost of every later merge of that range.
	DefaultMaxLeafBytes = 64 << 20

	// DefaultMergeThresholdBytes is the buffered payload size at which a leaf
	// becomes a merge candidate.
	DefaultMergeThresholdBytes = 8 << 20
)

var (
	// ErrNilFilesystem reports that no filesystem was supplied.
	ErrNilFilesystem = errors.New("tree: nil filesystem")

	// ErrNilManifest reports that a nil manifest was passed to Open.
	ErrNilManifest = errors.New("tree: nil manifest")

	// ErrClosed reports an operation on a tree that has already been closed.
	ErrClosed = errors.New("tree: tree is closed")

	// ErrNoLeaf reports that no leaf covers a key, which can only happen if the
	// leaf set lost its leftmost leaf and stopped covering the whole keyspace.
	ErrNoLeaf = errors.New("tree: no leaf covers key")

	// ErrEmptyKey rejects a write for the empty key. The segment block format
	// cannot encode it, and an empty low key already means "leftmost leaf", so
	// accepting one would only surface later as a merge that fails for the
	// whole leaf.
	ErrEmptyKey = errors.New("tree: empty key")

	// ErrLeafNotInManifest reports an in-memory leaf with no manifest record,
	// which means the two views of the leaf set have drifted apart.
	ErrLeafNotInManifest = errors.New("tree: leaf is missing from the manifest")

	// ErrAllocationMismatch reports a merge that produced a different number of
	// segments than it reserved identifiers for.
	ErrAllocationMismatch = errors.New("tree: merge output count does not match allocated segments")
)

// Options configure one leaf tree.
type Options struct {
	FS  disk.FS
	Dir string

	// Seed feeds leaf buffer height selection; leaves created by a split reuse
	// it so that buffer behavior does not depend on when a leaf was born.
	Seed uint64

	// MaxLeafBytes is the merged payload size at which a merge cuts a new
	// output segment, and with it a new leaf. Zero uses DefaultMaxLeafBytes.
	MaxLeafBytes int64

	// MergeThresholdBytes is the buffered payload size that makes a leaf show
	// up in PendingMerge. Zero uses DefaultMergeThresholdBytes.
	MergeThresholdBytes int64

	TargetBlockSize    int
	BloomFalsePositive float64
	Compression        segment.CompressionKind
	Dictionary         *compression.Dictionary
	Registry           *compression.Registry
}

// Tree is the ordered leaf set of one database directory.
type Tree struct {
	fs  disk.FS
	dir string

	seed                uint64
	maxLeafBytes        int64
	mergeThresholdBytes int64
	targetBlockSize     int
	bloomFalsePositive  float64
	compression         segment.CompressionKind
	dictionary          *compression.Dictionary
	registry            *compression.Registry
	merger              *leaf.Merger

	// leaves is the published snapshot, sorted by low key and replaced whole on
	// every structural change.
	leaves atomic.Pointer[[]*leaf.Leaf]

	// gen guards readers against the transient states of installing a merge. It
	// is odd while the result is being installed and is bumped on both sides of
	// that window, so a lookup that observed a torn state sees a different
	// generation and retries. It replaces a lock on the read path: the writer
	// side is two atomic adds per merge, and the window covers only the install
	// itself, never the segment build.
	gen atomic.Uint64

	// mu serializes structural changes: merges, splits, and manifest updates.
	// It is never taken by lookups or by buffered writes.
	mu sync.Mutex
	// manifest is the in-memory catalog; it is only ever read or replaced while
	// holding mu.
	manifest *manifest.Manifest
	// nextLeafID hands out leaf ids for split outputs. Leaf ids are not stored
	// in the manifest header, so a reopened tree continues after the highest id
	// it found.
	nextLeafID uint64
	// retryMerge remembers leaves whose merge failed after freezing their
	// buffer. Their data now sits in a frozen layer that BufferedLen does not
	// report, so without this set a later merge pass would skip them forever.
	retryMerge map[uint64]struct{}

	retired    chan retiredReader
	retireStop chan struct{}
	retireDone chan struct{}

	closeOnce sync.Once
	closed    atomic.Bool
}

// New creates a tree with one empty leaf covering the whole keyspace.
func New(opts Options) (*Tree, error) {
	t, err := newTree(opts)
	if err != nil {
		return nil, err
	}

	t.manifest = manifest.New()
	first := t.newLeaf(t.allocateLeafID(), nil, nil, 0)
	if err := t.manifest.AddLeaf(manifest.LeafRecord{LeafID: first.ID()}); err != nil {
		_ = first.Close()
		t.stopRetireWorker()
		return nil, err
	}
	t.publish([]*leaf.Leaf{first})
	return t, nil
}

// Open rebuilds a tree from a manifest, reopening the segment of every leaf.
//
// The manifest is cloned, so the caller keeps ownership of the value it passes
// in. A leaf record without a segment id is opened as a leaf that holds only
// buffered data, which is the normal state of a leaf that was never merged.
func Open(opts Options, m *manifest.Manifest) (*Tree, error) {
	if m == nil {
		return nil, ErrNilManifest
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}

	t, err := newTree(opts)
	if err != nil {
		return nil, err
	}
	t.manifest = m.Clone()
	if t.manifest.NextSegmentID == 0 {
		t.manifest.NextSegmentID = 1
	}

	if len(t.manifest.Leaves) == 0 {
		first := t.newLeaf(t.allocateLeafID(), nil, nil, 0)
		if err := t.manifest.AddLeaf(manifest.LeafRecord{LeafID: first.ID()}); err != nil {
			_ = first.Close()
			t.stopRetireWorker()
			return nil, err
		}
		t.publish([]*leaf.Leaf{first})
		return t, nil
	}

	leaves := make([]*leaf.Leaf, 0, len(t.manifest.Leaves))
	for _, record := range t.manifest.Leaves {
		var reader *segment.Reader
		if record.SegmentID != 0 {
			path := segment.SegmentFileName(t.dir, record.SegmentID, record.SegmentVersion)
			reader, err = segment.OpenReader(t.fs, path, t.registry)
			if err != nil {
				closeLeaves(leaves)
				t.stopRetireWorker()
				return nil, fmt.Errorf("tree: open leaf %d segment: %w", record.LeafID, err)
			}
		}
		// The key count comes from the manifest: the segment file does not
		// record it, and without it the first merge after reopening would size
		// its bloom filter from the buffer alone.
		leaves = append(leaves, t.newLeaf(record.LeafID, record.LowKey, reader, int64(record.Keys)))
		if record.LeafID >= t.nextLeafID {
			t.nextLeafID = record.LeafID + 1
		}
	}
	t.publish(leaves)
	return t, nil
}

func newTree(opts Options) (*Tree, error) {
	if opts.FS == nil {
		return nil, ErrNilFilesystem
	}

	maxLeafBytes := opts.MaxLeafBytes
	if maxLeafBytes <= 0 {
		maxLeafBytes = DefaultMaxLeafBytes
	}
	mergeThreshold := opts.MergeThresholdBytes
	if mergeThreshold <= 0 {
		mergeThreshold = DefaultMergeThresholdBytes
	}

	t := &Tree{
		fs:                  opts.FS,
		dir:                 opts.Dir,
		seed:                opts.Seed,
		maxLeafBytes:        maxLeafBytes,
		mergeThresholdBytes: mergeThreshold,
		targetBlockSize:     opts.TargetBlockSize,
		bloomFalsePositive:  opts.BloomFalsePositive,
		compression:         opts.Compression,
		dictionary:          opts.Dictionary,
		registry:            opts.Registry,
		nextLeafID:          1,
		retryMerge:          make(map[uint64]struct{}),
		retired:             make(chan retiredReader, retiredReaderBufSize),
		retireStop:          make(chan struct{}),
		retireDone:          make(chan struct{}),
	}
	t.merger = &leaf.Merger{
		TargetBlockSize:    opts.TargetBlockSize,
		BloomFalsePositive: opts.BloomFalsePositive,
		Compression:        opts.Registry,
	}
	t.publish(nil)
	t.startRetireWorker()
	return t, nil
}

// Put buffers a value replacement for key.
//
// A rejected write means the leaf was replaced by a split while the write was
// waiting, so the key has to be looked up again in the published tree. The
// retry terminates because a split publishes its new leaves before it releases
// the leaf lock that rejected the write.
func (t *Tree) Put(key, value []byte) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}
	for {
		target := t.leafFor(key)
		if target == nil {
			return ErrNoLeaf
		}
		if target.TryPut(key, value) {
			return nil
		}
		runtime.Gosched()
	}
}

// Delete buffers a delete tombstone for key. See Put for the retry contract.
func (t *Tree) Delete(key []byte) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}
	for {
		target := t.leafFor(key)
		if target == nil {
			return ErrNoLeaf
		}
		if target.TryDelete(key) {
			return nil
		}
		runtime.Gosched()
	}
}

// Inc buffers a signed counter increment for key. See Put for the retry
// contract.
func (t *Tree) Inc(key []byte, delta int64) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}
	for {
		target := t.leafFor(key)
		if target == nil {
			return ErrNoLeaf
		}
		if target.TryInc(key, delta) {
			return nil
		}
		runtime.Gosched()
	}
}

// Get returns the materialized user value for key.
//
// The returned slice is a view of live buffer or block memory and must be
// copied before it is handed to a caller that may retain or mutate it.
//
// Get takes no lock. It brackets the lookup with the merge generation instead:
// while a merge result is being installed a leaf can briefly answer from a
// buffer layer that the new segment already covers, or from a segment that a
// split has just retired, so a lookup that saw a different generation before
// and after simply looks again.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	if t.closed.Load() {
		return nil, false, ErrClosed
	}
	if len(key) == 0 {
		return nil, false, nil
	}
	for {
		gen := t.gen.Load()
		if gen&1 != 0 {
			runtime.Gosched()
			continue
		}

		target := t.leafFor(key)
		if target == nil {
			return nil, false, ErrNoLeaf
		}
		value, ok, err := target.Get(key)
		if t.gen.Load() == gen {
			if err != nil {
				return nil, false, err
			}
			return value, ok, nil
		}
		runtime.Gosched()
	}
}

// Leaves returns the current leaf snapshot in key order.
func (t *Tree) Leaves() []*leaf.Leaf {
	current := t.snapshot()
	out := make([]*leaf.Leaf, len(current))
	copy(out, current)
	return out
}

// LeafCount returns the number of leaves in the current snapshot.
func (t *Tree) LeafCount() int {
	return len(t.snapshot())
}

// FreezeAll freezes the buffer of every leaf.
//
// A checkpoint calls this while writers are excluded, so the frozen set across
// the whole tree corresponds to one exact log position. The subsequent merge
// then writes precisely those operations into segments, which is what lets the
// caller record a watermark that is neither ahead of nor behind the data on
// disk. A watermark that is behind replays an increment already in a segment
// and doubles it.
//
// Leaves created after this call are not covered, which is why the caller must
// hold writers off across both the freeze and the watermark read.
func (t *Tree) FreezeAll() {
	for _, l := range t.snapshot() {
		l.FreezeBuffer()
	}
}

// BufferedBytes returns the buffered payload size across all leaves.
func (t *Tree) BufferedBytes() int64 {
	var total int64
	for _, l := range t.snapshot() {
		total += l.BufferedBytes()
	}
	return total
}

// SetAppliedSeq records the log position that merged segments already cover.
//
// It only updates memory; the value reaches disk with the next SaveManifest.
func (t *Tree) SetAppliedSeq(seq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.manifest.AppliedSeq = seq
}

// SaveManifest writes the current catalog to disk.
func (t *Tree) SaveManifest() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.saveManifestLocked(t.manifest)
}

// Manifest returns a copy of the current catalog.
func (t *Tree) Manifest() *manifest.Manifest {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.manifest.Clone()
}

// Close releases every leaf and drains the retired segment queue.
//
// It deliberately does not merge: flushing buffered writes is a decision for
// the layer that owns the write-ahead log, which has to move its watermark in
// the same step.
func (t *Tree) Close() error {
	if t == nil {
		return nil
	}

	var err error
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		for _, l := range t.snapshot() {
			if closeErr := l.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
		t.stopRetireWorker()
	})
	return err
}

func (t *Tree) saveManifestLocked(m *manifest.Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return manifest.Save(t.fs, manifest.FileName(t.dir), m)
}

func (t *Tree) newLeaf(id uint64, lowKey []byte, reader *segment.Reader, segmentKeys int64) *leaf.Leaf {
	created := leaf.NewRangeLeaf(id, t.seed, lowKey, reader, t.merger, segmentKeys)
	created.SetRetireHook(t.retireReader)
	return created
}

// allocateLeafID must be called with mu held, or before the tree is published.
func (t *Tree) allocateLeafID() uint64 {
	id := t.nextLeafID
	t.nextLeafID++
	return id
}

func (t *Tree) snapshot() []*leaf.Leaf {
	current := t.leaves.Load()
	if current == nil {
		return nil
	}
	return *current
}

func (t *Tree) publish(leaves []*leaf.Leaf) {
	t.leaves.Store(&leaves)
}

func (t *Tree) leafFor(key []byte) *leaf.Leaf {
	return findLeaf(t.snapshot(), key)
}

// findLeaf returns the last leaf whose low key is not greater than key.
//
// Leaves are sorted by low key and the leftmost one has an empty low key, so
// the search only fails on an empty or malformed leaf set.
func findLeaf(leaves []*leaf.Leaf, key []byte) *leaf.Leaf {
	index := searchLeaf(leaves, key)
	if index < 0 {
		return nil
	}
	return leaves[index]
}

func searchLeaf(leaves []*leaf.Leaf, key []byte) int {
	lo, hi := 0, len(leaves)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(leaves[mid].LowKey(), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

func closeLeaves(leaves []*leaf.Leaf) {
	for _, l := range leaves {
		_ = l.Close()
	}
}
