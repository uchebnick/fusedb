package tree

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/value"
)

const testDir = "db"

func testOptions(fs disk.FS) Options {
	return Options{
		FS:   fs,
		Dir:  testDir,
		Seed: 42,
	}
}

func newTestTree(t *testing.T, opts Options) *Tree {
	t.Helper()

	tree, err := New(opts)
	if err != nil {
		t.Fatalf("new tree: %v", err)
	}
	t.Cleanup(func() {
		_ = tree.Close()
	})
	return tree
}

func testKey(i int) []byte {
	return []byte(fmt.Sprintf("key%06d", i))
}

func testValue(i, size int) []byte {
	prefix := fmt.Sprintf("value-%06d-", i)
	if len(prefix) >= size {
		return []byte(prefix)
	}
	return []byte(prefix + strings.Repeat("x", size-len(prefix)))
}

func mustGet(t *testing.T, tree *Tree, key []byte) []byte {
	t.Helper()

	got, ok, err := tree.Get(key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if !ok {
		t.Fatalf("get %q: missing", key)
	}
	return got
}

func putRange(t *testing.T, tree *Tree, from, to, size int) {
	t.Helper()

	for i := from; i < to; i++ {
		if err := tree.Put(testKey(i), testValue(i, size)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
}

func checkRange(t *testing.T, tree *Tree, from, to, size int) {
	t.Helper()

	for i := from; i < to; i++ {
		got := mustGet(t, tree, testKey(i))
		if !bytes.Equal(got, testValue(i, size)) {
			t.Fatalf("key %d: got %q, want %q", i, got, testValue(i, size))
		}
	}
}

// checkLeafLayout verifies that leaves stay sorted, start at the empty low key,
// and therefore cover the keyspace without a hole or an overlap.
func checkLeafLayout(t *testing.T, tree *Tree) {
	t.Helper()

	leaves := tree.Leaves()
	if len(leaves) == 0 {
		t.Fatal("tree has no leaves")
	}
	if len(leaves[0].LowKey()) != 0 {
		t.Fatalf("leftmost leaf low key = %q, want empty", leaves[0].LowKey())
	}

	seen := make(map[uint64]struct{}, len(leaves))
	for i, l := range leaves {
		if _, dup := seen[l.ID()]; dup {
			t.Fatalf("duplicate leaf id %d", l.ID())
		}
		seen[l.ID()] = struct{}{}
		if i == 0 {
			continue
		}
		if bytes.Compare(leaves[i-1].LowKey(), l.LowKey()) >= 0 {
			t.Fatalf("leaf %d low key %q is not greater than %q", i, l.LowKey(), leaves[i-1].LowKey())
		}
	}
}

func checkManifest(t *testing.T, fs disk.FS) *manifest.Manifest {
	t.Helper()

	loaded, err := manifest.Load(fs, manifest.FileName(testDir))
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if err := loaded.Validate(); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	return loaded
}

func countSegments(t *testing.T, fs disk.FS) int {
	t.Helper()

	names, err := fs.List(testDir)
	if err != nil {
		t.Fatalf("list dir: %v", err)
	}
	count := 0
	for _, name := range names {
		if strings.HasSuffix(name, ".seg") {
			count++
		}
	}
	return count
}

func TestPutGetDeleteInc(t *testing.T) {
	fs := disk.NewMemFS()
	tree := newTestTree(t, testOptions(fs))

	putRange(t, tree, 0, 32, 64)
	checkRange(t, tree, 0, 32, 64)

	if _, ok, err := tree.Get([]byte("missing")); err != nil || ok {
		t.Fatalf("get missing: ok=%v err=%v", ok, err)
	}

	if err := tree.Delete(testKey(3)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, err := tree.Get(testKey(3)); err != nil || ok {
		t.Fatalf("get deleted: ok=%v err=%v", ok, err)
	}

	if err := tree.Inc([]byte("counter"), 7); err != nil {
		t.Fatalf("inc: %v", err)
	}
	if err := tree.Inc([]byte("counter"), -2); err != nil {
		t.Fatalf("inc: %v", err)
	}
	counter, err := value.DecodeInt64(mustGet(t, tree, []byte("counter")))
	if err != nil {
		t.Fatalf("decode counter: %v", err)
	}
	if counter != 5 {
		t.Fatalf("counter = %d, want 5", counter)
	}

	if tree.LeafCount() != 1 {
		t.Fatalf("leaf count = %d, want 1", tree.LeafCount())
	}
}

// TestEmptyKeyRejected pins the empty key down at the write path. The segment
// block format cannot encode it, so a buffered empty key would only fail later,
// as a merge that never completes for the whole leaf.
func TestEmptyKeyRejected(t *testing.T) {
	fs := disk.NewMemFS()
	tree := newTestTree(t, testOptions(fs))

	if err := tree.Put(nil, []byte("value")); err != ErrEmptyKey {
		t.Fatalf("put empty key: %v, want %v", err, ErrEmptyKey)
	}
	if err := tree.Delete([]byte{}); err != ErrEmptyKey {
		t.Fatalf("delete empty key: %v, want %v", err, ErrEmptyKey)
	}
	if err := tree.Inc(nil, 1); err != ErrEmptyKey {
		t.Fatalf("inc empty key: %v, want %v", err, ErrEmptyKey)
	}
	if _, ok, err := tree.Get(nil); err != nil || ok {
		t.Fatalf("get empty key: ok=%v err=%v", ok, err)
	}

	putRange(t, tree, 0, 10, 64)
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	checkRange(t, tree, 0, 10, 64)
}

func TestMergeWithoutSplitKeepsLeafIdentity(t *testing.T) {
	fs := disk.NewMemFS()
	tree := newTestTree(t, testOptions(fs))

	putRange(t, tree, 0, 200, 64)
	before := tree.Leaves()
	if len(before) != 1 {
		t.Fatalf("leaf count before merge = %d, want 1", len(before))
	}

	if err := tree.MergeAll(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	after := tree.Leaves()
	if len(after) != 1 {
		t.Fatalf("leaf count after merge = %d, want 1", len(after))
	}
	if after[0] != before[0] {
		t.Fatal("merge without split replaced the leaf instead of reusing it")
	}
	if after[0].ID() != before[0].ID() {
		t.Fatalf("leaf id changed from %d to %d", before[0].ID(), after[0].ID())
	}
	if after[0].Reader() == nil {
		t.Fatal("merged leaf has no segment")
	}
	if segments := countSegments(t, fs); segments != 1 {
		t.Fatalf("segment files = %d, want 1", segments)
	}

	loaded := checkManifest(t, fs)
	if len(loaded.Leaves) != 1 || loaded.Leaves[0].SegmentID == 0 {
		t.Fatalf("manifest leaves = %+v", loaded.Leaves)
	}
	checkRange(t, tree, 0, 200, 64)
}

func TestMergeSplitsLeaf(t *testing.T) {
	fs := disk.NewMemFS()
	opts := testOptions(fs)
	opts.MaxLeafBytes = 32 << 10
	tree := newTestTree(t, opts)

	const keys = 500
	putRange(t, tree, 0, keys, 256)

	if err := tree.MergeAll(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	leaves := tree.Leaves()
	if len(leaves) < 2 {
		t.Fatalf("leaf count = %d, want at least 2", len(leaves))
	}
	checkLeafLayout(t, tree)
	checkRange(t, tree, 0, keys, 256)

	loaded := checkManifest(t, fs)
	if len(loaded.Leaves) != len(leaves) {
		t.Fatalf("manifest has %d leaves, tree has %d", len(loaded.Leaves), len(leaves))
	}
	for i, record := range loaded.Leaves {
		if record.LeafID != leaves[i].ID() {
			t.Fatalf("manifest leaf %d id = %d, tree id = %d", i, record.LeafID, leaves[i].ID())
		}
		if !bytes.Equal(record.LowKey, leaves[i].LowKey()) {
			t.Fatalf("manifest leaf %d low key = %q, tree low key = %q", i, record.LowKey, leaves[i].LowKey())
		}
		if record.SegmentID == 0 {
			t.Fatalf("manifest leaf %d has no segment", i)
		}
	}

	// Every key must land in the leaf that owns its range.
	for i := 0; i < keys; i++ {
		key := testKey(i)
		owner := findLeaf(leaves, key)
		if owner == nil {
			t.Fatalf("key %d has no owning leaf", i)
		}
		if _, ok, err := owner.Get(key); err != nil || !ok {
			t.Fatalf("key %d not found in owning leaf %d: ok=%v err=%v", i, owner.ID(), ok, err)
		}
	}
}

func TestRepeatedMergesKeepSplitting(t *testing.T) {
	fs := disk.NewMemFS()
	opts := testOptions(fs)
	opts.MaxLeafBytes = 16 << 10
	tree := newTestTree(t, opts)

	previous := 0
	for round := 0; round < 4; round++ {
		putRange(t, tree, round*200, (round+1)*200, 256)
		if err := tree.MergeAll(); err != nil {
			t.Fatalf("round %d merge: %v", round, err)
		}

		count := tree.LeafCount()
		if count <= previous {
			t.Fatalf("round %d leaf count = %d, want more than %d", round, count, previous)
		}
		previous = count

		checkLeafLayout(t, tree)
		checkManifest(t, fs)
		checkRange(t, tree, 0, (round+1)*200, 256)
	}
}

func TestOpenRoundTrip(t *testing.T) {
	fs := disk.NewMemFS()
	opts := testOptions(fs)
	opts.MaxLeafBytes = 32 << 10

	tree, err := New(opts)
	if err != nil {
		t.Fatalf("new tree: %v", err)
	}

	const keys = 400
	putRange(t, tree, 0, keys, 256)
	if err := tree.Delete(testKey(7)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := tree.Inc([]byte("counter"), 11); err != nil {
		t.Fatalf("inc: %v", err)
	}
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	tree.SetAppliedSeq(1234)
	if err := tree.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	leavesBefore := tree.LeafCount()
	if leavesBefore < 2 {
		t.Fatalf("leaf count = %d, want at least 2", leavesBefore)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	loaded := checkManifest(t, fs)
	if loaded.AppliedSeq != 1234 {
		t.Fatalf("applied seq = %d, want 1234", loaded.AppliedSeq)
	}

	reopened, err := Open(opts, loaded)
	if err != nil {
		t.Fatalf("open tree: %v", err)
	}
	defer reopened.Close()

	if reopened.LeafCount() != leavesBefore {
		t.Fatalf("reopened leaf count = %d, want %d", reopened.LeafCount(), leavesBefore)
	}
	checkLeafLayout(t, reopened)
	for i := 0; i < keys; i++ {
		if i == 7 {
			if _, ok, err := reopened.Get(testKey(i)); err != nil || ok {
				t.Fatalf("deleted key survived reopen: ok=%v err=%v", ok, err)
			}
			continue
		}
		got := mustGet(t, reopened, testKey(i))
		if !bytes.Equal(got, testValue(i, 256)) {
			t.Fatalf("key %d after reopen = %q", i, got)
		}
	}
	counter, err := value.DecodeInt64(mustGet(t, reopened, []byte("counter")))
	if err != nil {
		t.Fatalf("decode counter: %v", err)
	}
	if counter != 11 {
		t.Fatalf("counter after reopen = %d, want 11", counter)
	}

	// A reopened tree must keep allocating fresh identities.
	putRange(t, reopened, keys, keys+200, 256)
	if err := reopened.MergeAll(); err != nil {
		t.Fatalf("merge after reopen: %v", err)
	}
	checkLeafLayout(t, reopened)
	checkManifest(t, fs)
	checkRange(t, reopened, 0, 6, 256)
}

func TestDeleteSurvivesMerge(t *testing.T) {
	fs := disk.NewMemFS()
	opts := testOptions(fs)
	opts.MaxLeafBytes = 16 << 10
	tree := newTestTree(t, opts)

	const keys = 300
	putRange(t, tree, 0, keys, 256)
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("first merge: %v", err)
	}

	for i := 0; i < keys; i += 3 {
		if err := tree.Delete(testKey(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("second merge: %v", err)
	}
	// A tombstone that only lived in the buffer must not be resurrected by the
	// segment it was merged into.
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("third merge: %v", err)
	}

	for i := 0; i < keys; i++ {
		got, ok, err := tree.Get(testKey(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if i%3 == 0 {
			if ok {
				t.Fatalf("key %d resurrected as %q", i, got)
			}
			continue
		}
		if !ok || !bytes.Equal(got, testValue(i, 256)) {
			t.Fatalf("key %d = %q ok=%v", i, got, ok)
		}
	}
}

func TestEmptyMergeDropsSegment(t *testing.T) {
	fs := disk.NewMemFS()
	tree := newTestTree(t, testOptions(fs))

	putRange(t, tree, 0, 50, 64)
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("first merge: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := tree.Delete(testKey(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("second merge: %v", err)
	}

	loaded := checkManifest(t, fs)
	if len(loaded.Leaves) != 1 {
		t.Fatalf("manifest leaves = %d, want 1", len(loaded.Leaves))
	}
	if loaded.Leaves[0].SegmentID != 0 {
		t.Fatalf("empty leaf still points at segment %d", loaded.Leaves[0].SegmentID)
	}
	if tree.Leaves()[0].Reader() != nil {
		t.Fatal("empty leaf still holds a segment reader")
	}
	for i := 0; i < 50; i++ {
		if _, ok, err := tree.Get(testKey(i)); err != nil || ok {
			t.Fatalf("key %d survived: ok=%v err=%v", i, ok, err)
		}
	}
}

func TestIncrementsSurviveMergeAndSplit(t *testing.T) {
	fs := disk.NewMemFS()
	opts := testOptions(fs)
	opts.MaxLeafBytes = 16 << 10
	tree := newTestTree(t, opts)

	counterKey := func(i int) []byte {
		return []byte(fmt.Sprintf("cnt%06d", i))
	}

	const counters = 200
	for round := 0; round < 3; round++ {
		for i := 0; i < counters; i++ {
			if err := tree.Inc(counterKey(i), int64(i+1)); err != nil {
				t.Fatalf("inc %d: %v", i, err)
			}
		}
		// Byte payloads are what push the merged output past MaxLeafBytes, so
		// the counters end up spread over several leaves.
		putRange(t, tree, round*150, (round+1)*150, 256)
		if err := tree.MergeAll(); err != nil {
			t.Fatalf("round %d merge: %v", round, err)
		}
		checkLeafLayout(t, tree)
	}

	if tree.LeafCount() < 2 {
		t.Fatalf("leaf count = %d, want at least 2", tree.LeafCount())
	}
	for i := 0; i < counters; i++ {
		got, err := value.DecodeInt64(mustGet(t, tree, counterKey(i)))
		if err != nil {
			t.Fatalf("decode counter %d: %v", i, err)
		}
		if want := int64(3 * (i + 1)); got != want {
			t.Fatalf("counter %d = %d, want %d", i, got, want)
		}
	}
}

// hookFS runs a callback the first time a segment file is created.
//
// That moment sits between the merge freezing the leaf buffer and the split
// handing the buffer over, which is exactly where a write has to survive being
// routed into the leaves the split is about to publish.
type hookFS struct {
	disk.FS

	once sync.Once
	hook func()
}

func (f *hookFS) Create(name string) (disk.File, error) {
	if f.hook != nil && strings.Contains(name, ".seg") {
		f.once.Do(f.hook)
	}
	return f.FS.Create(name)
}

func TestSplitHandoverKeepsPendingWrites(t *testing.T) {
	fs := &hookFS{FS: disk.NewMemFS()}
	opts := testOptions(fs)
	opts.MaxLeafBytes = 16 << 10

	tree := newTestTree(t, opts)

	const keys = 400
	putRange(t, tree, 0, keys, 256)

	// Writes issued from the hook land in the buffer the merge already froze
	// over, so they are the operations the handover has to replay into the new
	// leaves without clobbering anything newer.
	overwritten := func(i int) []byte {
		return []byte(fmt.Sprintf("pending-overwrite-%06d", i))
	}
	fs.hook = func() {
		for i := 0; i < keys; i += 10 {
			if err := tree.Put(testKey(i), overwritten(i)); err != nil {
				t.Errorf("pending put %d: %v", i, err)
			}
		}
		for i := 5; i < keys; i += 50 {
			if err := tree.Delete(testKey(i)); err != nil {
				t.Errorf("pending delete %d: %v", i, err)
			}
		}
	}

	if err := tree.MergeAll(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if tree.LeafCount() < 2 {
		t.Fatalf("leaf count = %d, want at least 2", tree.LeafCount())
	}
	checkLeafLayout(t, tree)
	checkManifest(t, fs)

	leaves := tree.Leaves()
	for i := 0; i < keys; i++ {
		key := testKey(i)
		got, ok, err := tree.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}

		switch {
		case i%50 == 5:
			if ok {
				t.Fatalf("key %d deleted during the split but still reads %q", i, got)
			}
			continue
		case i%10 == 0:
			if !ok || !bytes.Equal(got, overwritten(i)) {
				t.Fatalf("key %d = %q ok=%v, want %q", i, got, ok, overwritten(i))
			}
		default:
			if !ok || !bytes.Equal(got, testValue(i, 256)) {
				t.Fatalf("key %d = %q ok=%v, want the merged value", i, got, ok)
			}
		}

		// The handover must place an operation in the leaf that owns its range,
		// not merely somewhere in the tree.
		owner := findLeaf(leaves, key)
		if _, ownerOK, err := owner.Get(key); err != nil || !ownerOK {
			t.Fatalf("key %d missing from owning leaf %d: ok=%v err=%v", i, owner.ID(), ownerOK, err)
		}
	}
}

// TestConcurrentReadsDuringSplits exercises the lock-free read path while
// leaves are replaced underneath it.
//
// Writes and merges share one goroutine, and readers only ask for keys that an
// earlier merge already made durable. What is under test is the part the tree
// owns: swapping the leaf snapshot, retiring a segment, and detaching a leaf
// must never make a key that lives in a segment disappear. Reading keys that
// are still buffered would instead test internal/leaf, which drops them for a
// moment while Buffer.Freeze has emptied the active layer and not yet published
// the frozen one.
func TestConcurrentReadsDuringSplits(t *testing.T) {
	fs := disk.NewMemFS()
	opts := testOptions(fs)
	opts.MaxLeafBytes = 8 << 10
	tree := newTestTree(t, opts)

	const (
		readers        = 4
		rounds         = 6
		keysPerRound   = 150
		readerValueLen = 192
	)

	// merged is the number of keys that a completed merge already wrote into a
	// segment, so a reader below it must always find its key.
	var merged atomic.Int64
	done := make(chan struct{})

	var readGroup sync.WaitGroup
	readGroup.Add(readers)
	for r := 0; r < readers; r++ {
		go func(worker int) {
			defer readGroup.Done()

			next := worker
			for {
				select {
				case <-done:
					return
				default:
				}

				limit := int(merged.Load())
				if limit == 0 {
					continue
				}
				next = (next + 1) % limit
				got, ok, err := tree.Get(testKey(next))
				if err != nil {
					t.Errorf("get %d: %v", next, err)
					return
				}
				if !ok {
					t.Errorf("key %d missing during split", next)
					return
				}
				if !bytes.Equal(got, testValue(next, readerValueLen)) {
					t.Errorf("key %d = %q, want %q", next, got, testValue(next, readerValueLen))
					return
				}
			}
		}(r)
	}

	for round := 0; round < rounds; round++ {
		for i := round * keysPerRound; i < (round+1)*keysPerRound; i++ {
			if err := tree.Put(testKey(i), testValue(i, readerValueLen)); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
		if err := tree.MergeAll(); err != nil {
			t.Fatalf("round %d merge: %v", round, err)
		}
		merged.Store(int64((round + 1) * keysPerRound))
	}
	close(done)
	readGroup.Wait()

	if tree.LeafCount() < 2 {
		t.Fatalf("leaf count = %d, want at least 2", tree.LeafCount())
	}
	checkLeafLayout(t, tree)
	checkRange(t, tree, 0, rounds*keysPerRound, readerValueLen)
}

// TestConcurrentWritesDuringMerge is the full write-side stress for split
// handover ordering.
//
// It writes from several goroutines while merges and splits reshape the tree
// underneath them, which is the only way the handover ordering invariant gets
// exercised: pending operations must reach the new leaves before those leaves
// become visible. It previously failed on defects in internal/skiplist (a nil
// predecessor when a concurrent insert grew the list height) and internal/leaf
// (freezing a buffer without excluding writers, and a window where a key was
// visible in neither layer); all three are fixed.
func TestConcurrentWritesDuringMerge(t *testing.T) {
	fs := disk.NewMemFS()
	opts := testOptions(fs)
	opts.MaxLeafBytes = 8 << 10
	tree := newTestTree(t, opts)

	const (
		writers        = 8
		keysPerWriter  = 300
		valueSize      = 192
		deletedModulus = 17
	)

	var writeGroup sync.WaitGroup
	writeGroup.Add(writers)
	for w := 0; w < writers; w++ {
		go func(worker int) {
			defer writeGroup.Done()

			for i := 0; i < keysPerWriter; i++ {
				index := worker*keysPerWriter + i
				if err := tree.Put(testKey(index), testValue(index, valueSize)); err != nil {
					t.Errorf("put %d: %v", index, err)
					return
				}
				if index%deletedModulus == 0 {
					if err := tree.Delete(testKey(index)); err != nil {
						t.Errorf("delete %d: %v", index, err)
						return
					}
				}
			}
		}(w)
	}

	done := make(chan struct{})
	var mergeGroup sync.WaitGroup
	mergeGroup.Add(2)
	go func() {
		defer mergeGroup.Done()

		for {
			select {
			case <-done:
				return
			default:
			}
			if err := tree.MergeAll(); err != nil {
				t.Errorf("merge all: %v", err)
				return
			}
		}
	}()
	go func() {
		defer mergeGroup.Done()

		for {
			select {
			case <-done:
				return
			default:
			}
			for _, l := range tree.PendingMerge() {
				if err := tree.MergeLeaf(l); err != nil {
					t.Errorf("merge leaf %d: %v", l.ID(), err)
					return
				}
			}
		}
	}()

	// Readers run against the same tree so that lookups are exercised while
	// leaves are being replaced.
	var readGroup sync.WaitGroup
	readGroup.Add(1)
	go func() {
		defer readGroup.Done()

		for {
			select {
			case <-done:
				return
			default:
			}
			if _, _, err := tree.Get(testKey(0)); err != nil {
				t.Errorf("concurrent get: %v", err)
				return
			}
		}
	}()

	writeGroup.Wait()
	close(done)
	mergeGroup.Wait()
	readGroup.Wait()

	if err := tree.MergeAll(); err != nil {
		t.Fatalf("final merge: %v", err)
	}
	checkLeafLayout(t, tree)
	checkManifest(t, fs)
	if tree.LeafCount() < 2 {
		t.Fatalf("leaf count = %d, want at least 2", tree.LeafCount())
	}

	for index := 0; index < writers*keysPerWriter; index++ {
		got, ok, err := tree.Get(testKey(index))
		if err != nil {
			t.Fatalf("get %d: %v", index, err)
		}
		if index%deletedModulus == 0 {
			if ok {
				t.Fatalf("key %d should be deleted, got %q", index, got)
			}
			continue
		}
		if !ok {
			t.Fatalf("key %d missing", index)
		}
		if !bytes.Equal(got, testValue(index, valueSize)) {
			t.Fatalf("key %d = %q, want %q", index, got, testValue(index, valueSize))
		}
	}
}

func TestClosedTreeRejectsOperations(t *testing.T) {
	fs := disk.NewMemFS()
	tree, err := New(testOptions(fs))
	if err != nil {
		t.Fatalf("new tree: %v", err)
	}
	putRange(t, tree, 0, 10, 32)
	if err := tree.MergeAll(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	if err := tree.Put(testKey(0), nil); err != ErrClosed {
		t.Fatalf("put after close: %v", err)
	}
	if _, _, err := tree.Get(testKey(0)); err != ErrClosed {
		t.Fatalf("get after close: %v", err)
	}
}
