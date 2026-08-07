package leaf

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/value"
)

// splitTestOptions builds merge options that write into a fresh directory.
//
// maxBytes is deliberately a parameter: the interesting behaviour of
// MergeSplit only shows up once the output is forced to cut.
func splitTestOptions(t *testing.T, dir string, maxBytes int64) SplitOptions {
	t.Helper()

	nextID := uint64(100)
	return SplitOptions{
		Base: segment.Options{
			FS:                 disk.DefaultFS,
			Dir:                dir,
			TargetBlockSize:    segment.DefaultTargetBlockSize,
			BloomFalsePositive: segment.DefaultBloomFilterFalseRate,
			Compression:        segment.CompressionNone,
		},
		MaxBytes: maxBytes,
		Allocate: func(index int) (uint64, uint64, error) {
			if index == 0 {
				return 1, 1, nil
			}
			nextID++
			return nextID, 1, nil
		},
	}
}

func closeResults(results []MergeResult) {
	for _, result := range results {
		if result.Reader != nil {
			_ = result.Reader.Close()
		}
	}
}

func TestMergeSplitProducesSingleSegmentBelowThreshold(t *testing.T) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	defer leaf.Close()

	for i := range 50 {
		leaf.Put(fmt.Appendf(nil, "key%04d", i), []byte("value"))
	}

	results, err := leaf.MergeSplit(splitTestOptions(t, t.TempDir(), 0))
	if err != nil {
		t.Fatalf("merge split: %v", err)
	}
	defer closeResults(results)

	if len(results) != 1 {
		t.Fatalf("got %d outputs, want 1", len(results))
	}
	if results[0].Keys != 50 {
		t.Fatalf("keys = %d, want 50", results[0].Keys)
	}
	if len(results[0].LowKey) != 0 {
		t.Fatalf("first output low key = %q, want empty", results[0].LowKey)
	}
}

func TestMergeSplitCutsOnByteThreshold(t *testing.T) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	defer leaf.Close()

	const keys = 400
	payload := bytes.Repeat([]byte("x"), 64)
	for i := range keys {
		leaf.Put(fmt.Appendf(nil, "key%05d", i), payload)
	}

	// Roughly 400 * 75 bytes of payload, cut every 4 KB: several outputs.
	results, err := leaf.MergeSplit(splitTestOptions(t, t.TempDir(), 4<<10))
	if err != nil {
		t.Fatalf("merge split: %v", err)
	}
	defer closeResults(results)

	if len(results) < 2 {
		t.Fatalf("got %d outputs, want the merge to split", len(results))
	}

	var total int64
	for i, result := range results {
		total += result.Keys
		if i == 0 {
			if len(result.LowKey) != 0 {
				t.Fatalf("output 0 low key = %q, want empty", result.LowKey)
			}
			continue
		}
		if len(result.LowKey) == 0 {
			t.Fatalf("output %d has an empty low key", i)
		}
		if prev := results[i-1].LowKey; bytes.Compare(prev, result.LowKey) >= 0 {
			t.Fatalf("output %d low key %q does not follow %q", i, result.LowKey, prev)
		}
	}
	if total != keys {
		t.Fatalf("outputs hold %d keys, want %d", total, keys)
	}
}

func TestMergeSplitOutputsCoverEveryKey(t *testing.T) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	defer leaf.Close()

	const keys = 300
	payload := bytes.Repeat([]byte("v"), 48)
	for i := range keys {
		leaf.Put(fmt.Appendf(nil, "key%05d", i), payload)
	}

	results, err := leaf.MergeSplit(splitTestOptions(t, t.TempDir(), 4<<10))
	if err != nil {
		t.Fatalf("merge split: %v", err)
	}
	defer closeResults(results)

	// Every key must be readable from exactly the output whose range owns it.
	for i := range keys {
		key := fmt.Appendf(nil, "key%05d", i)

		owner := 0
		for j, result := range results {
			if j == 0 || bytes.Compare(key, result.LowKey) >= 0 {
				owner = j
			}
		}

		got, ok, err := results[owner].Reader.Get(key)
		if err != nil {
			t.Fatalf("get %s from output %d: %v", key, owner, err)
		}
		if !ok {
			t.Fatalf("key %s missing from output %d", key, owner)
		}
		decoded, err := value.DecodeBytes(got)
		if err != nil {
			t.Fatalf("decode %s: %v", key, err)
		}
		if !bytes.Equal(decoded, payload) {
			t.Fatalf("key %s: got %q want %q", key, decoded, payload)
		}
	}
}

func TestMergeSplitDropsDeletedKeys(t *testing.T) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	defer leaf.Close()

	leaf.Put([]byte("keep"), []byte("value"))
	leaf.Put([]byte("drop"), []byte("value"))
	leaf.Delete([]byte("drop"))

	results, err := leaf.MergeSplit(splitTestOptions(t, t.TempDir(), 0))
	if err != nil {
		t.Fatalf("merge split: %v", err)
	}
	defer closeResults(results)

	if len(results) != 1 {
		t.Fatalf("got %d outputs, want 1", len(results))
	}
	if results[0].Keys != 1 {
		t.Fatalf("keys = %d, want 1", results[0].Keys)
	}
	if _, ok, _ := results[0].Reader.Get([]byte("drop")); ok {
		t.Fatal("deleted key survived the merge")
	}
	if _, ok, _ := results[0].Reader.Get([]byte("keep")); !ok {
		t.Fatal("kept key is missing")
	}
}

func TestMergeSplitEmptyResultWhenEverythingDeleted(t *testing.T) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	defer leaf.Close()

	leaf.Put([]byte("only"), []byte("value"))
	leaf.Delete([]byte("only"))

	results, err := leaf.MergeSplit(splitTestOptions(t, t.TempDir(), 0))
	if err != nil {
		t.Fatalf("merge split: %v", err)
	}
	defer closeResults(results)

	if len(results) != 0 {
		t.Fatalf("got %d outputs, want none", len(results))
	}
}

func TestMergeSplitFailureLeavesNoSegmentFiles(t *testing.T) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	defer leaf.Close()

	payload := bytes.Repeat([]byte("x"), 64)
	for i := range 200 {
		leaf.Put(fmt.Appendf(nil, "key%05d", i), payload)
	}

	dir := t.TempDir()
	opts := splitTestOptions(t, dir, 2<<10)
	failAt := 2
	calls := 0
	opts.Allocate = func(index int) (uint64, uint64, error) {
		calls++
		if calls > failAt {
			return 0, 0, fmt.Errorf("allocator refused output %d", index)
		}
		return uint64(index + 1), 1, nil
	}

	if _, err := leaf.MergeSplit(opts); err == nil {
		t.Fatal("expected the merge to fail")
	}

	// A failed merge must not leave finalized segments behind: their ids were
	// never recorded, and a later merge reusing one would refuse to overwrite.
	entries, err := disk.DefaultFS.List(dir)
	if err != nil {
		t.Fatalf("list dir: %v", err)
	}
	for _, name := range entries {
		t.Errorf("leftover file after failed merge: %s", name)
	}
}

func TestMergeSplitPreservesIncrementsOverSegment(t *testing.T) {
	dir := t.TempDir()
	leaf := NewLeaf(1, 42, nil, &Merger{})
	defer leaf.Close()

	leaf.Inc([]byte("counter"), 7)
	first, err := leaf.MergeSplit(splitTestOptions(t, dir, 0))
	if err != nil {
		t.Fatalf("first merge: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("got %d outputs, want 1", len(first))
	}
	leaf.InstallMerge(first[0])

	leaf.Inc([]byte("counter"), 5)

	opts := splitTestOptions(t, dir, 0)
	opts.Allocate = func(int) (uint64, uint64, error) { return 1, 2, nil }
	second, err := leaf.MergeSplit(opts)
	if err != nil {
		t.Fatalf("second merge: %v", err)
	}
	defer closeResults(second)

	got, ok, err := second[0].Reader.Get([]byte("counter"))
	if err != nil || !ok {
		t.Fatalf("get counter: ok=%v err=%v", ok, err)
	}
	decoded, err := value.DecodeInt64(got)
	if err != nil {
		t.Fatalf("decode counter: %v", err)
	}
	if decoded != 12 {
		t.Fatalf("counter = %d, want 12", decoded)
	}
}
