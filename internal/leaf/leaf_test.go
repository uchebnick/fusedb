package leaf

import (
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/value"
)

func TestLeafGetAppliesIncOverSegmentValue(t *testing.T) {
	leaf := NewLeaf(11, 42, nil, &Merger{})
	defer leaf.Close()

	leaf.Inc([]byte("counter"), 10)
	if err := leaf.Merge(testSegmentOptions(t, leaf, 1)); err != nil {
		t.Fatalf("merge initial counter: %v", err)
	}

	leaf.Inc([]byte("counter"), 5)

	got, ok, err := leaf.Get([]byte("counter"))
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	if !ok {
		t.Fatal("counter missing")
	}
	if decoded, err := value.DecodeInt64(got); err != nil || decoded != 15 {
		t.Fatalf("counter = %d, %v; want 15", decoded, err)
	}
}

func TestLeafGetAppliesActiveIncOverFrozenInc(t *testing.T) {
	leaf := NewLeaf(12, 42, nil, &Merger{})
	defer leaf.Close()

	leaf.Inc([]byte("counter"), 10)
	if !leaf.buffer.Freeze() {
		t.Fatal("freeze buffer")
	}
	leaf.Inc([]byte("counter"), 5)

	got, ok, err := leaf.Get([]byte("counter"))
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	if !ok {
		t.Fatal("counter missing")
	}
	if decoded, err := value.DecodeInt64(got); err != nil || decoded != 15 {
		t.Fatalf("counter = %d, %v; want 15", decoded, err)
	}
}

func testSegmentOptions(t *testing.T, leaf *Leaf, version uint64) segment.Options {
	t.Helper()

	return segment.Options{
		FS:                 disk.DefaultFS,
		Dir:                t.TempDir(),
		SegmentID:          leaf.id,
		Version:            version,
		ExpectedKeys:       int(leaf.BufferedLen()),
		TargetBlockSize:    segment.DefaultTargetBlockSize,
		BloomFalsePositive: segment.DefaultBloomFilterFalseRate,
		Compression:        segment.CompressionNone,
	}
}
