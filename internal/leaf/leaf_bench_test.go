package leaf

import (
	"fmt"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/skiplist"
	"github.com/uchebnick/fusedb/internal/value"
)

var (
	benchValueSink []byte
	benchBoolSink  bool
	benchErrSink   error
)

func BenchmarkLeafGetActiveHit64K(b *testing.B) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	valueBytes := []byte("value")
	for i := 0; i < 64*1024; i++ {
		leaf.Put(benchKey(i), valueBytes)
	}
	key := benchKey(32768)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, ok, err := leaf.Get(key)
		if err != nil || !ok {
			b.Fatalf("get active: ok=%v err=%v", ok, err)
		}
		benchValueSink = value
	}
}

func BenchmarkLeafGetFrozenHit64K(b *testing.B) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	valueBytes := []byte("value")
	for i := 0; i < 64*1024; i++ {
		leaf.Put(benchKey(i), valueBytes)
	}
	if !leaf.buffer.Freeze() {
		b.Fatal("freeze failed")
	}
	key := benchKey(32768)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, ok, err := leaf.Get(key)
		if err != nil || !ok {
			b.Fatalf("get frozen: ok=%v err=%v", ok, err)
		}
		benchValueSink = value
	}
}

func BenchmarkLeafGetSegmentHit64K(b *testing.B) {
	fs := disk.NewMemFS()
	reader := benchSegmentReader(b, fs, "segments", 1, 1, 64*1024)
	defer reader.Close()

	leaf := NewLeaf(1, 42, reader, &Merger{})
	key := benchKey(32768)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, ok, err := leaf.Get(key)
		if err != nil || !ok {
			b.Fatalf("get segment: ok=%v err=%v", ok, err)
		}
		benchValueSink = value
	}
}

func BenchmarkLeafGetMiss64K(b *testing.B) {
	fs := disk.NewMemFS()
	reader := benchSegmentReader(b, fs, "segments", 1, 1, 64*1024)
	defer reader.Close()

	leaf := NewLeaf(1, 42, reader, &Merger{})
	key := []byte("key:99999999")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, ok, err := leaf.Get(key)
		if err != nil || ok {
			b.Fatalf("get miss: ok=%v err=%v", ok, err)
		}
		benchValueSink = value
	}
}

func BenchmarkLeafPutNew(b *testing.B) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	valueBytes := []byte("value")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leaf.Put(benchKey(i), valueBytes)
	}
}

func BenchmarkLeafPutExisting(b *testing.B) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	key := []byte("key:00000001")
	valueBytes := []byte("value")
	leaf.Put(key, valueBytes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leaf.Put(key, valueBytes)
	}
}

func BenchmarkLeafDeleteExisting(b *testing.B) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	key := []byte("key:00000001")
	leaf.Put(key, []byte("value"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leaf.Delete(key)
	}
}

func BenchmarkLeafIncExisting(b *testing.B) {
	leaf := NewLeaf(1, 42, nil, &Merger{})
	key := []byte("counter")
	leaf.Inc(key, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leaf.Inc(key, 1)
	}
}

func BenchmarkLeafMerge64K_1KUpdates(b *testing.B) {
	const (
		baseKeys   = 64 * 1024
		updateKeys = 1024
	)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fs := disk.NewMemFS()
		reader := benchSegmentReader(b, fs, "segments", uint64(i+1), 1, baseKeys)
		leaf := NewLeaf(1, 42, reader, &Merger{})
		for j := 0; j < updateKeys; j++ {
			leaf.Put(benchKey(j*16), []byte("updated-value"))
		}
		opts := segment.Options{
			FS:                 fs,
			Dir:                "segments",
			SegmentID:          uint64(i + 1),
			Version:            2,
			ExpectedKeys:       baseKeys,
			TargetBlockSize:    segment.DefaultTargetBlockSize,
			BloomFalsePositive: 0.01,
			Compression:        segment.CompressionNone,
		}

		b.StartTimer()
		err := leaf.Merge(opts)
		b.StopTimer()

		if err != nil {
			b.Fatalf("merge: %v", err)
		}
		if reader := leaf.reader.Load(); reader != nil {
			_ = leaf.Close()
		}
	}
}

func BenchmarkLeafMerge64K_1KUpdatesOSFS(b *testing.B) {
	const (
		baseKeys   = 64 * 1024
		updateKeys = 1024
	)

	dir := b.TempDir()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		reader := benchSegmentReader(b, disk.DefaultFS, dir, uint64(10_000+i), 1, baseKeys)
		leaf := NewLeaf(1, 42, reader, &Merger{})
		for j := 0; j < updateKeys; j++ {
			leaf.Put(benchKey(j*16), []byte("updated-value"))
		}
		opts := segment.Options{
			FS:                 disk.DefaultFS,
			Dir:                dir,
			SegmentID:          uint64(10_000 + i),
			Version:            2,
			ExpectedKeys:       baseKeys,
			TargetBlockSize:    segment.DefaultTargetBlockSize,
			BloomFalsePositive: 0.01,
			Compression:        segment.CompressionNone,
		}

		b.StartTimer()
		err := leaf.Merge(opts)
		b.StopTimer()

		if err != nil {
			b.Fatalf("merge: %v", err)
		}
		if reader := leaf.reader.Load(); reader != nil {
			_ = leaf.Close()
			_ = reader.Segment().Remove()
		}
	}
}

func BenchmarkMergerMerge64K_1KUpdates(b *testing.B) {
	const (
		baseKeys   = 64 * 1024
		updateKeys = 1024
	)

	fs := disk.NewMemFS()
	reader := benchSegmentReader(b, fs, "segments", 1, 1, baseKeys)
	defer reader.Close()

	updates := skiplist.NewSkipList(42)
	for i := 0; i < updateKeys; i++ {
		updates.Apply(benchKey(i*16), ops.NewPut(value.EncodeBytes([]byte("updated-value"))))
	}

	merger := &Merger{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opts := segment.Options{
			FS:                 fs,
			Dir:                "merge-out",
			SegmentID:          uint64(i + 1),
			Version:            1,
			ExpectedKeys:       baseKeys,
			TargetBlockSize:    segment.DefaultTargetBlockSize,
			BloomFalsePositive: 0.01,
			Compression:        segment.CompressionNone,
		}
		merged, err := merger.merge(reader.Iter(), nil, updates.Iter(), opts)
		if err != nil {
			b.Fatalf("merge: %v", err)
		}
		_ = merged.Remove()
	}
}

func BenchmarkMergerMerge64K_1KUpdatesOSFS(b *testing.B) {
	const (
		baseKeys   = 64 * 1024
		updateKeys = 1024
	)

	dir := b.TempDir()
	reader := benchSegmentReader(b, disk.DefaultFS, dir, 1, 1, baseKeys)
	defer reader.Close()

	updates := skiplist.NewSkipList(42)
	for i := 0; i < updateKeys; i++ {
		updates.Apply(benchKey(i*16), ops.NewPut(value.EncodeBytes([]byte("updated-value"))))
	}

	merger := &Merger{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opts := segment.Options{
			FS:                 disk.DefaultFS,
			Dir:                dir,
			SegmentID:          uint64(20_000 + i),
			Version:            1,
			ExpectedKeys:       baseKeys,
			TargetBlockSize:    segment.DefaultTargetBlockSize,
			BloomFalsePositive: 0.01,
			Compression:        segment.CompressionNone,
		}
		merged, err := merger.merge(reader.Iter(), nil, updates.Iter(), opts)
		if err != nil {
			b.Fatalf("merge: %v", err)
		}
		if err := merged.Remove(); err != nil {
			b.Fatalf("remove merged segment: %v", err)
		}
	}
}

func benchSegmentReader(
	b testing.TB,
	fs disk.FS,
	dir string,
	segmentID uint64,
	version uint64,
	keys int,
) *segment.Reader {
	b.Helper()

	seg, err := segment.NewSegment(segment.Options{
		FS:                 fs,
		Dir:                dir,
		SegmentID:          segmentID,
		Version:            version,
		ExpectedKeys:       keys,
		TargetBlockSize:    segment.DefaultTargetBlockSize,
		BloomFalsePositive: 0.01,
		Compression:        segment.CompressionNone,
	})
	if err != nil {
		b.Fatalf("new segment: %v", err)
	}

	for i := 0; i < keys; i++ {
		if err := seg.AppendKV(benchKey(i), value.EncodeBytes([]byte("value"))); err != nil {
			b.Fatalf("append %d: %v", i, err)
		}
	}
	if err := seg.Freeze(); err != nil {
		b.Fatalf("freeze segment: %v", err)
	}

	reader, err := segment.NewReader(seg, nil)
	if err != nil {
		b.Fatalf("new reader: %v", err)
	}
	return reader
}

func benchKey(i int) []byte {
	return []byte(fmt.Sprintf("key:%08d", i))
}
