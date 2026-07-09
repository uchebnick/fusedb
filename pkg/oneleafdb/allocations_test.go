package oneleafdb

import (
	"fmt"
	"testing"
)

func BenchmarkGetAllocations(b *testing.B) {
	db, err := OpenDB(DBOptions{
		Dir:            b.TempDir(),
		ThresholdBytes: 2 << 20, // 2MB threshold - will trigger merge
		CacheBytes:     5 << 20,
	})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	const numKeys = 10000 // 10K keys × 1KB = 10MB
	for i := 0; i < numKeys; i++ {
		key := DBKey(i)
		value := make([]byte, 1024)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := db.Put(key, value); err != nil {
			b.Fatalf("put key %d: %v", i, err)
		}
	}

	// Force merge to disk
	if err := db.Merge(); err != nil {
		b.Fatalf("merge: %v", err)
	}

	testKey := DBKey(42)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ok, err := db.Get(testKey)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("key not found")
		}
	}
}

func BenchmarkGetAllocationsCacheMiss(b *testing.B) {
	db, err := OpenDB(DBOptions{
		Dir:            b.TempDir(),
		ThresholdBytes: 2 << 20, // 2MB threshold
		CacheBytes:     512,     // Tiny cache to force misses
	})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	const numKeys = 50000 // 50K keys × 1KB = 50MB
	for i := 0; i < numKeys; i++ {
		key := DBKey(i)
		value := make([]byte, 1024)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := db.Put(key, value); err != nil {
			b.Fatalf("put key %d: %v", i, err)
		}
	}

	if err := db.Merge(); err != nil {
		b.Fatalf("merge: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := DBKey(i % numKeys)
		_, ok, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("key not found")
		}
	}
}

func TestGetAllocationLimit(t *testing.T) {
	db, err := OpenDB(DBOptions{
		Dir:            t.TempDir(),
		ThresholdBytes: 1 << 30,
		CacheBytes:     5 << 20,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := DBKey(i)
		value := make([]byte, 1024)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("put key %d: %v", i, err)
		}
	}

	testKey := DBKey(42)

	allocs := testing.AllocsPerRun(100, func() {
		_, ok, err := db.Get(testKey)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("key not found")
		}
	})

	const allocLimit = 5.0
	if allocs > allocLimit {
		t.Errorf("too many allocations: %.1f allocs/op, want <= %.1f", allocs, allocLimit)
		t.Logf("Run 'go test -bench=BenchmarkGetAllocations -benchmem' to see details")
	} else {
		t.Logf("allocation count: %.1f allocs/op (limit: %.1f)", allocs, allocLimit)
	}
}

func BenchmarkPutAllocations(b *testing.B) {
	db, err := OpenDB(DBOptions{
		Dir:            b.TempDir(),
		ThresholdBytes: 1 << 30,
		CacheBytes:     5 << 20,
	})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key:%016d", i))
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetFromDisk(b *testing.B) {
	db, err := OpenDB(DBOptions{
		Dir:            b.TempDir(),
		ThresholdBytes: 2 << 20, // 2MB threshold
		CacheBytes:     5 << 20,
	})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	const numKeys = 100000 // 100K keys × 1KB = 100MB
	b.Logf("Inserting %d keys...", numKeys)
	for i := 0; i < numKeys; i++ {
		key := DBKey(i)
		value := make([]byte, 1024)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := db.Put(key, value); err != nil {
			b.Fatalf("put key %d: %v", i, err)
		}
	}

	// Force merge to disk
	if err := db.Merge(); err != nil {
		b.Fatalf("merge: %v", err)
	}
	b.Logf("Merged to disk: %s", db.SegmentPath())

	// Clear cache to force disk reads
	db.cache.clear()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := DBKey(i % numKeys)
		_, ok, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("key not found")
		}
	}
}
