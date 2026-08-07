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

// TestGetAllocationLimit pins the allocation cost of a read, per read path.
//
// Get hands back a private copy of the value, so one allocation is the floor for
// any successful read and each path is budgeted by what it has to do on top of
// that. The limits are measured, not guessed:
//
//	cache hit    1 = the copy returned to the caller
//	segment miss 5 = that copy, two in the segment read path, and the two the
//	                 cache makes when it stores the entry (its own key string
//	                 and its own copy of the value)
//
// A single loose budget for "a Get" used to cover both, which meant the cheap
// path could regress all the way to the cost of the expensive one unnoticed.
//
// The log is off because Get never touches it, and its group-commit goroutine
// would otherwise be free to charge a stray allocation to the read under test.
func TestGetAllocationLimit(t *testing.T) {
	const numKeys = 1000
	const runs = 100

	newSeededDB := func(t *testing.T) *DB {
		t.Helper()
		db, err := OpenDB(DBOptions{
			Dir:            t.TempDir(),
			ThresholdBytes: 1 << 30, // merge only when this test asks for it
			CacheBytes:     5 << 20,
			DisableWAL:     true,
		})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Errorf("close db: %v", err)
			}
		})

		value := make([]byte, 1024)
		for i := 0; i < numKeys; i++ {
			for j := range value {
				value[j] = byte(i % 256)
			}
			if err := db.Put(DBKey(i), value); err != nil {
				t.Fatalf("put key %d: %v", i, err)
			}
		}
		return db
	}

	t.Run("cache hit", func(t *testing.T) {
		db := newSeededDB(t)

		// The first read populates the cache, so every measured read is a hit.
		testKey := DBKey(42)
		if _, ok, err := db.Get(testKey); err != nil || !ok {
			t.Fatalf("warm up get: ok=%v err=%v", ok, err)
		}

		allocs := testing.AllocsPerRun(runs, func() {
			_, ok, err := db.Get(testKey)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("key not found")
			}
		})

		const allocLimit = 1.0
		if allocs > allocLimit {
			t.Errorf("cache hit allocates %.1f allocs/op, want <= %.1f", allocs, allocLimit)
		}
		t.Logf("cache hit: %.1f allocs/op (limit %.1f)", allocs, allocLimit)
	})

	t.Run("segment miss", func(t *testing.T) {
		db := newSeededDB(t)
		if err := db.Merge(); err != nil {
			t.Fatalf("merge: %v", err)
		}

		// Each measured read touches a key for the first time, so it always
		// misses the cache and goes down to the segment. runs must stay well
		// under numKeys for that to hold. The keys are built up front: DBKey
		// formats a string, and that allocation belongs to the caller, not to
		// the read path being budgeted.
		keys := make([][]byte, numKeys)
		for i := range keys {
			keys[i] = DBKey(i)
		}

		next := 0
		allocs := testing.AllocsPerRun(runs, func() {
			_, ok, err := db.Get(keys[next%numKeys])
			next++
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("key not found")
			}
		})

		const allocLimit = 5.0
		if allocs > allocLimit {
			t.Errorf("segment miss allocates %.1f allocs/op, want <= %.1f", allocs, allocLimit)
			t.Logf("Run 'go test -bench=BenchmarkGetAllocations -benchmem' to see details")
		}
		t.Logf("segment miss: %.1f allocs/op (limit %.1f)", allocs, allocLimit)
	})
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

// BenchmarkGetFromDisk measures reads that have to come out of segments.
//
// The data is seeded into a directory, the database is closed, and the reads run
// against a freshly opened one. A reopen is the honest way to reach that state:
// closing checkpoints every leaf, so nothing is left in a write buffer, and the
// new database starts with an empty value cache.
func BenchmarkGetFromDisk(b *testing.B) {
	const numKeys = 100000 // 100K keys × 1KB = 100MB
	opts := DBOptions{
		Dir:            b.TempDir(),
		ThresholdBytes: 2 << 20, // 2MB threshold
		CacheBytes:     5 << 20,
		// The log has no part in a read benchmark, and leaving it on would make
		// seeding write the whole dataset twice.
		DisableWAL: true,
	}

	b.Logf("Inserting %d keys...", numKeys)
	seedClosedDB(b, opts, numKeys, 1024)

	db := reopenSeeded(b, opts)
	b.Logf("Reopened from disk: %d leaves", db.LeafCount())

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
