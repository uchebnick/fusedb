package oneleafdb

import (
	"testing"
)

// BenchmarkScalabilityLimitedRAM tests with constrained memory to force disk reads
func BenchmarkScalabilityLimitedRAM(b *testing.B) {
	sizes := []struct {
		name      string
		numKeys   int
		cacheSize int64
	}{
		{"10K_512KB", 10_000, 512 << 10},    // 512KB cache for 1.28MB data
		{"50K_512KB", 50_000, 512 << 10},    // 512KB cache for 6.4MB data
		{"100K_1MB", 100_000, 1 << 20},      // 1MB cache for 12.8MB data
		{"500K_2MB", 500_000, 2 << 20},      // 2MB cache for 64MB data
		{"1M_2MB", 1_000_000, 2 << 20},      // 2MB cache for 128MB data
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			db, err := OpenDB(DBOptions{
				Dir:            b.TempDir(),
				ThresholdBytes: 10 << 20, // 10MB threshold
				CacheBytes:     size.cacheSize,
			})
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			defer db.Close()

			b.Logf("Inserting %d keys with %d bytes cache...", size.numKeys, size.cacheSize)
			value := make([]byte, 128)
			for i := 0; i < size.numKeys; i++ {
				key := DBKey(i)
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

			segmentPath := db.SegmentPath()
			b.Logf("Merged to: %s", segmentPath)
			b.Logf("Cache size: %d bytes, Data size: ~%d MB", size.cacheSize, (size.numKeys*128)/(1<<20))

			// Clear cache to force cold reads
			db.cache.clear()

			b.ReportAllocs()
			b.ResetTimer()

			// Random reads - will mostly miss cache
			for i := 0; i < b.N; i++ {
				key := DBKey(i % size.numKeys)
				_, ok, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					b.Fatal("key not found")
				}
			}
		})
	}
}

// BenchmarkScalabilityColdReads tests pure disk reads with no cache
func BenchmarkScalabilityColdReads(b *testing.B) {
	sizes := []struct {
		name    string
		numKeys int
	}{
		{"10K", 10_000},
		{"50K", 50_000},
		{"100K", 100_000},
		{"500K", 500_000},
		{"1M", 1_000_000},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			db, err := OpenDB(DBOptions{
				Dir:            b.TempDir(),
				ThresholdBytes: 10 << 20,
				CacheBytes:     1024, // Tiny 1KB cache - essentially disabled
			})
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			defer db.Close()

			b.Logf("Inserting %d keys with minimal cache...", size.numKeys)
			value := make([]byte, 128)
			for i := 0; i < size.numKeys; i++ {
				key := DBKey(i)
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

			b.Logf("Data size: ~%d MB", (size.numKeys*128)/(1<<20))

			b.ReportAllocs()
			b.ResetTimer()

			// Sequential reads to avoid OS page cache benefits
			for i := 0; i < b.N; i++ {
				// Use different key each iteration to avoid cache
				key := DBKey((i * 997) % size.numKeys) // Prime number for better distribution
				_, ok, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					b.Fatal("key not found")
				}
			}
		})
	}
}

// BenchmarkScalabilityWorstCase tests with cache disabled and random access
func BenchmarkScalabilityWorstCase(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping worst-case benchmark in short mode")
	}

	sizes := []struct {
		name    string
		numKeys int
	}{
		{"100K", 100_000},
		{"500K", 500_000},
		{"1M", 1_000_000},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			db, err := OpenDB(DBOptions{
				Dir:            b.TempDir(),
				ThresholdBytes: 10 << 20,
				CacheBytes:     0, // No cache at all
			})
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			defer db.Close()

			b.Logf("Inserting %d keys with NO cache...", size.numKeys)
			value := make([]byte, 128)
			for i := 0; i < size.numKeys; i++ {
				key := DBKey(i)
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

			b.Logf("Worst case: no cache, random access, %d MB data", (size.numKeys*128)/(1<<20))

			b.ReportAllocs()
			b.ResetTimer()

			// Completely random access pattern
			for i := 0; i < b.N; i++ {
				key := DBKey((i * 7919) % size.numKeys) // Large prime for randomness
				_, ok, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					b.Fatal("key not found")
				}
			}
		})
	}
}
