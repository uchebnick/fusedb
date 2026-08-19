package engine

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
		{"10K_512KB", 10_000, 512 << 10}, // 512KB cache for 1.28MB data
		{"50K_512KB", 50_000, 512 << 10}, // 512KB cache for 6.4MB data
		{"100K_1MB", 100_000, 1 << 20},   // 1MB cache for 12.8MB data
		{"500K_2MB", 500_000, 2 << 20},   // 2MB cache for 64MB data
		{"1M_2MB", 1_000_000, 2 << 20},   // 2MB cache for 128MB data
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			opts := DBOptions{
				Dir:            b.TempDir(),
				ThresholdBytes: 10 << 20, // 10MB threshold
				CacheBytes:     size.cacheSize,
				DisableWAL:     true,
			}

			b.Logf("Inserting %d keys with %d bytes cache...", size.numKeys, size.cacheSize)
			seedClosedDB(b, opts, size.numKeys, 128)

			// Reopening is what makes the reads cold: the close checkpointed
			// every leaf into a segment and the new database starts with an
			// empty cache that is far too small to hold the dataset.
			db := reopenSeeded(b, opts)
			b.Logf("Cache size: %d bytes, data size: ~%d MB, leaves: %d",
				size.cacheSize, (size.numKeys*128)/(1<<20), db.LeafCount())

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

// BenchmarkScalabilityColdReads tests pure disk reads with a near-useless cache
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
			opts := DBOptions{
				Dir:            b.TempDir(),
				ThresholdBytes: 10 << 20,
				CacheBytes:     1024, // Tiny 1KB cache - essentially disabled
				DisableWAL:     true,
			}

			b.Logf("Inserting %d keys with minimal cache...", size.numKeys)
			seedClosedDB(b, opts, size.numKeys, 128)

			db := reopenSeeded(b, opts)
			b.Logf("Data size: ~%d MB, leaves: %d", (size.numKeys*128)/(1<<20), db.LeafCount())

			b.ReportAllocs()
			b.ResetTimer()

			// Strided reads to defeat any locality the previous lookup left behind
			for i := 0; i < b.N; i++ {
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
			opts := DBOptions{
				Dir:            b.TempDir(),
				ThresholdBytes: 10 << 20,
				// CacheBytes zero means "use the default", so it has to be
				// negative to actually run without a value cache. This benchmark
				// used to pass 0 and quietly measure a 5MB cache instead.
				CacheBytes: -1,
				DisableWAL: true,
			}

			b.Logf("Inserting %d keys with NO cache...", size.numKeys)
			seedClosedDB(b, opts, size.numKeys, 128)

			db := reopenSeeded(b, opts)
			if db.cache != nil {
				b.Fatal("value cache is enabled: this benchmark measures the uncached read path")
			}
			b.Logf("Worst case: no cache, random access, %d MB data, %d leaves",
				(size.numKeys*128)/(1<<20), db.LeafCount())

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
