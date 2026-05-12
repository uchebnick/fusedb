package oneleafdb

import (
	"fmt"
	"testing"
)

// BenchmarkScalability tests OneLeaf performance at different dataset sizes
func BenchmarkScalability(b *testing.B) {
	sizes := []struct {
		name      string
		numKeys   int
		cacheSize int64
	}{
		{"10K", 10_000, 5 << 20},
		{"50K", 50_000, 5 << 20},
		{"100K", 100_000, 5 << 20},
		{"500K", 500_000, 5 << 20},
		{"1M", 1_000_000, 5 << 20},
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

			b.Logf("Inserting %d keys...", size.numKeys)
			value := make([]byte, 128) // 128 bytes like YCSB
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
			b.Logf("Buffered bytes: %d", db.BufferedBytes())

			b.ReportAllocs()
			b.ResetTimer()

			// Random reads
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

// BenchmarkWriteScalability tests write performance at scale
func BenchmarkWriteScalability(b *testing.B) {
	sizes := []struct {
		name      string
		threshold int64
	}{
		{"2MB", 2 << 20},
		{"10MB", 10 << 20},
		{"50MB", 50 << 20},
		{"100MB", 100 << 20},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			db, err := OpenDB(DBOptions{
				Dir:            b.TempDir(),
				ThresholdBytes: size.threshold,
				CacheBytes:     5 << 20,
			})
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			defer db.Close()

			value := make([]byte, 128)
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

			b.StopTimer()
			mergeCount := int(db.version)
			b.Logf("Triggered %d merges", mergeCount)
		})
	}
}
