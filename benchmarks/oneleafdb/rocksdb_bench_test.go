//go:build rocksdb

package oneleafdbbench

import (
	"testing"

	onedb "fusedb/internal/oneleafdb"

	"github.com/linxGnu/grocksdb"
)

func BenchmarkRocksDBPutNoSync(b *testing.B) {
	db, opts := openRocksDBForBench(b)
	defer db.Close()
	defer opts.Destroy()

	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	writeOptions.SetSync(false)

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(writeOptions, onedb.DBKey(i), value); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkRocksDBGet64K(b *testing.B) {
	db, opts := openSeededRocksDBForBench(b, benchSeedKeys)
	defer db.Close()
	defer opts.Destroy()

	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := db.Get(readOptions, onedb.DBKey(i%benchSeedKeys))
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], value.Data()...)
		value.Free()
	}
}

func BenchmarkRocksDBMixedPutGetNoSync(b *testing.B) {
	db, opts := openSeededRocksDBForBench(b, benchSeedKeys)
	defer db.Close()
	defer opts.Destroy()

	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	writeOptions.SetSync(false)
	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&1 == 0 {
			if err := db.Put(writeOptions, onedb.DBKey(benchSeedKeys+i), value); err != nil {
				b.Fatalf("put: %v", err)
			}
			continue
		}
		got, err := db.Get(readOptions, onedb.DBKey(i%benchSeedKeys))
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], got.Data()...)
		got.Free()
	}
}

func openRocksDBForBench(b *testing.B) (*grocksdb.DB, *grocksdb.Options) {
	b.Helper()

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, b.TempDir())
	if err != nil {
		opts.Destroy()
		b.Fatalf("open rocksdb: %v", err)
	}
	return db, opts
}

func openSeededRocksDBForBench(b *testing.B, keys int) (*grocksdb.DB, *grocksdb.Options) {
	b.Helper()

	db, opts := openRocksDBForBench(b)
	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	writeOptions.SetSync(false)

	value := make([]byte, benchValueSize)
	for i := 0; i < keys; i++ {
		if err := db.Put(writeOptions, onedb.DBKey(i), value); err != nil {
			db.Close()
			opts.Destroy()
			b.Fatalf("seed rocksdb %d: %v", i, err)
		}
	}
	return db, opts
}
