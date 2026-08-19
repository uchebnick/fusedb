//go:build rocksdb

package kvbench

import "testing"

func TestRocksDBAdapterRoundTrip(t *testing.T) {
	factory := rocksDBFactory()
	if !factory.Info().Available {
		t.Fatal("RocksDB adapter is unavailable in a rocksdb-tagged build")
	}
	db, err := factory.Open(t.TempDir(), engineOptions{
		Durability: DurabilitySync, CacheBytes: 1 << 20, MemtableBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	value, found, err := db.Get([]byte("key"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("Get = %q, %v, %v", value, found, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
