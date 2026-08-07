package oneleafdb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/uchebnick/fusedb/internal/value"
)

func TestDBCacheInvalidatesOnPutDeleteAndInc(t *testing.T) {
	db, err := OpenDB(DBOptions{
		Dir:            t.TempDir(),
		ThresholdBytes: 1 << 30,
		CacheEntries:   8,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	key := []byte("key")
	if err := db.Put(key, []byte("one")); err != nil {
		t.Fatalf("put one: %v", err)
	}
	got, ok, err := db.Get(key)
	if err != nil || !ok {
		t.Fatalf("get one ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("got %q, want one", got)
	}

	// Everything below is about invalidation, and invalidation is only tested if
	// there is something cached to invalidate. Without this check the rest of the
	// test would still pass against a Get that never used the cache at all.
	if cached, ok := cachedValue(db, key); !ok || !bytes.Equal(cached, []byte("one")) {
		t.Fatalf("Get did not cache the value: cached=%q ok=%v", cached, ok)
	}

	got[0] = 'x'
	got, ok, err = db.Get(key)
	if err != nil || !ok {
		t.Fatalf("get cached one ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("cache returned shared value: %q", got)
	}

	if err := db.Put(key, []byte("two")); err != nil {
		t.Fatalf("put two: %v", err)
	}
	if cached, ok := cachedValue(db, key); ok {
		t.Fatalf("cache still serves %q after an overwrite", cached)
	}
	got, ok, err = db.Get(key)
	if err != nil || !ok {
		t.Fatalf("get two ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, []byte("two")) {
		t.Fatalf("got %q, want two", got)
	}

	if err := db.Delete(key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if cached, ok := cachedValue(db, key); ok {
		t.Fatalf("cache still serves %q after a delete", cached)
	}
	if got, ok, err := db.Get(key); err != nil || ok {
		t.Fatalf("get deleted = %q ok=%v err=%v, want miss", got, ok, err)
	}

	counterKey := []byte("counter")
	if err := db.Inc(counterKey, 1); err != nil {
		t.Fatalf("inc one: %v", err)
	}
	got, ok, err = db.Get(counterKey)
	if err != nil || !ok {
		t.Fatalf("get counter one ok=%v err=%v", ok, err)
	}
	gotCounter, err := value.DecodeInt64(got)
	if err != nil {
		t.Fatalf("decode counter one: %v", err)
	}
	if gotCounter != 1 {
		t.Fatalf("counter = %d, want 1", gotCounter)
	}
	if _, ok := cachedValue(db, counterKey); !ok {
		t.Fatal("Get did not cache the counter, so the next Inc has nothing to invalidate")
	}

	if err := db.Inc(counterKey, 2); err != nil {
		t.Fatalf("inc two: %v", err)
	}
	got, ok, err = db.Get(counterKey)
	if err != nil || !ok {
		t.Fatalf("get counter three ok=%v err=%v", ok, err)
	}
	gotCounter, err = value.DecodeInt64(got)
	if err != nil {
		t.Fatalf("decode counter three: %v", err)
	}
	if gotCounter != 3 {
		t.Fatalf("counter = %d, want 3", gotCounter)
	}
}

// cachedValue reports what the value cache would serve for key right now.
//
// It reads the same shard epoch the read path reads, so an entry left behind
// with a stale epoch counts as a miss, exactly as it does in Get.
func cachedValue(db *DB, key []byte) ([]byte, bool) {
	return db.cache.get(key, db.cacheEpoch[cacheShard(key)].Load())
}

// TestCacheInvalidationIsSharded pins what the sharded epochs are actually for.
//
// A write drops the entry for its own key outright, so every read stays correct
// even if the epoch arithmetic is wrong: the correctness assertions elsewhere
// cannot see a bug here. What the sharding buys is that a write leaves other
// keys' entries alone, and losing that would only show up as a collapsed hit
// rate under mixed load. So this test asserts it directly, through the cache.
func TestCacheInvalidationIsSharded(t *testing.T) {
	db, err := OpenDB(DBOptions{
		Dir:            t.TempDir(),
		ThresholdBytes: 1 << 30,
		CacheBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	probe, neighbour, outsider := shardedKeys(t)
	for _, key := range [][]byte{probe, neighbour, outsider} {
		if err := db.Put(key, []byte("value")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}

	// A marker value distinguishes a cache hit from a fresh read of the stored
	// value, which are otherwise identical from the outside.
	const marker = "cached-marker"
	probeShard := cacheShard(probe)
	db.cache.set(probe, []byte(marker), db.cacheEpoch[probeShard].Load())
	if got, _, err := db.Get(probe); err != nil || string(got) != marker {
		t.Fatalf("get probe = %q err=%v, want the cached marker", got, err)
	}

	before := epochSnapshot(db)
	if err := db.Put(outsider, []byte("other")); err != nil {
		t.Fatalf("put outsider: %v", err)
	}
	after := epochSnapshot(db)
	for shard := range before {
		bumped := after[shard] != before[shard]
		if want := uint64(shard) == cacheShard(outsider); bumped != want {
			t.Fatalf("shard %d bumped=%v, want %v after writing one key", shard, bumped, want)
		}
	}
	if got, _, err := db.Get(probe); err != nil || string(got) != marker {
		t.Fatalf("get probe = %q err=%v, want the cached marker: a write to another shard evicted it", got, err)
	}

	// A write to a key in the same shard is the one that has to invalidate the
	// probe, even though the probe's own entry is never touched.
	if err := db.Put(neighbour, []byte("other")); err != nil {
		t.Fatalf("put neighbour: %v", err)
	}
	if got, _, err := db.Get(probe); err != nil || string(got) != "value" {
		t.Fatalf("get probe = %q err=%v, want the stored value: the shard was not invalidated", got, err)
	}
}

func epochSnapshot(db *DB) [cacheEpochShards]uint64 {
	var snapshot [cacheEpochShards]uint64
	for i := range snapshot {
		snapshot[i] = db.cacheEpoch[i].Load()
	}
	return snapshot
}

// shardedKeys returns a key, a second key in the same epoch shard, and a third
// in a different one.
func shardedKeys(t *testing.T) (probe, neighbour, outsider []byte) {
	t.Helper()

	probe = []byte("shard-probe")
	probeShard := cacheShard(probe)
	for i := 0; neighbour == nil || outsider == nil; i++ {
		if i > 1_000_000 {
			t.Fatal("could not find keys on both sides of the shard split")
		}
		candidate := fmt.Appendf(nil, "shard-candidate-%d", i)
		switch {
		case cacheShard(candidate) == probeShard && neighbour == nil:
			neighbour = candidate
		case cacheShard(candidate) != probeShard && outsider == nil:
			outsider = candidate
		}
	}
	return probe, neighbour, outsider
}
