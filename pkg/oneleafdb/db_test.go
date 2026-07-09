package oneleafdb

import (
	"bytes"
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
