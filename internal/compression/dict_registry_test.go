package compression

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"fusedb/internal/disk"
)

func TestPersistentRegistrySaveAndLazyLoad(t *testing.T) {
	fs := disk.NewMemFS()
	dict := newTestDictionary(t, 7)

	writer, err := NewPersistentRegistry(fs, "dicts")
	if err != nil {
		t.Fatalf("new writer registry: %v", err)
	}
	if err := writer.Save(dict); err != nil {
		t.Fatalf("save dictionary: %v", err)
	}

	reader, err := NewPersistentRegistry(fs, "dicts")
	if err != nil {
		t.Fatalf("new reader registry: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	loaded, err := reader.MustGet(dict.ID())
	if err != nil {
		t.Fatalf("lazy load dictionary: %v", err)
	}
	loadedAgain, err := reader.MustGet(dict.ID())
	if err != nil {
		t.Fatalf("get cached dictionary: %v", err)
	}
	if loadedAgain != loaded {
		t.Fatal("registry did not reuse cached dictionary")
	}

	payload := []byte("tenant=a|region=eu|state=active|count=99")
	compressed, err := loaded.Compress(payload)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	decoded, err := loaded.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("decoded payload = %q, want %q", decoded, payload)
	}
}

func TestLRURegistryEvictsLeastRecentlyUsed(t *testing.T) {
	registry := NewLRURegistry(2)

	dict1 := newTestDictionary(t, 21)
	dict2 := newTestDictionary(t, 22)
	dict3 := newTestDictionary(t, 23)

	if err := registry.Add(dict1); err != nil {
		t.Fatalf("add dict1: %v", err)
	}
	if err := registry.Add(dict2); err != nil {
		t.Fatalf("add dict2: %v", err)
	}

	if _, ok := registry.Get(dict1.ID()); !ok {
		t.Fatal("expected dict1 before eviction")
	}
	if err := registry.Add(dict3); err != nil {
		t.Fatalf("add dict3: %v", err)
	}

	if _, ok := registry.Get(dict2.ID()); ok {
		t.Fatal("dict2 should be evicted as least recently used")
	}
	if _, ok := registry.Get(dict1.ID()); !ok {
		t.Fatal("dict1 should stay cached after touch")
	}
	if _, ok := registry.Get(dict3.ID()); !ok {
		t.Fatal("dict3 should stay cached")
	}
}

func TestPersistentLRURegistryReloadsEvictedDictionary(t *testing.T) {
	fs := disk.NewMemFS()
	registry, err := NewPersistentRegistry(fs, "dicts", 1)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	dict1 := newTestDictionary(t, 31)
	dict2 := newTestDictionary(t, 32)
	if err := registry.Save(dict1); err != nil {
		t.Fatalf("save dict1: %v", err)
	}
	if err := registry.Save(dict2); err != nil {
		t.Fatalf("save dict2: %v", err)
	}

	if _, ok := registry.Get(dict1.ID()); ok {
		t.Fatal("dict1 should be evicted from cache")
	}
	if _, ok := registry.Get(dict2.ID()); !ok {
		t.Fatal("dict2 should stay cached")
	}

	loaded1, err := registry.MustGet(dict1.ID())
	if err != nil {
		t.Fatalf("reload dict1: %v", err)
	}
	if loaded1.ID() != dict1.ID() {
		t.Fatalf("loaded id = %d, want %d", loaded1.ID(), dict1.ID())
	}
	if _, ok := registry.Get(dict2.ID()); ok {
		t.Fatal("dict2 should be evicted after dict1 reload")
	}
}

func TestDictionaryConcurrentCompressDecompress(t *testing.T) {
	dict := newTestDictionary(t, 11)
	payload := []byte("tenant=b|region=us|state=disabled|count=12345")

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				compressed, err := dict.Compress(payload)
				if err != nil {
					errs <- fmt.Errorf("worker %d compress: %w", worker, err)
					return
				}
				decoded, err := dict.Decompress(compressed)
				if err != nil {
					errs <- fmt.Errorf("worker %d decompress: %w", worker, err)
					return
				}
				if !bytes.Equal(decoded, payload) {
					errs <- fmt.Errorf("worker %d decoded payload mismatch", worker)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func newTestDictionary(t *testing.T, id uint32) *Dictionary {
	t.Helper()

	raw, err := TrainDictionary(TrainOptions{
		ID: id,
		Samples: [][]byte{
			[]byte("tenant=a|region=eu|state=active|count=1"),
			[]byte("tenant=a|region=eu|state=active|count=2"),
			[]byte("tenant=b|region=us|state=active|count=1"),
			[]byte("tenant=b|region=us|state=disabled|count=8"),
		},
	})
	if err != nil {
		t.Fatalf("train dictionary: %v", err)
	}

	dict, err := NewDictionaryLevel(id, raw, 3)
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	t.Cleanup(func() { _ = dict.Close() })
	return dict
}
