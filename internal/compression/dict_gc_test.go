package compression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

type dictionaryGCSyncFaultFS struct {
	disk.FS
	fail bool
}

func (f *dictionaryGCSyncFaultFS) SyncDir(dir string) error {
	if f.fail {
		return fmt.Errorf("injected dictionary GC directory sync failure")
	}
	return f.FS.SyncDir(dir)
}

func TestPersistentRegistryCollectGarbageKeepsLiveAndBoundsPass(t *testing.T) {
	fs := disk.NewMemFS()
	registry, err := NewPersistentRegistry(fs, "dicts")
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	for _, id := range []uint32{1, 2, 3} {
		if err := registry.Save(newTestDictionary(t, id)); err != nil {
			t.Fatalf("save %d: %v", id, err)
		}
	}

	first, err := registry.CollectGarbage(context.Background(), map[uint32]struct{}{2: {}}, 1)
	if err != nil {
		t.Fatalf("first collect: %v", err)
	}
	if first.Scanned != 3 || first.Referenced != 1 || first.Deleted != 1 || !first.Remaining || first.ReclaimedBytes == 0 {
		t.Fatalf("first report = %+v", first)
	}
	second, err := registry.CollectGarbage(context.Background(), map[uint32]struct{}{2: {}}, 1)
	if err != nil {
		t.Fatalf("second collect: %v", err)
	}
	if second.Deleted != 1 || second.Remaining {
		t.Fatalf("second report = %+v", second)
	}
	if _, err := fs.Stat(DictionaryFileName("dicts", 2)); err != nil {
		t.Fatalf("live dictionary removed: %v", err)
	}
	for _, id := range []uint32{1, 3} {
		if _, err := fs.Stat(DictionaryFileName("dicts", id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dictionary %d stat = %v, want not exist", id, err)
		}
	}
}

func TestPersistentRegistryCollectGarbageHonorsCancellation(t *testing.T) {
	fs := disk.NewMemFS()
	registry, err := NewPersistentRegistry(fs, "dicts")
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := registry.Save(newTestDictionary(t, 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.CollectGarbage(ctx, nil, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("collect error = %v, want context.Canceled", err)
	}
	if _, err := fs.Stat(DictionaryFileName("dicts", 1)); err != nil {
		t.Fatalf("cancelled collection removed dictionary: %v", err)
	}
}

func TestPersistentRegistryCollectGarbageReportsDirectorySyncFailure(t *testing.T) {
	base := disk.NewMemFS()
	fs := &dictionaryGCSyncFaultFS{FS: base}
	registry, err := NewPersistentRegistry(fs, "dicts")
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := registry.Save(newTestDictionary(t, 1)); err != nil {
		t.Fatal(err)
	}
	fs.fail = true
	report, err := registry.CollectGarbage(context.Background(), nil, 1)
	if err == nil || report.Deleted != 1 {
		t.Fatalf("collect report=%+v error=%v, want visible deletion and sync error", report, err)
	}
	if _, statErr := base.Stat(DictionaryFileName("dicts", 1)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("removed dictionary stat = %v, want not exist", statErr)
	}
}

func TestParseDictionaryFileNameRejectsNonCanonicalNames(t *testing.T) {
	for _, test := range []struct {
		name string
		id   uint32
		ok   bool
	}{
		{name: "dict-00000001.zdict", id: 1, ok: true},
		{name: "dict-100000000.zdict", id: 100000000, ok: true},
		{name: "dict-1.zdict"},
		{name: "dict-00000000.zdict"},
		{name: "dict-00000001.zdict.tmp"},
		{name: "other-00000001.zdict"},
	} {
		id, ok := parseDictionaryFileName(test.name)
		if id != test.id || ok != test.ok {
			t.Fatalf("parse %q = (%d,%v), want (%d,%v)", test.name, id, ok, test.id, test.ok)
		}
	}
}
