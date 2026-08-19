package disk

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestMemFSLockIsExclusiveAndReusable(t *testing.T) {
	fs := NewMemFS()
	first, err := fs.Lock("LOCK")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Lock("LOCK"); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := fs.Lock("LOCK")
	if err != nil {
		t.Fatalf("reacquire lock: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOSLockIsExclusiveAndReusable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")
	first, err := DefaultFS.Lock(path)
	if errors.Is(err, ErrLockUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DefaultFS.Lock(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := DefaultFS.Lock(path)
	if err != nil {
		t.Fatalf("reacquire lock: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}
