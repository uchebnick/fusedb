package faultfs

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

func TestRuleFiresOnSelectedOccurrenceAndPath(t *testing.T) {
	base := disk.NewMemFS()
	fs := New(base)
	for _, name := range []string{"a", "target"} {
		file, err := fs.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	fs.Arm(Rule{Operation: OpOpen, PathSuffix: "target", At: 2, Err: syscall.EIO})
	if file, err := fs.Open("a"); err != nil {
		t.Fatal(err)
	} else if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if file, err := fs.Open("target"); err != nil {
		t.Fatal(err)
	} else if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Open("target"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("second matching open error = %v", err)
	}
	if !fs.Fired() || fs.Seen() != 2 {
		t.Fatalf("fault state fired=%v seen=%d", fs.Fired(), fs.Seen())
	}
}

func TestOSAtomicRenameThenInjectedDirectorySyncFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")
	oldData := []byte("old-complete-state")
	newData := []byte("new-complete-state")
	if err := disk.WriteFileAtomically(disk.DefaultFS, path, oldData); err != nil {
		t.Fatal(err)
	}
	fs := New(disk.DefaultFS)
	fs.Arm(Rule{Operation: OpSyncDir, PathSuffix: dir, Err: syscall.EIO})
	if err := disk.WriteFileAtomically(fs, path, newData); !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("write error = %v, want ErrCommitUncertain", err)
	}
	got, err := disk.ReadFile(disk.DefaultFS, path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatalf("visible target = %q, want complete new data", got)
	}
}

func TestWriteRulesModelShortZeroAndENOSPC(t *testing.T) {
	for _, test := range []struct {
		name      string
		rule      Rule
		wantN     int
		wantError error
	}{
		{name: "short progress", rule: Rule{Operation: OpWrite, MaxBytes: 2}, wantN: 2},
		{name: "zero progress", rule: Rule{Operation: OpWrite, ZeroProgress: true}},
		{name: "partial ENOSPC", rule: Rule{Operation: OpWrite, MaxBytes: 3, Err: syscall.ENOSPC}, wantN: 3, wantError: syscall.ENOSPC},
		{name: "immediate ENOSPC", rule: Rule{Operation: OpWrite, Err: syscall.ENOSPC}, wantError: syscall.ENOSPC},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := New(disk.NewMemFS())
			file, err := fs.Create("file")
			if err != nil {
				t.Fatal(err)
			}
			fs.Arm(test.rule)
			n, err := file.Write([]byte("payload"))
			if n != test.wantN || !errors.Is(err, test.wantError) {
				t.Fatalf("write = (%d,%v), want (%d,%v)", n, err, test.wantN, test.wantError)
			}
			if !fs.Fired() {
				t.Fatal("fault did not fire")
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestZeroProgressRuleIsRejectedByWriteAll(t *testing.T) {
	fs := New(disk.NewMemFS())
	file, err := fs.Create("file")
	if err != nil {
		t.Fatal(err)
	}
	fs.Arm(Rule{Operation: OpWrite, ZeroProgress: true})
	if err := disk.WriteAll(file, []byte("payload")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteAll error = %v, want io.ErrShortWrite", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAfterRulePerformsOperationBeforeReturningFailure(t *testing.T) {
	base := disk.NewMemFS()
	fs := New(base)
	fs.Arm(Rule{Operation: OpCreate, PathSuffix: "visible", After: true, Err: syscall.EIO})
	if _, err := fs.Create("visible"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("create error = %v", err)
	}
	if _, err := base.Stat("visible"); err != nil {
		t.Fatalf("after-create did not leave visible result: %v", err)
	}
}
