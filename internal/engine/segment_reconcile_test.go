package engine

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/faultfs"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/segment"
)

func TestOpenRemovesOnlyUnreferencedCanonicalSegmentFiles(t *testing.T) {
	base := disk.NewMemFS()
	dir := "orphan-reconcile"
	seedFaultCounter(t, base, dir)
	snapshot, err := manifest.Load(base, manifest.FileName(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Leaves) != 1 || snapshot.Leaves[0].SegmentID == 0 {
		t.Fatalf("seed manifest = %+v", snapshot)
	}
	live := segment.SegmentFileName(dir, snapshot.Leaves[0].SegmentID, snapshot.Leaves[0].SegmentVersion)
	orphan := segment.SegmentFileName(dir, 999, 1)
	temporary := segment.SegmentTempFileName(dir, 888, 1)
	unrelated := filepath.Join(dir, "segment-999-v1.seg")
	for _, name := range []string{orphan, temporary, unrelated} {
		writeTestFile(t, base, name, []byte("leftover"))
	}

	db, err := OpenDB(faultDBOptions(dir, base))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{live, unrelated} {
		if _, err := base.Stat(name); err != nil {
			t.Fatalf("retained file %s: %v", name, err)
		}
	}
	for _, name := range []string{orphan, temporary} {
		if _, err := base.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphan file %s still exists: %v", name, err)
		}
	}
}

func TestOrphanReconcileFailuresAbortOpenAndRemainRetryable(t *testing.T) {
	for _, test := range []struct {
		name string
		rule faultfs.Rule
	}{
		{name: "remove", rule: faultfs.Rule{Operation: faultfs.OpRemove, PathSuffix: ".seg", Err: syscall.EIO}},
		{name: "directory sync", rule: faultfs.Rule{Operation: faultfs.OpSyncDir, Err: syscall.EIO}},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := disk.NewMemFS()
			dir := "orphan-reconcile-" + test.name
			seedFaultCounter(t, base, dir)
			orphan := segment.SegmentFileName(dir, 999, 1)
			writeTestFile(t, base, orphan, []byte("leftover"))
			fs := faultfs.New(base)
			fs.Arm(test.rule)
			if _, err := OpenDB(faultDBOptions(dir, fs)); !errors.Is(err, syscall.EIO) {
				t.Fatalf("open error = %v, want EIO", err)
			}
			if !fs.Fired() {
				t.Fatal("reconcile fault did not fire")
			}
			reopened, err := OpenDB(faultDBOptions(dir, base))
			if err != nil {
				t.Fatalf("retry open: %v", err)
			}
			if got := readFaultCounter(t, reopened); got != 1 {
				t.Fatalf("counter after retry = %d", got)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func writeTestFile(t *testing.T, fs disk.FS, name string, data []byte) {
	t.Helper()
	file, err := fs.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.WriteAll(file, data); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
