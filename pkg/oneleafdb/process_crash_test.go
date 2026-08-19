package oneleafdb

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/scheduler"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/value"
)

const (
	crashWorkerDirEnv    = "FUSEDB_CRASH_POINT_DIR"
	crashWorkerPointEnv  = "FUSEDB_CRASH_POINT"
	crashWorkerMarkerEnv = "FUSEDB_CRASH_POINT_MARKER"
)

type processCrashPoint string

const (
	crashAfterWALWrite        processCrashPoint = "wal-write"
	crashAfterWALSync         processCrashPoint = "wal-sync"
	crashAfterSegmentSync     processCrashPoint = "segment-sync"
	crashAfterSegmentRename   processCrashPoint = "segment-rename"
	crashAfterManifestSync    processCrashPoint = "manifest-sync"
	crashAfterManifestRename  processCrashPoint = "manifest-rename"
	crashAfterWALRotateSync   processCrashPoint = "wal-rotate-sync"
	crashAfterWALRotateRename processCrashPoint = "wal-rotate-rename"
)

// TestProcessCrashWorker is launched as a subprocess by TestProcessCrashMatrix.
// It must not use defer after arming the crash point: process death, not orderly
// cleanup, is the behavior under test.
func TestProcessCrashWorker(t *testing.T) {
	dir := os.Getenv(crashWorkerDirEnv)
	if dir == "" {
		return
	}
	point := processCrashPoint(os.Getenv(crashWorkerPointEnv))
	marker := os.Getenv(crashWorkerMarkerEnv)
	fs := &processCrashFS{FS: disk.DefaultFS, point: point, marker: marker}
	db, err := OpenDB(processCrashOptions(dir, fs))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open crash worker:", err)
		os.Exit(80)
	}

	switch point {
	case crashAfterWALWrite, crashAfterWALSync:
		fs.armed.Store(true)
		_, _ = db.ApplyOnce([]byte("m-event/purchase-2"), crashPurchaseMutations())
	default:
		if applied, err := db.ApplyOnce([]byte("m-event/purchase-2"), crashPurchaseMutations()); err != nil || !applied {
			fmt.Fprintln(os.Stderr, "prepare crash worker:", err)
			os.Exit(81)
		}
		fs.armed.Store(true)
		_ = db.Merge()
	}
	fmt.Fprintln(os.Stderr, "crash point was not reached:", point)
	os.Exit(82)
}

func TestProcessCrashMatrix(t *testing.T) {
	points := []processCrashPoint{
		crashAfterWALWrite,
		crashAfterWALSync,
		crashAfterSegmentSync,
		crashAfterSegmentRename,
		crashAfterManifestSync,
		crashAfterManifestRename,
		crashAfterWALRotateSync,
		crashAfterWALRotateRename,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "db")
			marker := filepath.Join(root, "crash-markers", string(point))
			seedProcessCrashDatabase(t, dir)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCrashWorker$")
			command.Env = append(os.Environ(),
				crashWorkerDirEnv+"="+dir,
				crashWorkerPointEnv+"="+string(point),
				crashWorkerMarkerEnv+"="+marker,
			)
			output, runErr := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("crash worker timeout: %v\n%s", ctx.Err(), output)
			}
			if runErr == nil {
				t.Fatalf("crash worker exited cleanly instead of being killed\n%s", output)
			}
			markerData, err := disk.ReadFile(disk.DefaultFS, marker)
			if err != nil || string(markerData) != string(point) {
				t.Fatalf("crash point not confirmed: marker=%q err=%v child=%v\n%s",
					markerData, err, runErr, output)
			}

			recovered, err := OpenDB(processCrashOptions(dir, disk.DefaultFS))
			if err != nil {
				t.Fatalf("reopen after %s: %v", point, err)
			}
			if got := processCrashCounter(t, recovered); got != 2 {
				t.Fatalf("counter after %s = %d, want exactly 2", point, got)
			}
			order, found, err := recovered.Get([]byte("a-order/purchase-2"))
			if err != nil || !found || string(order) != "paid" {
				t.Fatalf("order after %s = (%q,%v,%v)", point, order, found, err)
			}
			if applied, err := recovered.ApplyOnce([]byte("m-event/purchase-2"), crashPurchaseMutations()); err != nil || applied {
				t.Fatalf("duplicate event after %s = (%v,%v)", point, applied, err)
			}
			if _, err := recovered.Verify(context.Background()); err != nil {
				t.Fatalf("verify after %s: %v", point, err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatalf("close after %s: %v", point, err)
			}
			assertCanonicalSegmentRootSet(t, dir)
		})
	}
}

type processCrashFS struct {
	disk.FS
	point  processCrashPoint
	marker string
	armed  atomic.Bool
	fired  atomic.Bool
}

func (f *processCrashFS) Create(name string) (disk.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &processCrashFile{File: file, fs: f, path: name}, nil
}

func (f *processCrashFS) Open(name string) (disk.File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &processCrashFile{File: file, fs: f, path: name}, nil
}

func (f *processCrashFS) OpenReadWrite(name string) (disk.File, error) {
	file, err := f.FS.OpenReadWrite(name)
	if err != nil {
		return nil, err
	}
	return &processCrashFile{File: file, fs: f, path: name}, nil
}

func (f *processCrashFS) Rename(oldname, newname string) error {
	if err := f.FS.Rename(oldname, newname); err != nil {
		return err
	}
	switch f.point {
	case crashAfterSegmentRename:
		if strings.HasSuffix(newname, ".seg") {
			f.crash()
		}
	case crashAfterManifestRename:
		if filepath.Base(newname) == manifest.DefaultFileName {
			f.crash()
		}
	case crashAfterWALRotateRename:
		if filepath.Base(newname) == "wal.log" && strings.HasSuffix(oldname, ".rotate.tmp") {
			f.crash()
		}
	}
	return nil
}

type processCrashFile struct {
	disk.File
	fs   *processCrashFS
	path string
}

func (f *processCrashFile) WriteAt(data []byte, offset int64) (int, error) {
	n, err := f.File.WriteAt(data, offset)
	if err == nil && f.fs.point == crashAfterWALWrite && filepath.Base(f.path) == "wal.log" {
		f.fs.crash()
	}
	return n, err
}

func (f *processCrashFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	base := filepath.Base(f.path)
	switch f.fs.point {
	case crashAfterWALSync:
		if base == "wal.log" {
			f.fs.crash()
		}
	case crashAfterSegmentSync:
		if strings.HasSuffix(base, ".seg.tmp") {
			f.fs.crash()
		}
	case crashAfterManifestSync:
		if base == manifest.DefaultFileName+".tmp" {
			f.fs.crash()
		}
	case crashAfterWALRotateSync:
		if strings.HasSuffix(base, ".rotate.tmp") {
			f.fs.crash()
		}
	}
	return nil
}

func (f *processCrashFS) crash() {
	if !f.armed.Load() || !f.fired.CompareAndSwap(false, true) {
		return
	}
	if err := disk.WriteFileAtomically(disk.DefaultFS, f.marker, []byte(f.point)); err != nil {
		fmt.Fprintln(os.Stderr, "write crash marker:", err)
		os.Exit(90)
	}
	process, err := os.FindProcess(os.Getpid())
	if err == nil {
		_ = process.Kill()
	}
	os.Exit(137)
}

func processCrashOptions(dir string, fs disk.FS) DBOptions {
	return DBOptions{
		Dir:                              dir,
		FS:                               fs,
		WALSyncWrites:                    true,
		ThresholdBytes:                   1 << 30,
		MaxLeafBytes:                     4 << 10,
		DisableSchedulerModelPersistence: true,
		DictionaryTraining:               DictionaryTrainingConfig{Disabled: true},
		DictionaryGC:                     DictionaryGCConfig{Disabled: true},
		SchedulerConfig: scheduler.Config{
			PollInterval:      time.Millisecond,
			ObservationWindow: 5 * time.Millisecond,
			QuietConfirm:      time.Millisecond,
			TargetReadP99:     time.Second,
			TargetWriteP99:    time.Second,
		},
	}
}

func seedProcessCrashDatabase(t *testing.T, dir string) {
	t.Helper()
	db, err := OpenDB(processCrashOptions(dir, disk.DefaultFS))
	if err != nil {
		t.Fatal(err)
	}
	for index := range 128 {
		key := []byte(fmt.Sprintf("seed/%04d", index))
		value := []byte(strings.Repeat("x", 128))
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Inc([]byte("z-counter"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func processCrashCounter(t *testing.T, db *DB) int64 {
	t.Helper()
	raw, found, err := db.Get([]byte("z-counter"))
	if err != nil || !found {
		t.Fatalf("counter = (%q,%v,%v)", raw, found, err)
	}
	got, err := value.DecodeInt64(raw)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func crashPurchaseMutations() []Mutation {
	return []Mutation{
		PutMutation([]byte("a-order/purchase-2"), []byte("paid")),
		IncMutation([]byte("z-counter"), 1),
	}
}

func assertCanonicalSegmentRootSet(t *testing.T, dir string) {
	t.Helper()
	snapshot, err := manifest.Load(disk.DefaultFS, manifest.FileName(dir))
	if err != nil {
		t.Fatal(err)
	}
	live := make(map[string]struct{}, len(snapshot.Leaves))
	for _, leaf := range snapshot.Leaves {
		if leaf.SegmentID != 0 {
			live[filepath.Base(segment.SegmentFileName(dir, leaf.SegmentID, leaf.SegmentVersion))] = struct{}{}
		}
	}
	names, err := disk.DefaultFS.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		base := filepath.Base(name)
		_, _, temporary, ok := segment.ParseSegmentFileName(base)
		if !ok {
			continue
		}
		if temporary {
			t.Fatalf("temporary segment survived recovery: %s", base)
		}
		if _, referenced := live[base]; !referenced {
			t.Fatalf("orphan segment survived recovery: %s", base)
		}
	}
}
