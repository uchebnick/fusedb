package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/faultfs"
	"github.com/uchebnick/fusedb/internal/limits"
)

type syncGateFS struct {
	disk.FS
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (f *syncGateFS) arm() (<-chan struct{}, chan<- struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = true
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
	return f.entered, f.release
}

func (f *syncGateFS) OpenReadWrite(name string) (disk.File, error) {
	file, err := f.FS.OpenReadWrite(name)
	if err != nil {
		return nil, err
	}
	return &syncGateFile{File: file, fs: f}, nil
}

type syncGateFile struct {
	disk.File
	fs *syncGateFS
}

func (f *syncGateFile) Sync() error {
	f.fs.mu.Lock()
	if !f.fs.armed {
		f.fs.mu.Unlock()
		return f.File.Sync()
	}
	f.fs.armed = false
	entered := f.fs.entered
	release := f.fs.release
	f.fs.mu.Unlock()
	close(entered)
	<-release
	return f.File.Sync()
}

type uncertainSyncDirFS struct {
	disk.FS
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (f *uncertainSyncDirFS) arm() (<-chan struct{}, chan<- struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = true
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
	return f.entered, f.release
}

func (f *uncertainSyncDirFS) SyncDir(dir string) error {
	f.mu.Lock()
	if !f.armed {
		f.mu.Unlock()
		return f.FS.SyncDir(dir)
	}
	f.armed = false
	entered := f.entered
	release := f.release
	f.mu.Unlock()
	close(entered)
	<-release
	if err := f.FS.SyncDir(dir); err != nil {
		return err
	}
	return syscall.EIO
}

func TestIterateRejectsOversizedRecordBeforeAllocation(t *testing.T) {
	data := EmptyFile(1)
	data = append(data, recordKindPut)
	data = binary.AppendUvarint(data, 1)
	data = binary.AppendUvarint(data, limits.MaxKeyBytes+1)
	data = binary.AppendUvarint(data, 0)
	fs := disk.NewMemFS()
	file, err := fs.Create("wal.log")
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.WriteAll(file, data); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := Iterate(fs, "wal.log", nil); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Iterate error = %v, want ErrCorruptRecord", err)
	}
}

func TestWALWriteFaultsBecomeTerminalAndReopenable(t *testing.T) {
	tests := []struct {
		name          string
		rule          faultfs.Rule
		wantAppendErr error
		wantUncertain bool
		wantRecords   int
	}{
		{
			name:        "progress-making short write",
			rule:        faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", MaxBytes: 3},
			wantRecords: 1,
		},
		{
			name:          "zero progress",
			rule:          faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", ZeroProgress: true},
			wantAppendErr: io.ErrShortWrite,
			wantUncertain: true,
		},
		{
			name:          "immediate ENOSPC",
			rule:          faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", Err: syscall.ENOSPC},
			wantAppendErr: syscall.ENOSPC,
		},
		{
			name: "partial ENOSPC",
			rule: faultfs.Rule{
				Operation:  faultfs.OpWriteAt,
				PathSuffix: "wal.log",
				MaxBytes:   5,
				Err:        syscall.ENOSPC,
			},
			wantAppendErr: syscall.ENOSPC,
			wantUncertain: true,
		},
		{
			name:          "sync EIO",
			rule:          faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: "wal.log", Err: syscall.EIO},
			wantAppendErr: syscall.EIO,
			wantUncertain: true,
			wantRecords:   1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := disk.NewMemFS()
			fs := faultfs.New(base)
			w, err := Open(Options{FS: fs, Path: "db/wal.log", SyncWrites: true})
			if err != nil {
				t.Fatal(err)
			}
			fs.Arm(test.rule)
			seq, appendErr := w.AppendPut([]byte("key"), []byte("value"))
			if test.wantAppendErr == nil {
				if appendErr != nil || seq != 1 {
					t.Fatalf("append = (%d,%v), want (1,nil)", seq, appendErr)
				}
				if w.Err() != nil {
					t.Fatalf("successful short write poisoned WAL: %v", w.Err())
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(appendErr, ErrPersistence) || !errors.Is(appendErr, test.wantAppendErr) {
					t.Fatalf("append error = %v, want ErrPersistence and %v", appendErr, test.wantAppendErr)
				}
				if errors.Is(appendErr, disk.ErrCommitUncertain) != test.wantUncertain {
					t.Fatalf("append uncertainty = %v, want %v: %v", errors.Is(appendErr, disk.ErrCommitUncertain), test.wantUncertain, appendErr)
				}
				if _, err := w.AppendPut([]byte("later"), []byte("must-not-commit")); !errors.Is(err, ErrPersistence) {
					t.Fatalf("append after terminal error = %v", err)
				}
				if err := w.Close(); !errors.Is(err, ErrPersistence) {
					t.Fatalf("close error = %v, want ErrPersistence", err)
				}
			}

			result, err := Iterate(base, "db/wal.log", nil)
			if err != nil {
				t.Fatalf("iterate after fault: %v", err)
			}
			if result.Count != test.wantRecords {
				t.Fatalf("records after fault = %d, want %d", result.Count, test.wantRecords)
			}
			reopened, err := Open(Options{FS: base, Path: "db/wal.log", SyncWrites: true})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if _, err := reopened.AppendPut([]byte("after-reopen"), []byte("ok")); err != nil {
				t.Fatalf("append after reopen: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAsyncWALSyncFailureIsObservableAndTerminal(t *testing.T) {
	base := disk.NewMemFS()
	fs := faultfs.New(base)
	w, err := Open(Options{
		FS:                  fs,
		Path:                "db/wal.log",
		SyncWrites:          false,
		GroupCommitInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	fs.Arm(faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: "wal.log", Err: syscall.EIO})
	if seq, err := w.AppendInc([]byte("counter"), 1); err != nil || seq != 1 {
		t.Fatalf("async append = (%d,%v)", seq, err)
	}
	if err := w.Sync(); !errors.Is(err, ErrPersistence) || !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("sync error = %v", err)
	}
	if _, err := w.AppendInc([]byte("counter"), 1); !errors.Is(err, ErrPersistence) {
		t.Fatalf("append after failed group commit = %v", err)
	}
	if err := w.Close(); !errors.Is(err, ErrPersistence) {
		t.Fatalf("close error = %v", err)
	}
	result, err := Iterate(base, "db/wal.log", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Count != 1 {
		t.Fatalf("visible records = %d, want 1", result.Count)
	}
}

func TestTruncateDoesNotAdvancePastConcurrentActiveRecord(t *testing.T) {
	base := disk.NewMemFS()
	fs := &syncGateFS{FS: base}
	w, err := Open(Options{
		FS:                  fs,
		Path:                "db/wal.log",
		SyncWrites:          false,
		GroupCommitInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if seq, err := w.AppendPut([]byte("first"), []byte("one")); err != nil || seq != 1 {
		t.Fatalf("first append = (%d,%v)", seq, err)
	}

	entered, release := fs.arm()
	truncateDone := make(chan error, 1)
	go func() { truncateDone <- w.Truncate(^uint64(0)) }()
	<-entered

	if seq, err := w.AppendPut([]byte("concurrent"), []byte("two")); err != nil || seq != 2 {
		t.Fatalf("concurrent append = (%d,%v)", seq, err)
	}
	close(release)
	if err := <-truncateDone; err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := w.BaseSeq(); got != 2 {
		t.Fatalf("base seq = %d, want 2", got)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync concurrent record: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	records, result, err := ReadAll(base, "db/wal.log")
	if err != nil {
		t.Fatal(err)
	}
	if result.BaseSeq != 2 || len(records) != 1 || records[0].Seq != 2 || string(records[0].Key) != "concurrent" {
		t.Fatalf("records after concurrent truncate = %+v, result=%+v", records, result)
	}
}

func TestCommitUncertainRotationPoisonsQueuedSyncWrite(t *testing.T) {
	base := disk.NewMemFS()
	fs := &uncertainSyncDirFS{FS: base}
	w, err := Open(Options{FS: fs, Path: "db/wal.log", SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendPut([]byte("first"), []byte("one")); err != nil {
		t.Fatal(err)
	}

	entered, release := fs.arm()
	truncateDone := make(chan error, 1)
	go func() { truncateDone <- w.Truncate(1) }()
	<-entered

	appendDone := make(chan error, 1)
	go func() {
		_, err := w.AppendPut([]byte("queued"), []byte("must-not-be-acked"))
		appendDone <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for w.LastSeq() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if w.LastSeq() != 2 {
		t.Fatal("queued append was not staged")
	}
	close(release)

	truncateErr := <-truncateDone
	if !errors.Is(truncateErr, ErrPersistence) || !errors.Is(truncateErr, disk.ErrCommitUncertain) {
		t.Fatalf("truncate error = %v, want terminal commit-uncertain", truncateErr)
	}
	appendErr := <-appendDone
	if !errors.Is(appendErr, ErrPersistence) || !errors.Is(appendErr, disk.ErrCommitUncertain) {
		t.Fatalf("queued append error = %v, want terminal commit-uncertain", appendErr)
	}
	if _, err := w.AppendPut([]byte("later"), []byte("no")); !errors.Is(err, ErrPersistence) {
		t.Fatalf("append after uncertain rotation = %v", err)
	}
	if err := w.Close(); !errors.Is(err, ErrPersistence) {
		t.Fatalf("close error = %v, want terminal persistence error", err)
	}
}

func FuzzIterateNeverPanics(f *testing.F) {
	f.Add(EmptyFile(1))
	valid := appendRecord(EmptyFile(1), recordKindPut, 1, []byte("key"), []byte("value"))
	f.Add(valid)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		fs := disk.NewMemFS()
		file, err := fs.Create("wal.log")
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
		_, _ = Iterate(fs, "wal.log", nil)
	})
}
