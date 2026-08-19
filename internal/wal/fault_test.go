package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/faultfs"
	"github.com/uchebnick/fusedb/internal/limits"
)

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
