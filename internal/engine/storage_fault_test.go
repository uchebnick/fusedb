package engine

import (
	"context"
	"errors"
	"io"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/faultfs"
	"github.com/uchebnick/fusedb/internal/scheduler"
	"github.com/uchebnick/fusedb/internal/value"
	"github.com/uchebnick/fusedb/internal/wal"
)

func TestDatabaseWALFaultsRequireReopenWithoutLosingOrDoublingIncrement(t *testing.T) {
	tests := []struct {
		name          string
		rule          faultfs.Rule
		wantError     error
		wantUncertain bool
		wantCounter   int64
	}{
		{
			name:        "progress-making short write",
			rule:        faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", MaxBytes: 3},
			wantCounter: 2,
		},
		{
			name:          "zero-progress write",
			rule:          faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", ZeroProgress: true},
			wantError:     io.ErrShortWrite,
			wantUncertain: true,
			wantCounter:   1,
		},
		{
			name:        "immediate ENOSPC",
			rule:        faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", Err: syscall.ENOSPC},
			wantError:   syscall.ENOSPC,
			wantCounter: 1,
		},
		{
			name: "partial ENOSPC",
			rule: faultfs.Rule{
				Operation:  faultfs.OpWriteAt,
				PathSuffix: "wal.log",
				MaxBytes:   5,
				Err:        syscall.ENOSPC,
			},
			wantError:     syscall.ENOSPC,
			wantUncertain: true,
			wantCounter:   1,
		},
		{
			name:          "sync EIO",
			rule:          faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: "wal.log", Err: syscall.EIO},
			wantError:     syscall.EIO,
			wantUncertain: true,
			wantCounter:   2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := "wal-fault-" + strings.ReplaceAll(test.name, " ", "-")
			base := disk.NewMemFS()
			seedFaultCounter(t, base, dir)
			fs := faultfs.New(base)
			db, err := OpenDB(faultDBOptions(dir, fs))
			if err != nil {
				t.Fatal(err)
			}
			fs.Arm(test.rule)
			incrementErr := db.Inc([]byte("counter"), 1)
			if test.wantError == nil {
				if incrementErr != nil {
					t.Fatalf("increment: %v", incrementErr)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(incrementErr, wal.ErrPersistence) || !errors.Is(incrementErr, test.wantError) {
					t.Fatalf("increment error = %v, want WAL persistence and %v", incrementErr, test.wantError)
				}
				if errors.Is(incrementErr, disk.ErrCommitUncertain) != test.wantUncertain {
					t.Fatalf("uncertainty = %v, want %v: %v", errors.Is(incrementErr, disk.ErrCommitUncertain), test.wantUncertain, incrementErr)
				}
				if _, _, err := db.Get([]byte("counter")); !errors.Is(err, wal.ErrPersistence) {
					t.Fatalf("read on poisoned handle = %v", err)
				}
				metrics := db.MetricsSnapshot()
				if metrics.TerminalErrors != 1 || (metrics.CommitUncertainErrors == 1) != test.wantUncertain {
					t.Fatalf("terminal metrics = (%d,%d), uncertainty=%v",
						metrics.TerminalErrors, metrics.CommitUncertainErrors, test.wantUncertain)
				}
				if err := db.Inc([]byte("counter"), 1); !errors.Is(err, wal.ErrPersistence) {
					t.Fatalf("second increment on poisoned handle = %v", err)
				}
				if err := db.Close(); !errors.Is(err, wal.ErrPersistence) {
					t.Fatalf("close error = %v", err)
				}
			}

			reopened, err := OpenDB(faultDBOptions(dir, base))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if got := readFaultCounter(t, reopened); got != test.wantCounter {
				t.Fatalf("counter after recovery = %d, want %d", got, test.wantCounter)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApplyOnceWALFaultRecoversWholeBatchAndRetryIsSafe(t *testing.T) {
	tests := []struct {
		name        string
		rule        faultfs.Rule
		wantPresent bool
	}{
		{
			name: "immediate ENOSPC",
			rule: faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", Err: syscall.ENOSPC},
		},
		{
			name: "partial ENOSPC",
			rule: faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: "wal.log", MaxBytes: 7, Err: syscall.ENOSPC},
		},
		{
			name:        "sync EIO after complete record",
			rule:        faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: "wal.log", Err: syscall.EIO},
			wantPresent: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := "batch-wal-fault-" + strings.ReplaceAll(test.name, " ", "-")
			base := disk.NewMemFS()
			seedFaultCounter(t, base, dir)
			fs := faultfs.New(base)
			db, err := OpenDB(faultDBOptions(dir, fs))
			if err != nil {
				t.Fatal(err)
			}
			mutations := []Mutation{
				PutMutation([]byte("order/991"), []byte("paid")),
				IncMutation([]byte("counter"), 1),
			}
			fs.Arm(test.rule)
			if applied, err := db.ApplyOnce([]byte("event/order-991"), mutations); applied || !errors.Is(err, wal.ErrPersistence) {
				t.Fatalf("faulted apply = (%v, %v)", applied, err)
			}
			if err := db.Close(); !errors.Is(err, wal.ErrPersistence) {
				t.Fatalf("close error = %v", err)
			}

			reopened, err := OpenDB(faultDBOptions(dir, base))
			if err != nil {
				t.Fatal(err)
			}
			order, found, err := reopened.Get([]byte("order/991"))
			if err != nil || found != test.wantPresent || (found && string(order) != "paid") {
				t.Fatalf("recovered order = (%q,%v,%v), want present=%v", order, found, err, test.wantPresent)
			}
			wantBeforeRetry := int64(1)
			if test.wantPresent {
				wantBeforeRetry = 2
			}
			if got := readFaultCounter(t, reopened); got != wantBeforeRetry {
				t.Fatalf("counter before retry = %d, want %d", got, wantBeforeRetry)
			}
			applied, err := reopened.ApplyOnce([]byte("event/order-991"), mutations)
			if err != nil || applied == test.wantPresent {
				t.Fatalf("retry = (%v,%v), recovered present=%v", applied, err, test.wantPresent)
			}
			if got := readFaultCounter(t, reopened); got != 2 {
				t.Fatalf("counter after retry = %d, want 2", got)
			}
			if _, err := reopened.Verify(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAsyncDatabaseWALFailurePoisonsHandleBeforeCloseCheckpoint(t *testing.T) {
	base := disk.NewMemFS()
	dir := "async-wal-fault"
	seedFaultCounter(t, base, dir)
	fs := faultfs.New(base)
	opts := faultDBOptions(dir, fs)
	opts.WALSyncWrites = false
	opts.WALGroupCommitInterval = time.Hour
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	fs.Arm(faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: "wal.log", Err: syscall.EIO})
	if err := db.Inc([]byte("counter"), 1); err != nil {
		t.Fatalf("async increment: %v", err)
	}
	if err := db.wal.Sync(); !errors.Is(err, wal.ErrPersistence) || !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("forced group commit error = %v", err)
	}
	if health := db.Health(); health.Ready || health.Closed || !health.TerminalError {
		t.Fatalf("health after async persistence failure = %+v", health)
	}
	if _, _, err := db.Get([]byte("counter")); !errors.Is(err, wal.ErrPersistence) {
		t.Fatalf("read after async persistence failure = %v", err)
	}
	if metrics := db.MetricsSnapshot(); metrics.TerminalErrors != 1 || metrics.CommitUncertainErrors != 1 {
		t.Fatalf("terminal metrics = (%d,%d)", metrics.TerminalErrors, metrics.CommitUncertainErrors)
	}
	if err := db.Close(); !errors.Is(err, wal.ErrPersistence) {
		t.Fatalf("close error = %v", err)
	}
	if health := db.Health(); health.Ready || !health.Closed || !health.TerminalError {
		t.Fatalf("health after close = %+v", health)
	}

	reopened, err := OpenDB(faultDBOptions(dir, base))
	if err != nil {
		t.Fatal(err)
	}
	if health := reopened.Health(); !health.Ready || health.Closed || health.TerminalError {
		t.Fatalf("health after reopen = %+v", health)
	}
	if got := readFaultCounter(t, reopened); got != 2 {
		t.Fatalf("counter after async recovery = %d, want 2", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentCommitFaultMatrixRecoversFromWALExactlyOnce(t *testing.T) {
	tests := []struct {
		name          string
		rule          faultfs.Rule
		wantUncertain bool
	}{
		{name: "create ENOSPC", rule: faultfs.Rule{Operation: faultfs.OpCreate, PathSuffix: ".seg.tmp", Err: syscall.ENOSPC}},
		{name: "partial write ENOSPC", rule: faultfs.Rule{Operation: faultfs.OpWrite, PathSuffix: ".seg.tmp", MaxBytes: 5, Err: syscall.ENOSPC}},
		{name: "zero-progress write", rule: faultfs.Rule{Operation: faultfs.OpWrite, PathSuffix: ".seg.tmp", ZeroProgress: true}},
		{name: "sync EIO", rule: faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: ".seg.tmp", Err: syscall.EIO}},
		{name: "close EIO", rule: faultfs.Rule{Operation: faultfs.OpClose, PathSuffix: ".seg.tmp", Err: syscall.EIO}},
		{name: "rename EIO", rule: faultfs.Rule{Operation: faultfs.OpRename, PathSuffix: ".seg", Err: syscall.EIO}},
		{name: "directory sync EIO", rule: faultfs.Rule{Operation: faultfs.OpSyncDir, Err: syscall.EIO}, wantUncertain: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := "segment-fault-" + strings.ReplaceAll(test.name, " ", "-")
			base := disk.NewMemFS()
			seedFaultCounter(t, base, dir)
			fs := faultfs.New(base)
			db, err := OpenDB(faultDBOptions(dir, fs))
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Inc([]byte("counter"), 1); err != nil {
				t.Fatal(err)
			}
			fs.Arm(test.rule)
			mergeErr := db.Merge()
			if mergeErr == nil || !fs.Fired() {
				t.Fatalf("merge error = %v, fired=%v", mergeErr, fs.Fired())
			}
			if errors.Is(mergeErr, disk.ErrCommitUncertain) != test.wantUncertain {
				t.Fatalf("merge uncertainty = %v, want %v: %v", errors.Is(mergeErr, disk.ErrCommitUncertain), test.wantUncertain, mergeErr)
			}
			if err := db.Close(); err == nil {
				t.Fatal("close hid terminal merge failure")
			}

			reopened, err := OpenDB(faultDBOptions(dir, base))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if got := readFaultCounter(t, reopened); got != 2 {
				t.Fatalf("counter after failed segment commit = %d, want 2", got)
			}
			if _, err := reopened.Verify(context.Background()); err != nil {
				t.Fatalf("verify recovered database: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManifestCommitFaultMatrixRecoversExactlyOnce(t *testing.T) {
	tests := []struct {
		name          string
		rule          faultfs.Rule
		wantUncertain bool
	}{
		{name: "create ENOSPC", rule: faultfs.Rule{Operation: faultfs.OpCreate, PathSuffix: "MANIFEST.tmp", Err: syscall.ENOSPC}},
		{name: "partial write ENOSPC", rule: faultfs.Rule{Operation: faultfs.OpWrite, PathSuffix: "MANIFEST.tmp", MaxBytes: 7, Err: syscall.ENOSPC}},
		{name: "zero-progress write", rule: faultfs.Rule{Operation: faultfs.OpWrite, PathSuffix: "MANIFEST.tmp", ZeroProgress: true}},
		{name: "sync EIO", rule: faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: "MANIFEST.tmp", Err: syscall.EIO}},
		{name: "close EIO", rule: faultfs.Rule{Operation: faultfs.OpClose, PathSuffix: "MANIFEST.tmp", Err: syscall.EIO}},
		{name: "rename EIO", rule: faultfs.Rule{Operation: faultfs.OpRename, PathSuffix: "MANIFEST", Err: syscall.EIO}},
		// The segment directory sync is the first matching call after Arm;
		// manifest directory sync is the second.
		{name: "directory sync EIO", rule: faultfs.Rule{Operation: faultfs.OpSyncDir, At: 2, Err: syscall.EIO}, wantUncertain: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runCheckpointFaultRecovery(t, "manifest", test.name, test.rule, test.wantUncertain)
		})
	}
}

func TestWALRotationFaultMatrixRecoversExactlyOnce(t *testing.T) {
	tests := []struct {
		name          string
		rule          faultfs.Rule
		wantUncertain bool
	}{
		{name: "create ENOSPC", rule: faultfs.Rule{Operation: faultfs.OpCreate, PathSuffix: ".rotate.tmp", Err: syscall.ENOSPC}},
		{name: "partial write ENOSPC", rule: faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: ".rotate.tmp", MaxBytes: 7, Err: syscall.ENOSPC}},
		{name: "zero-progress write", rule: faultfs.Rule{Operation: faultfs.OpWriteAt, PathSuffix: ".rotate.tmp", ZeroProgress: true}},
		{name: "sync EIO", rule: faultfs.Rule{Operation: faultfs.OpSync, PathSuffix: ".rotate.tmp", Err: syscall.EIO}},
		{name: "temp close EIO", rule: faultfs.Rule{Operation: faultfs.OpClose, PathSuffix: ".rotate.tmp", Err: syscall.EIO}},
		// Rotation releases the active writer before streaming retained records
		// from a separate read-only cursor.
		{name: "current WAL close EIO", rule: faultfs.Rule{Operation: faultfs.OpClose, PathSuffix: "wal.log", At: 1, Err: syscall.EIO}},
		{name: "rename EIO", rule: faultfs.Rule{Operation: faultfs.OpRename, PathSuffix: "wal.log", Err: syscall.EIO}},
		{name: "reopen EIO", rule: faultfs.Rule{Operation: faultfs.OpOpenReadWrite, PathSuffix: "wal.log", Err: syscall.EIO}},
		// Segment and manifest directory syncs precede WAL rotation.
		{name: "directory sync EIO", rule: faultfs.Rule{Operation: faultfs.OpSyncDir, At: 3, Err: syscall.EIO}, wantUncertain: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runCheckpointFaultRecovery(t, "wal-rotation", test.name, test.rule, test.wantUncertain)
		})
	}
}

func runCheckpointFaultRecovery(t *testing.T, phase, name string, rule faultfs.Rule, wantUncertain bool) {
	t.Helper()
	dir := phase + "-fault-" + strings.ReplaceAll(name, " ", "-")
	base := disk.NewMemFS()
	seedFaultCounter(t, base, dir)
	fs := faultfs.New(base)
	db, err := OpenDB(faultDBOptions(dir, fs))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Inc([]byte("counter"), 1); err != nil {
		t.Fatal(err)
	}
	fs.Arm(rule)
	mergeErr := db.Merge()
	if mergeErr == nil || !fs.Fired() {
		t.Fatalf("%s merge error = %v, fired=%v seen=%d", phase, mergeErr, fs.Fired(), fs.Seen())
	}
	if errors.Is(mergeErr, disk.ErrCommitUncertain) != wantUncertain {
		t.Fatalf("%s uncertainty = %v, want %v: %v", phase, errors.Is(mergeErr, disk.ErrCommitUncertain), wantUncertain, mergeErr)
	}
	if err := db.Close(); err == nil {
		t.Fatalf("%s close hid terminal checkpoint failure", phase)
	}

	reopened, err := OpenDB(faultDBOptions(dir, base))
	if err != nil {
		t.Fatalf("%s reopen: %v", phase, err)
	}
	if got := readFaultCounter(t, reopened); got != 2 {
		t.Fatalf("%s counter after recovery = %d, want 2", phase, got)
	}
	if _, err := reopened.Verify(context.Background()); err != nil {
		t.Fatalf("%s verify recovered database: %v", phase, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("%s close recovered database: %v", phase, err)
	}
}

func faultDBOptions(dir string, fs disk.FS) DBOptions {
	return DBOptions{
		Dir:                              dir,
		FS:                               fs,
		WALSyncWrites:                    true,
		ThresholdBytes:                   1 << 30,
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

func seedFaultCounter(t *testing.T, fs disk.FS, dir string) {
	t.Helper()
	db, err := OpenDB(faultDBOptions(dir, fs))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Inc([]byte("counter"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func readFaultCounter(t *testing.T, db *DB) int64 {
	t.Helper()
	raw, found, err := db.Get([]byte("counter"))
	if err != nil || !found {
		t.Fatalf("get counter = (%q,%v,%v)", raw, found, err)
	}
	got, err := value.DecodeInt64(raw)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
