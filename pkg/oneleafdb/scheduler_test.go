package oneleafdb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
)

func TestSchedulerDefersDictionaryGCDuringLatencyPressure(t *testing.T) {
	fs := disk.NewMemFS()
	cfg := scheduler.DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ObservationWindow = 30 * time.Millisecond
	cfg.QuietConfirm = 20 * time.Millisecond
	cfg.RecoveryPeriod = 10 * time.Millisecond
	cfg.TargetReadP99 = 100 * time.Microsecond
	cfg.TargetWriteP99 = 100 * time.Microsecond
	db, err := OpenDB(DBOptions{
		Dir:             "gc-latency-pressure",
		FS:              fs,
		DisableWAL:      true,
		SchedulerConfig: cfg,
		MetricsConfig:   enginemetrics.Config{LatencySampleEvery: 1},
		DictionaryGC:    DictionaryGCConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.CloseWithoutCheckpoint()
	orphan, err := compression.NewDictionary(401, bytes.Repeat([]byte("gc-pressure-orphan|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.registry.Save(orphan); err != nil {
		t.Fatal(err)
	}
	path := compression.DictionaryFileName(filepath.Join("gc-latency-pressure", DefaultDictionaryDirName), orphan.ID())

	started := db.telemetry.BeginRead()
	time.Sleep(time.Millisecond)
	db.telemetry.EndRead(started)
	waitForSchedulerState(t, db, scheduler.StateOverloaded)
	db.dictionaryGC.Disabled = false
	db.requestDictionaryGC()
	time.Sleep(2 * cfg.PollInterval)
	if got := db.MetricsSnapshot().BackgroundStarted[enginemetrics.BackgroundDictionaryGC]; got != 0 {
		t.Fatalf("dictionary GC started under latency pressure: %d", got)
	}
	if _, err := fs.Stat(path); err != nil {
		t.Fatalf("dictionary removed under latency pressure: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := fs.Stat(path); errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("dictionary GC did not resume after foreground recovery")
}

func TestSchedulerDefersModelPersistenceDuringLatencyPressure(t *testing.T) {
	fs := disk.NewMemFS()
	cfg := scheduler.DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ObservationWindow = 30 * time.Millisecond
	cfg.QuietConfirm = 15 * time.Millisecond
	cfg.RecoveryPeriod = 10 * time.Millisecond
	cfg.TargetReadP99 = 100 * time.Microsecond
	cfg.TargetWriteP99 = 100 * time.Microsecond
	db, err := OpenDB(DBOptions{
		Dir:             "model-latency-pressure",
		FS:              fs,
		DisableWAL:      true,
		SchedulerConfig: cfg,
		MetricsConfig:   enginemetrics.Config{LatencySampleEvery: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.CloseWithoutCheckpoint()

	started := db.telemetry.BeginRead()
	time.Sleep(time.Millisecond)
	db.telemetry.EndRead(started)
	waitForSchedulerState(t, db, scheduler.StateOverloaded)
	db.schedulerModelDirty.Store(true)
	db.requestSchedulerModelPersist(0)
	time.Sleep(2 * cfg.PollInterval)
	if got := db.MetricsSnapshot().BackgroundStarted[enginemetrics.BackgroundSchedulerModelPersist]; got != 0 {
		t.Fatalf("scheduler model persistence started under latency pressure: %d", got)
	}
	if _, err := fs.Stat(schedulerModelPath("model-latency-pressure")); err == nil {
		t.Fatal("scheduler model was persisted under latency pressure")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := loadSchedulerModel(fs, "model-latency-pressure"); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("scheduler model persistence did not resume in quiet window")
}

func TestSchedulerDefersMergeDuringLatencyPressure(t *testing.T) {
	cfg := scheduler.DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ObservationWindow = 30 * time.Millisecond
	cfg.QuietConfirm = 20 * time.Millisecond
	cfg.RecoveryPeriod = 10 * time.Millisecond
	cfg.TargetReadP99 = 100 * time.Microsecond
	cfg.TargetWriteP99 = 100 * time.Microsecond

	db, err := OpenDB(DBOptions{
		Dir:             t.TempDir(),
		ThresholdBytes:  4 << 10,
		MaxLeafBytes:    64 << 10,
		DisableWAL:      true,
		SchedulerConfig: cfg,
		MetricsConfig:   enginemetrics.Config{LatencySampleEvery: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Inject a foreground latency spike through the same recorder the scheduler
	// observes. Waiting for the classified state prevents a merge from racing the
	// first observation tick.
	started := db.telemetry.BeginRead()
	time.Sleep(time.Millisecond)
	db.telemetry.EndRead(started)
	waitForSchedulerState(t, db, scheduler.StateOverloaded)

	payload := make([]byte, 256)
	for i := 0; db.BufferedBytes() < 6<<10; i++ {
		if err := db.Put(DBKey(i), payload); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * cfg.PollInterval)

	snapshot := db.MetricsSnapshot()
	if got := snapshot.BackgroundStarted[enginemetrics.BackgroundMerge]; got != 0 {
		t.Fatalf("merge started under latency pressure: %d", got)
	}
	if db.BufferedBytes() == 0 {
		t.Fatal("buffer was merged under latency pressure")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if db.MetricsSnapshot().BackgroundCompleted[enginemetrics.BackgroundMerge] > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("deferred merge did not resume after foreground recovery")
}

func TestManualMergeRunsAsScheduledCheckpoint(t *testing.T) {
	cfg := scheduler.DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	db, err := OpenDB(DBOptions{
		Dir:             t.TempDir(),
		DisableWAL:      true,
		SchedulerConfig: cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	snapshot := db.MetricsSnapshot()
	if snapshot.BackgroundCompleted[enginemetrics.BackgroundCheckpoint] != 1 {
		t.Fatalf("completed checkpoints = %d", snapshot.BackgroundCompleted[enginemetrics.BackgroundCheckpoint])
	}
}

func TestMandatoryMergeBoundsDebtUnderSustainedPressure(t *testing.T) {
	cfg := scheduler.DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ObservationWindow = 25 * time.Millisecond
	cfg.TargetReadP99 = 100 * time.Microsecond
	cfg.TargetWriteP99 = 100 * time.Microsecond
	db, err := OpenDB(DBOptions{
		Dir:             t.TempDir(),
		ThresholdBytes:  2 << 10,
		MaxLeafBytes:    64 << 10,
		DisableWAL:      true,
		SchedulerConfig: cfg,
		MetricsConfig:   enginemetrics.Config{LatencySampleEvery: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	started := db.telemetry.BeginRead()
	time.Sleep(time.Millisecond)
	db.telemetry.EndRead(started)
	waitForSchedulerState(t, db, scheduler.StateOverloaded)

	payload := make([]byte, 256)
	for i := 0; db.BufferedBytes() < 9<<10; i++ {
		if err := db.Put(DBKey(i), payload); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if db.MetricsSnapshot().BackgroundCompleted[enginemetrics.BackgroundMerge] > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("mandatory merge did not bound debt during sustained pressure")
}

func TestResourceCapacityIsAutomaticOrExplicit(t *testing.T) {
	auto, err := OpenDB(DBOptions{Dir: t.TempDir(), DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	autoResources := auto.MetricsSnapshot().Resources
	if !autoResources.Capacity.CPUAutomatic || autoResources.Capacity.CPUCores <= 0 {
		t.Fatalf("automatic CPU capacity = %+v", autoResources.Capacity)
	}
	if !autoResources.Capacity.MemoryAutomatic || autoResources.Capacity.MemoryBytes == 0 {
		t.Fatalf("automatic memory capacity = %+v", autoResources.Capacity)
	}
	if err := auto.Close(); err != nil {
		t.Fatal(err)
	}

	explicit, err := OpenDB(DBOptions{
		Dir:        t.TempDir(),
		DisableWAL: true,
		ResourceConfig: enginemetrics.ResourceConfig{
			CPUCores:           2.5,
			DiskBytesPerSecond: 32 << 20,
			MemoryBytes:        256 << 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer explicit.Close()
	capacity := explicit.MetricsSnapshot().Resources.Capacity
	if capacity.CPUCores != 2.5 || capacity.DiskBytesPerSecond != 32<<20 || capacity.MemoryBytes != 256<<20 {
		t.Fatalf("explicit capacity = %+v", capacity)
	}
	if capacity.CPUAutomatic || capacity.DiskAutomatic || capacity.MemoryAutomatic {
		t.Fatalf("explicit capacity marked automatic = %+v", capacity)
	}
}

func waitForSchedulerState(t *testing.T, db *DB, want scheduler.State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if db.SchedulerState() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scheduler state = %s, want %s", db.SchedulerState(), want)
}
