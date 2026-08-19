package oneleafdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
)

const DefaultSchedulerModelFileName = "SCHEDULER-STATE"

func schedulerModelPath(dir string) string {
	return filepath.Join(dir, DefaultSchedulerModelFileName)
}

func loadSchedulerModel(fs disk.FS, dir string) (scheduler.ModelSnapshot, error) {
	data, err := disk.ReadFileLimited(fs, schedulerModelPath(dir), limits.MaxSchedulerModelBytes)
	if err != nil {
		return scheduler.ModelSnapshot{}, err
	}
	return scheduler.DecodeModelSnapshot(data)
}

func (db *DB) requestSchedulerModelPersist(generation uint64) {
	if db == nil || db.schedulerModelDisabled || db.closed.Load() {
		return
	}
	if generation <= db.schedulerModelPersisted.Load() && !db.schedulerModelDirty.Load() {
		return
	}
	db.schedulerModelDirty.Store(true)
	if db.background != nil && db.schedulerModelTask.ID != "" {
		db.background.Submit(db.schedulerModelTask)
	}
}

func (db *DB) newSchedulerModelPersistJob() scheduler.Job {
	return scheduler.Job{
		ID:          "scheduler-model-persist",
		Class:       scheduler.ClassSchedulerModelPersist,
		MetricsKind: enginemetrics.BackgroundSchedulerModelPersist,
		Priority:    1,
		Cost: scheduler.Cost{
			CPUFraction:    0.01,
			DiskFraction:   0.05,
			MemoryFraction: 0.01,
			Interference:   0.10,
		},
		Needed: func() bool {
			return db.backgroundErr() == nil && db.schedulerModelDirty.Load()
		},
		Run: func(ctx context.Context) error {
			db.schedulerModelDirty.Store(false)
			if err := db.persistSchedulerModelNow(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					db.schedulerModelDirty.Store(true)
				}
				return err
			}
			return nil
		},
	}
}

func (db *DB) persistSchedulerModelNow(ctx context.Context) error {
	if db == nil || db.schedulerModelDisabled || db.background == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot := db.background.ModelSnapshot()
	data, err := scheduler.EncodeModelSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("%w: encode: %v", ErrSchedulerModelPersistence, err)
	}
	if err := disk.WriteFileAtomically(db.fs, schedulerModelPath(db.dir), data); err != nil {
		// Scheduler state is an auxiliary optimization. Do not expose a storage
		// ErrCommitUncertain sentinel that would imply user-data uncertainty.
		return fmt.Errorf("%w: write: %v", ErrSchedulerModelPersistence, err)
	}
	db.schedulerModelPersisted.Store(snapshot.Generation)
	db.schedulerModelDirty.Store(false)
	return nil
}

func (db *DB) verifySchedulerModel(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	snapshot, err := loadSchedulerModel(db.fs, db.dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return snapshot.Generation, nil
}
