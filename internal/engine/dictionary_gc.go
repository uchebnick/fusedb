package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/uchebnick/fusedb/internal/compression"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
)

const defaultDictionaryGCMaxFilesPerRun = 64

// DictionaryGCConfig controls conservative cleanup of dictionary versions
// that are absent from both the active group catalog and all live segments.
type DictionaryGCConfig struct {
	Disabled       bool
	MaxFilesPerRun int
}

type normalizedDictionaryGC struct {
	DictionaryGCConfig
}

func normalizeDictionaryGC(cfg DictionaryGCConfig) normalizedDictionaryGC {
	if cfg.MaxFilesPerRun <= 0 {
		cfg.MaxFilesPerRun = defaultDictionaryGCMaxFilesPerRun
	}
	return normalizedDictionaryGC{DictionaryGCConfig: cfg}
}

// DictionaryGCReport describes one explicit or automatic cleanup pass.
type DictionaryGCReport struct {
	ScannedFiles     uint64
	LiveDictionaries uint64
	DeletedFiles     uint64
	ReclaimedBytes   uint64
	Remaining        bool
}

// CollectDictionaryGarbage requests a complete preemptible cleanup through the
// adaptive scheduler. It may return context.Canceled when foreground pressure
// appears; every deletion completed before cancellation remains safe.
func (db *DB) CollectDictionaryGarbage(ctx context.Context) (DictionaryGCReport, error) {
	if err := db.beginForeground(); err != nil {
		return DictionaryGCReport{}, err
	}
	defer db.endForeground()
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil || db.background == nil || db.tree == nil {
		return DictionaryGCReport{}, errors.New("fusedb: database is not open")
	}
	if db.closed.Load() {
		return DictionaryGCReport{}, errors.New("fusedb: database is closed")
	}
	if err := db.backgroundErr(); err != nil {
		return DictionaryGCReport{}, err
	}

	var report DictionaryGCReport
	job := scheduler.Job{
		ID:          fmt.Sprintf("dictionary-gc:manual:%d", db.manualTaskID.Add(1)),
		Class:       scheduler.ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundDictionaryGC,
		Priority:    1e6,
		Cost: scheduler.Cost{
			CPUFraction:    0.02,
			DiskFraction:   0.10,
			MemoryFraction: 0.01,
			Interference:   0.10,
		},
		Preemptible: true,
		Run: func(jobCtx context.Context) error {
			collected, err := db.collectDictionaryGarbageNow(jobCtx, 0)
			if err == nil {
				report = collected
				if !collected.Remaining {
					db.dictionaryGCDirty.Store(false)
				}
			}
			return err
		},
	}
	if err := db.background.Execute(ctx, job); err != nil {
		return DictionaryGCReport{}, err
	}
	return report, nil
}

func (db *DB) requestDictionaryGC() {
	if db == nil || db.dictionaryGC.Disabled || db.closed.Load() {
		return
	}
	db.dictionaryGCDirty.Store(true)
	if db.background != nil && db.dictionaryGCTask.ID != "" {
		db.background.Submit(db.dictionaryGCTask)
	}
}

func (db *DB) newDictionaryGCJob() scheduler.Job {
	return scheduler.Job{
		ID:          "dictionary-gc",
		Class:       scheduler.ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundDictionaryGC,
		Priority:    1,
		Cost: scheduler.Cost{
			CPUFraction:    0.02,
			DiskFraction:   0.10,
			MemoryFraction: 0.01,
			Interference:   0.10,
		},
		Preemptible: true,
		Needed: func() bool {
			return db.backgroundErr() == nil && db.dictionaryGCDirty.Load()
		},
		Run: func(ctx context.Context) error {
			db.dictionaryGCDirty.Store(false)
			report, err := db.collectDictionaryGarbageNow(ctx, db.dictionaryGC.MaxFilesPerRun)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					db.dictionaryGCDirty.Store(true)
				}
				return err
			}
			if report.Remaining {
				db.dictionaryGCDirty.Store(true)
			}
			return nil
		},
	}
}

func (db *DB) collectDictionaryGarbageNow(ctx context.Context, maxDeletes int) (DictionaryGCReport, error) {
	live, err := db.liveDictionaryIDs(ctx)
	if err != nil {
		return DictionaryGCReport{}, err
	}
	// Validate every dependency before deleting anything. In particular, an
	// in-process registry cache must not hide a missing or corrupt active file
	// while GC removes the only remaining forensic candidates.
	for id := range live {
		if err := ctx.Err(); err != nil {
			return DictionaryGCReport{}, err
		}
		persisted, err := compression.LoadDictionary(db.fs, compression.DictionaryFileName(
			filepath.Join(db.dir, DefaultDictionaryDirName), id,
		))
		if err != nil {
			return DictionaryGCReport{}, fmt.Errorf("%w: validate live dictionary %d before GC: %v", ErrCorruption, id, err)
		}
		persistedID := persisted.ID()
		_ = persisted.Close()
		if persistedID != id {
			return DictionaryGCReport{}, fmt.Errorf("%w: live dictionary id %d, want %d", ErrCorruption, persistedID, id)
		}
	}
	collected, err := db.registry.CollectGarbage(ctx, live, maxDeletes)
	return DictionaryGCReport{
		ScannedFiles:     collected.Scanned,
		LiveDictionaries: uint64(len(live)),
		DeletedFiles:     collected.Deleted,
		ReclaimedBytes:   collected.ReclaimedBytes,
		Remaining:        collected.Remaining,
	}, err
}

func (db *DB) liveDictionaryIDs(ctx context.Context) (map[uint32]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	catalog, err := compression.LoadDictionaryGroupSnapshot(db.fs, filepath.Join(db.dir, compression.DefaultGroupCatalogFileName))
	if err != nil {
		return nil, fmt.Errorf("%w: dictionary catalog for GC: %v", ErrCorruption, err)
	}
	live := make(map[uint32]struct{}, len(catalog.Groups))
	for _, group := range catalog.Groups {
		live[group.ActiveDictionaryID] = struct{}{}
	}

	// Current readers already carry validated immutable segment headers. Using
	// them avoids reopening every segment file after each local merge merely to
	// rediscover its dictionary ID.
	for _, leaf := range db.tree.Leaves() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reader := leaf.Reader()
		if reader == nil || reader.Segment() == nil {
			continue
		}
		if id := reader.Segment().Header.DictionaryID; id != 0 {
			live[id] = struct{}{}
		}
	}
	return live, nil
}
