package compression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DictionaryGCReport describes one bounded registry garbage-collection pass.
type DictionaryGCReport struct {
	Scanned        uint64
	Referenced     uint64
	Deleted        uint64
	ReclaimedBytes uint64
	Remaining      bool
}

// CollectGarbage removes persisted dictionary versions absent from liveIDs.
// maxDeletes bounds one pass; values <= 0 mean no per-pass deletion limit.
//
// Loaded segment readers retain their immutable Dictionary pointer, so cache
// eviction and file removal do not invalidate reads that started before the
// durable manifest stopped referencing a version.
func (r *Registry) CollectGarbage(
	ctx context.Context,
	liveIDs map[uint32]struct{},
	maxDeletes int,
) (DictionaryGCReport, error) {
	if r == nil || !r.hasStorage() {
		return DictionaryGCReport{}, ErrDictionaryStorage
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Save and lazy Load use the same lock. This prevents a candidate file from
	// appearing between the directory scan and removal and prevents a first
	// lookup from racing deletion of an unreferenced version.
	r.loadMu.Lock()
	defer r.loadMu.Unlock()

	entries, err := r.fs.List(r.dir)
	if err != nil {
		return DictionaryGCReport{}, fmt.Errorf("compression: list dictionaries: %w", err)
	}

	var report DictionaryGCReport
	directoryDirty := false
	finish := func(runErr error) (DictionaryGCReport, error) {
		if directoryDirty {
			if syncErr := r.fs.SyncDir(r.dir); syncErr != nil {
				syncErr = fmt.Errorf("compression: sync dictionary directory: %w", syncErr)
				if runErr != nil {
					return report, errors.Join(runErr, syncErr)
				}
				return report, syncErr
			}
		}
		return report, runErr
	}

	for _, name := range entries {
		if err := ctx.Err(); err != nil {
			return finish(err)
		}
		id, ok := parseDictionaryFileName(name)
		if !ok {
			continue
		}
		report.Scanned++
		if _, live := liveIDs[id]; live {
			report.Referenced++
			continue
		}
		if maxDeletes > 0 && report.Deleted >= uint64(maxDeletes) {
			report.Remaining = true
			continue
		}

		info, statErr := r.fs.Stat(name)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return finish(fmt.Errorf("compression: stat dictionary %d: %w", id, statErr))
		}
		if err := r.fs.Remove(name); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return finish(fmt.Errorf("compression: remove dictionary %d: %w", id, err))
		}
		directoryDirty = true
		report.Deleted++
		if statErr == nil && info.Size() > 0 {
			report.ReclaimedBytes += uint64(info.Size())
		}

		// Do not close the immutable object: an already-open segment reader may
		// still hold it. Removing only the cache entry lets that reader finish.
		r.mu.Lock()
		r.removeLocked(id)
		r.mu.Unlock()
	}
	return finish(nil)
}

func parseDictionaryFileName(name string) (uint32, bool) {
	base := filepath.Base(name)
	const prefix = "dict-"
	const suffix = ".zdict"
	if !strings.HasPrefix(base, prefix) || !strings.HasSuffix(base, suffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(base, prefix), suffix)
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || parsed == 0 {
		return 0, false
	}
	id := uint32(parsed)
	if filepath.Base(DictionaryFileName("", id)) != base {
		return 0, false
	}
	return id, true
}
