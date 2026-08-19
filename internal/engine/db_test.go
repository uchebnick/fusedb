package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/backup"
	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/dbformat"
	"github.com/uchebnick/fusedb/internal/disk"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/tree"
	"github.com/uchebnick/fusedb/internal/value"
	"github.com/uchebnick/fusedb/internal/wal"
)

type closeCheckpointGateFS struct {
	disk.FS
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (f *closeCheckpointGateFS) arm() (<-chan struct{}, chan<- struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = true
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
	return f.entered, f.release
}

func (f *closeCheckpointGateFS) SyncDir(dir string) error {
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
	return f.FS.SyncDir(dir)
}

func TestCloseExcludesWritesAfterFinalFreeze(t *testing.T) {
	fs := &closeCheckpointGateFS{FS: disk.NewMemFS()}
	opts := DBOptions{
		Dir:                              "close-admission",
		FS:                               fs,
		DisableWAL:                       true,
		ThresholdBytes:                   1 << 30,
		DisableSchedulerModelPersistence: true,
		DictionaryTraining:               DictionaryTrainingConfig{Disabled: true},
		DictionaryGC:                     DictionaryGCConfig{Disabled: true},
	}
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("before-close"), []byte("stable")); err != nil {
		t.Fatal(err)
	}

	entered, release := fs.arm()
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not reach checkpoint commit")
	}

	putDone := make(chan error, 1)
	go func() { putDone <- db.Put([]byte("after-freeze"), []byte("must-not-ack")) }()
	select {
	case err := <-putDone:
		t.Fatalf("write escaped close admission gate: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := <-putDone; err == nil {
		t.Fatal("write that started during close was acknowledged")
	}

	reopened, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, found, err := reopened.Get([]byte("before-close")); err != nil || !found || string(value) != "stable" {
		t.Fatalf("durable pre-close write = (%q,%v,%v)", value, found, err)
	}
	if _, found, err := reopened.Get([]byte("after-freeze")); err != nil || found {
		t.Fatalf("post-freeze write after reopen = (%v,%v), want missing", found, err)
	}
}

func TestRejectedMutationsNeverEnterWAL(t *testing.T) {
	fs := disk.NewMemFS()
	dir := "rejected-mutations"
	db, err := OpenDB(DBOptions{
		Dir:                              dir,
		FS:                               fs,
		ThresholdBytes:                   1 << 30,
		WALSyncWrites:                    true,
		DisableSchedulerModelPersistence: true,
		DictionaryTraining:               DictionaryTrainingConfig{Disabled: true},
		DictionaryGC:                     DictionaryGCConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(nil, []byte("must-not-log")); !errors.Is(err, tree.ErrEmptyKey) {
		t.Fatalf("empty put error = %v", err)
	}
	if err := db.Put([]byte("typed"), []byte("bytes")); err != nil {
		t.Fatal(err)
	}
	if err := db.Inc([]byte("typed"), 1); !errors.Is(err, value.ErrKindMismatch) {
		t.Fatalf("Inc over bytes error = %v, want ErrKindMismatch", err)
	}
	if err := db.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}

	result, err := wal.Iterate(fs, filepath.Join(dir, "wal.log"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Count != 1 {
		t.Fatalf("WAL records = %d, want only the accepted Put", result.Count)
	}
	reopened, err := OpenDB(DBOptions{Dir: dir, FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, found, err := reopened.Get([]byte("typed"))
	if err != nil || !found || string(got) != "bytes" {
		t.Fatalf("recovered typed value = (%q,%v,%v)", got, found, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIncOverSegmentByteValueIsRejectedBeforeWAL(t *testing.T) {
	fs := disk.NewMemFS()
	dir := "typed-segment"
	opts := DBOptions{
		Dir:                              dir,
		FS:                               fs,
		WALSyncWrites:                    true,
		DisableSchedulerModelPersistence: true,
		DictionaryTraining:               DictionaryTrainingConfig{Disabled: true},
		DictionaryGC:                     DictionaryGCConfig{Disabled: true},
	}
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("typed"), []byte("segment-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	before, err := wal.Iterate(fs, filepath.Join(dir, "wal.log"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Inc([]byte("typed"), 1); !errors.Is(err, value.ErrKindMismatch) {
		t.Fatalf("Inc over segment bytes error = %v", err)
	}
	if err := db.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
	after, err := wal.Iterate(fs, filepath.Join(dir, "wal.log"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if after.Count != before.Count {
		t.Fatalf("rejected Inc changed WAL count from %d to %d", before.Count, after.Count)
	}
	reopened, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := reopened.Get([]byte("typed"))
	if err != nil || !found || string(got) != "segment-bytes" {
		t.Fatalf("reopened value = (%q,%v,%v)", got, found, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentSameKeyOrderMatchesWALReplay(t *testing.T) {
	fs := disk.NewMemFS()
	dir := "same-key-order"
	opts := DBOptions{
		Dir:                              dir,
		FS:                               fs,
		ThresholdBytes:                   1 << 30,
		WALGroupCommitInterval:           time.Hour,
		DisableSchedulerModelPersistence: true,
		DictionaryTraining:               DictionaryTrainingConfig{Disabled: true},
		DictionaryGC:                     DictionaryGCConfig{Disabled: true},
	}
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("contended")
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := range 250 {
				payload := fmt.Appendf(nil, "%d/%d", worker, iteration)
				if err := db.Put(key, payload); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	live, found, err := db.Get(key)
	if err != nil || !found {
		t.Fatalf("live value = (%q,%v,%v)", live, found, err)
	}
	if err := db.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	recovered, found, err := reopened.Get(key)
	if err != nil || !found || !bytes.Equal(recovered, live) {
		t.Fatalf("recovered value = (%q,%v,%v), live=%q", recovered, found, err, live)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMutationSizeLimits(t *testing.T) {
	if err := validateMutationLengths(MaxKeyBytes+1, 0); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized key error = %v", err)
	}
	if err := validateMutationLengths(3, MaxValueBytes+1); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("oversized value error = %v", err)
	}
}

func TestCompressedDatabaseReopensFromOwnedDictionary(t *testing.T) {
	fs := disk.NewMemFS()
	dict, err := compression.NewDictionary(91, bytes.Repeat([]byte("tenant=example|region=eu|kind=session|"), 128))
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	db, err := OpenDB(DBOptions{Dir: "owned-dictionary", FS: fs, Dictionary: dict, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want := bytes.Repeat([]byte("tenant=example|region=eu|kind=session|value=active;"), 20)
	if err := db.Put([]byte("session:1"), want); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// No Dictionary option is supplied on reopen. The segment's exact
	// dictionary is resolved from the database-owned persistent registry.
	reopened, err := OpenDB(DBOptions{Dir: "owned-dictionary", FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("reopen without dictionary option: %v", err)
	}
	defer reopened.Close()
	got, found, err := reopened.Get([]byte("session:1"))
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("get after reopen = (%q,%v,%v)", got, found, err)
	}
	if err := reopened.Put([]byte("session:2"), want); err != nil {
		t.Fatalf("put after reopen: %v", err)
	}
	if err := reopened.Merge(); err != nil {
		t.Fatalf("merge after reopen: %v", err)
	}
	for _, record := range reopened.tree.Manifest().Leaves {
		if record.SegmentID == 0 {
			continue
		}
		persisted, err := segment.OpenSegment(fs, segment.SegmentFileName("owned-dictionary", record.SegmentID, record.SegmentVersion))
		if err != nil {
			t.Fatalf("open merged segment: %v", err)
		}
		if persisted.Header.DictionaryID != dict.ID() {
			t.Fatalf("dictionary after reopen = %d, want %d", persisted.Header.DictionaryID, dict.ID())
		}
	}
}

func TestLegacySingleDictionaryPolicyMigratesFromSegments(t *testing.T) {
	fs := disk.NewMemFS()
	dict, err := compression.NewDictionary(93, bytes.Repeat([]byte("legacy-dictionary-pattern"), 128))
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	db, err := OpenDB(DBOptions{Dir: "legacy-dictionary", FS: fs, Dictionary: dict, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("legacy"), bytes.Repeat([]byte("legacy-dictionary-pattern"), 64)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := fs.Remove(filepath.Join("legacy-dictionary", compression.DefaultGroupCatalogFileName)); err != nil {
		t.Fatalf("remove group catalog: %v", err)
	}
	if err := fs.Remove(dbformat.FileName("legacy-dictionary")); err != nil {
		t.Fatalf("remove legacy FORMAT: %v", err)
	}

	reopened, err := OpenDB(DBOptions{Dir: "legacy-dictionary", FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("reopen legacy database: %v", err)
	}
	defer reopened.Close()
	if active, ok := reopened.catalog.ActiveDictionaryID(DefaultDictionaryGroupID); !ok || active != dict.ID() {
		t.Fatalf("migrated active dictionary = (%d,%v), want (%d,true)", active, ok, dict.ID())
	}
}

func TestRuntimeDictionaryTrainingPublishesAndSurvivesReopen(t *testing.T) {
	fs := disk.NewMemFS()
	db, err := OpenDB(DBOptions{
		Dir:           "runtime-training",
		FS:            fs,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 20 * time.Millisecond,
			QuietConfirm:      5 * time.Millisecond,
			RecoveryPeriod:    5 * time.Millisecond,
			TargetReadP99:     time.Second,
			TargetWriteP99:    time.Second,
			QuietRateCeiling:  1_000_000,
		},
		DictionaryTraining: DictionaryTrainingConfig{
			GroupLeaves:              4,
			DictionarySize:           1024,
			MinimumTrainingSamples:   4,
			MinimumEvaluationSamples: 2,
			MaxSamplesPerGroup:       64,
			MaxSampleBytesPerGroup:   2 << 20,
			MaxTotalSampleBytes:      4 << 20,
			MinimumGain:              0.01,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	value := bytes.Repeat([]byte("tenant=acme|region=eu|kind=session|status=active|"), 12)
	for i := range 1200 {
		if err := db.Put([]byte(fmt.Sprintf("session:%08d", i)), value); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("seed merge: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var activeID uint32
	for time.Now().Before(deadline) {
		if id, ok := db.catalog.ActiveDictionaryID(DefaultDictionaryGroupID); ok {
			activeID = id
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if activeID == 0 {
		t.Fatalf("runtime dictionary was not published; metrics=%+v", db.MetricsSnapshot())
	}

	for i := 1200; i < 1400; i++ {
		if err := db.Put([]byte(fmt.Sprintf("session:%08d", i)), value); err != nil {
			t.Fatalf("put after training %d: %v", i, err)
		}
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("compressed merge: %v", err)
	}
	for _, record := range db.tree.Manifest().Leaves {
		if record.SegmentID == 0 {
			continue
		}
		persisted, err := segment.OpenSegment(fs, segment.SegmentFileName("runtime-training", record.SegmentID, record.SegmentVersion))
		if err != nil {
			t.Fatalf("open trained segment: %v", err)
		}
		if persisted.Header.DictionaryID != activeID {
			t.Fatalf("segment dictionary = %d, want active %d", persisted.Header.DictionaryID, activeID)
		}
	}
	metrics := db.MetricsSnapshot()
	if metrics.BackgroundCompleted[enginemetrics.BackgroundDictionaryTrain] == 0 ||
		metrics.BackgroundCompleted[enginemetrics.BackgroundDictionaryEvaluate] == 0 {
		t.Fatalf("dictionary job metrics = %+v", metrics)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenDB(DBOptions{
		Dir:                "runtime-training",
		FS:                 fs,
		WALSyncWrites:      true,
		DictionaryTraining: DictionaryTrainingConfig{Disabled: true},
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if id, ok := reopened.catalog.ActiveDictionaryID(DefaultDictionaryGroupID); !ok || id != activeID {
		t.Fatalf("active dictionary after reopen = (%d,%v), want (%d,true)", id, ok, activeID)
	}
}

func TestDictionaryGCKeepsSegmentReferencesThenDeletesObsoleteVersions(t *testing.T) {
	fs := disk.NewMemFS()
	dict1, err := compression.NewDictionary(101, bytes.Repeat([]byte("dictionary-generation-one|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(DBOptions{
		Dir:           "dictionary-gc",
		FS:            fs,
		Dictionary:    dict1,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 10 * time.Millisecond,
			TargetReadP99:     time.Second,
			TargetWriteP99:    time.Second,
		},
		DictionaryGC: DictionaryGCConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	value1 := bytes.Repeat([]byte("dictionary-generation-one|value|"), 64)
	if err := db.Put([]byte("key"), value1); err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	dict2, err := compression.NewDictionary(102, bytes.Repeat([]byte("dictionary-generation-two|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.catalog.EnsureNextAfter(dict2.ID()); err != nil {
		t.Fatal(err)
	}
	if err := db.registry.Save(dict2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.catalog.Publish(DefaultDictionaryGroupID, dict2.ID()); err != nil {
		t.Fatal(err)
	}

	// Dictionary 101 is no longer active, but the live segment still names it.
	kept, err := db.CollectDictionaryGarbage(context.Background())
	if err != nil {
		t.Fatalf("collect while old segment is live: %v", err)
	}
	if kept.DeletedFiles != 0 || kept.LiveDictionaries != 2 {
		t.Fatalf("live-reference collection = %+v", kept)
	}
	oldPath := compression.DictionaryFileName(filepath.Join("dictionary-gc", DefaultDictionaryDirName), dict1.ID())
	if _, err := fs.Stat(oldPath); err != nil {
		t.Fatalf("old dictionary removed while referenced: %v", err)
	}

	// Rewriting the leaf under the new active policy removes the last durable
	// reference to 101. Add a fully orphaned version to exercise crash debris.
	value2 := bytes.Repeat([]byte("dictionary-generation-two|value|"), 64)
	if err := db.Put([]byte("key"), value2); err != nil {
		t.Fatal(err)
	}
	oldReader := db.tree.Leaves()[0].Reader()
	if oldReader == nil {
		t.Fatal("expected old segment reader")
	}
	orphan, err := compression.NewDictionary(103, bytes.Repeat([]byte("orphan-generation|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.catalog.EnsureNextAfter(orphan.ID()); err != nil {
		t.Fatal(err)
	}
	if err := db.registry.Save(orphan); err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	orphanPath := compression.DictionaryFileName(filepath.Join("dictionary-gc", DefaultDictionaryDirName), orphan.ID())
	collected, err := db.CollectDictionaryGarbage(context.Background())
	if err != nil {
		t.Fatalf("collect obsolete dictionaries: %v", err)
	}
	if collected.DeletedFiles != 2 || collected.LiveDictionaries != 1 || collected.ReclaimedBytes == 0 {
		t.Fatalf("obsolete collection = %+v", collected)
	}
	for _, path := range []string{oldPath, orphanPath} {
		if _, err := fs.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("obsolete dictionary %q stat = %v, want not exist", path, err)
		}
	}
	activePath := compression.DictionaryFileName(filepath.Join("dictionary-gc", DefaultDictionaryDirName), dict2.ID())
	if _, err := fs.Stat(activePath); err != nil {
		t.Fatalf("active dictionary removed: %v", err)
	}
	encoded, found, err := oldReader.Get([]byte("key"))
	if err != nil || !found {
		t.Fatalf("retired reader after dictionary GC = (%v,%v)", found, err)
	}
	retiredValue, err := value.DecodeBytes(encoded)
	if err != nil || !bytes.Equal(retiredValue, value1) {
		t.Fatalf("retired reader value = (%q,%v), want old value", retiredValue, err)
	}
	got, found, err := db.Get([]byte("key"))
	if err != nil || !found || !bytes.Equal(got, value2) {
		t.Fatalf("get after GC = (%q,%v,%v)", got, found, err)
	}
	if db.MetricsSnapshot().BackgroundCompleted[enginemetrics.BackgroundDictionaryGC] < 2 {
		t.Fatal("manual dictionary GC passes were not recorded")
	}
	backupReport, err := db.Backup(context.Background(), "dictionary-gc.fbak")
	if err != nil {
		t.Fatalf("backup after dictionary GC: %v", err)
	}
	if backupReport.Dictionaries != 1 {
		t.Fatalf("backup dictionaries = %d, want only active version", backupReport.Dictionaries)
	}
	if _, err := backup.Restore(context.Background(), fs, "dictionary-gc.fbak", "dictionary-gc-restored"); err != nil {
		t.Fatalf("restore after dictionary GC: %v", err)
	}
	restored, err := OpenDB(DBOptions{
		Dir:                "dictionary-gc-restored",
		FS:                 fs,
		WALSyncWrites:      true,
		DictionaryTraining: DictionaryTrainingConfig{Disabled: true},
	})
	if err != nil {
		t.Fatalf("open restored after dictionary GC: %v", err)
	}
	restoredValue, restoredFound, err := restored.Get([]byte("key"))
	if err != nil || !restoredFound || !bytes.Equal(restoredValue, value2) {
		t.Fatalf("restored get after GC = (%q,%v,%v)", restoredValue, restoredFound, err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(DBOptions{
		Dir:                "dictionary-gc",
		FS:                 fs,
		WALSyncWrites:      true,
		DictionaryTraining: DictionaryTrainingConfig{Disabled: true},
	})
	if err != nil {
		t.Fatalf("reopen after GC: %v", err)
	}
	defer reopened.Close()
	got, found, err = reopened.Get([]byte("key"))
	if err != nil || !found || !bytes.Equal(got, value2) {
		t.Fatalf("get after reopen = (%q,%v,%v)", got, found, err)
	}
}

func TestAutomaticDictionaryGCRemovesOrphanOnOpen(t *testing.T) {
	fs := disk.NewMemFS()
	db, err := OpenDB(DBOptions{
		Dir:          "automatic-dictionary-gc",
		FS:           fs,
		DisableWAL:   true,
		DictionaryGC: DictionaryGCConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := compression.NewDictionary(201, bytes.Repeat([]byte("startup-orphan|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.registry.Save(orphan); err != nil {
		t.Fatal(err)
	}
	path := compression.DictionaryFileName(filepath.Join("automatic-dictionary-gc", DefaultDictionaryDirName), orphan.ID())
	if err := db.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(DBOptions{
		Dir:        "automatic-dictionary-gc",
		FS:         fs,
		DisableWAL: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseWithoutCheckpoint()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := fs.Stat(path); errors.Is(err, os.ErrNotExist) {
			if reopened.MetricsSnapshot().BackgroundCompleted[enginemetrics.BackgroundDictionaryGC] == 0 {
				t.Fatal("automatic dictionary GC deletion was not recorded")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("automatic dictionary GC did not remove startup orphan")
}

func TestDictionaryGCRefusesToDeleteWhenCatalogIsCorrupt(t *testing.T) {
	fs := disk.NewMemFS()
	active, err := compression.NewDictionary(301, bytes.Repeat([]byte("active-dictionary|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(DBOptions{
		Dir:          "dictionary-gc-corrupt-catalog",
		FS:           fs,
		DisableWAL:   true,
		Dictionary:   active,
		DictionaryGC: DictionaryGCConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.CloseWithoutCheckpoint()
	orphan, err := compression.NewDictionary(302, bytes.Repeat([]byte("orphan-dictionary|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.registry.Save(orphan); err != nil {
		t.Fatal(err)
	}

	catalogPath := filepath.Join("dictionary-gc-corrupt-catalog", compression.DefaultGroupCatalogFileName)
	catalogFile, err := fs.OpenReadWrite(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalogFile.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatal(err)
	}
	_ = catalogFile.Close()
	if _, err := db.CollectDictionaryGarbage(context.Background()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("collect error = %v, want ErrCorruption", err)
	}
	for _, id := range []uint32{active.ID(), orphan.ID()} {
		path := compression.DictionaryFileName(filepath.Join("dictionary-gc-corrupt-catalog", DefaultDictionaryDirName), id)
		if _, err := fs.Stat(path); err != nil {
			t.Fatalf("dictionary %d was removed after failed liveness scan: %v", id, err)
		}
	}
}

func TestDictionaryGCRefusesToDeleteWhenLiveDictionaryIsMissing(t *testing.T) {
	fs := disk.NewMemFS()
	active, err := compression.NewDictionary(311, bytes.Repeat([]byte("active-dictionary|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(DBOptions{
		Dir:          "dictionary-gc-missing-live",
		FS:           fs,
		DisableWAL:   true,
		Dictionary:   active,
		DictionaryGC: DictionaryGCConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.CloseWithoutCheckpoint()
	orphan, err := compression.NewDictionary(312, bytes.Repeat([]byte("orphan-dictionary|"), 128))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.registry.Save(orphan); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join("dictionary-gc-missing-live", DefaultDictionaryDirName)
	if err := fs.Remove(compression.DictionaryFileName(dir, active.ID())); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CollectDictionaryGarbage(context.Background()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("collect error = %v, want ErrCorruption", err)
	}
	if _, err := fs.Stat(compression.DictionaryFileName(dir, orphan.ID())); err != nil {
		t.Fatalf("orphan removed despite missing live dependency: %v", err)
	}
}

func TestBackupRejectsCorruptPersistedDictionaryEvenWhenCached(t *testing.T) {
	fs := disk.NewMemFS()
	dict, err := compression.NewDictionary(92, bytes.Repeat([]byte("tenant=example|region=eu|kind=session|"), 128))
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	db, err := OpenDB(DBOptions{
		Dir:           "corrupt-dictionary",
		FS:            fs,
		Dictionary:    dict,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			TargetReadP99:  time.Second,
			TargetWriteP99: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("session:1"), bytes.Repeat([]byte("tenant=example|status=active|"), 64)); err != nil {
		t.Fatalf("put: %v", err)
	}
	dictionaryPath := compression.DictionaryFileName(filepath.Join("corrupt-dictionary", DefaultDictionaryDirName), dict.ID())
	file, err := fs.OpenReadWrite(dictionaryPath)
	if err != nil {
		t.Fatalf("open dictionary: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 24); err != nil {
		t.Fatalf("corrupt dictionary: %v", err)
	}
	_ = file.Close()

	if _, err := db.Backup(context.Background(), "backup.fbak"); !errors.Is(err, ErrCorruption) {
		t.Fatalf("backup error = %v, want ErrCorruption", err)
	}
}

func TestVerifyAndBackupRejectCorruptPersistedDictionaryCatalog(t *testing.T) {
	fs := disk.NewMemFS()
	dict, err := compression.NewDictionary(94, bytes.Repeat([]byte("catalog-integrity-pattern"), 128))
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	db, err := OpenDB(DBOptions{
		Dir:           "corrupt-catalog",
		FS:            fs,
		Dictionary:    dict,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			TargetReadP99:  time.Second,
			TargetWriteP99: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	path := filepath.Join("corrupt-catalog", compression.DefaultGroupCatalogFileName)
	file, err := fs.OpenReadWrite(path)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatalf("corrupt catalog: %v", err)
	}
	_ = file.Close()

	if _, err := db.Verify(context.Background()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("verify error = %v, want ErrCorruption", err)
	}
	if _, err := db.Backup(context.Background(), "corrupt-catalog.fbak"); !errors.Is(err, ErrCorruption) {
		t.Fatalf("backup error = %v, want ErrCorruption", err)
	}
}

func TestDictionaryTrainingConfigRejectsUnsafeBounds(t *testing.T) {
	tests := []struct {
		name string
		cfg  DictionaryTrainingConfig
	}{
		{name: "negative gain", cfg: DictionaryTrainingConfig{MinimumGain: -0.01}},
		{name: "gain at one", cfg: DictionaryTrainingConfig{MinimumGain: 1}},
		{name: "chunk larger than dictionary", cfg: DictionaryTrainingConfig{DictionarySize: 1024, ChunkBytes: 2048}},
		{name: "sample minimums exceed cap", cfg: DictionaryTrainingConfig{MinimumTrainingSamples: 100, MinimumEvaluationSamples: 50, MaxSamplesPerGroup: 128}},
		{name: "group bytes exceed global", cfg: DictionaryTrainingConfig{MaxSampleBytesPerGroup: 8 << 20, MaxTotalSampleBytes: 4 << 20}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if db, err := OpenDB(DBOptions{Dir: "invalid-" + strings.ReplaceAll(test.name, " ", "-"), FS: disk.NewMemFS(), DictionaryTraining: test.cfg}); err == nil {
				_ = db.Close()
				t.Fatal("OpenDB accepted invalid dictionary training configuration")
			}
		})
	}
}

type archiveGateFS struct {
	disk.FS
	target  string
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *archiveGateFS) Create(name string) (disk.File, error) {
	file, err := f.FS.Create(name)
	if err != nil || name != f.target {
		return file, err
	}
	return &archiveGateFile{File: file, fs: f}, nil
}

type archiveGateFile struct {
	disk.File
	fs *archiveGateFS
}

func (f *archiveGateFile) Write(data []byte) (int, error) {
	f.fs.once.Do(func() { close(f.fs.started) })
	<-f.fs.release
	return f.File.Write(data)
}

func TestForegroundWriteContinuesWhileBackupArchiveStreams(t *testing.T) {
	base := disk.NewMemFS()
	fs := &archiveGateFS{
		FS:      base,
		target:  "snapshot.fbak.tmp",
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer func() {
		select {
		case <-fs.release:
		default:
			close(fs.release)
		}
	}()
	db, err := OpenDB(DBOptions{
		Dir:           "online-backup",
		FS:            fs,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      time.Millisecond,
			ObservationWindow: 20 * time.Millisecond,
			RecoveryPeriod:    time.Millisecond,
			TargetReadP99:     time.Second,
			TargetWriteP99:    time.Second,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("before"), []byte("included")); err != nil {
		t.Fatalf("put before: %v", err)
	}

	backupDone := make(chan error, 1)
	go func() {
		_, backupErr := db.Backup(context.Background(), "snapshot.fbak")
		backupDone <- backupErr
	}()
	select {
	case <-fs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("backup did not reach archive streaming")
	}

	writeDone := make(chan error, 1)
	go func() { writeDone <- db.Put([]byte("during"), []byte("excluded")) }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("foreground write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("foreground write blocked behind archive streaming")
	}
	close(fs.release)
	if err := <-backupDone; err != nil {
		t.Fatalf("backup: %v", err)
	}

	if _, err := backup.Restore(context.Background(), base, "snapshot.fbak", "restored-online"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := OpenDB(DBOptions{Dir: "restored-online", FS: base, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	if got, found, err := restored.Get([]byte("before")); err != nil || !found || string(got) != "included" {
		t.Fatalf("before = (%q,%v,%v)", got, found, err)
	}
	if _, found, err := restored.Get([]byte("during")); err != nil || found {
		t.Fatalf("during = (found=%v, err=%v), want snapshot exclusion", found, err)
	}
}

type segmentGateFS struct {
	disk.FS
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *segmentGateFS) Create(name string) (disk.File, error) {
	file, err := f.FS.Create(name)
	if err != nil || !strings.HasSuffix(name, ".seg.tmp") {
		return file, err
	}
	return &segmentGateFile{File: file, fs: f}, nil
}

type segmentGateFile struct {
	disk.File
	fs *segmentGateFS
}

func (f *segmentGateFile) Write(data []byte) (int, error) {
	f.fs.once.Do(func() { close(f.fs.started) })
	<-f.fs.release
	return f.File.Write(data)
}

func TestCheckpointPreservesWALDebtWrittenAfterFreeze(t *testing.T) {
	base := disk.NewMemFS()
	fs := &segmentGateFS{FS: base, started: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-fs.release:
		default:
			close(fs.release)
		}
	}()
	db, err := OpenDB(DBOptions{
		Dir:           "checkpoint-debt",
		FS:            fs,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      time.Millisecond,
			ObservationWindow: 20 * time.Millisecond,
			RecoveryPeriod:    time.Millisecond,
			TargetReadP99:     time.Second,
			TargetWriteP99:    time.Second,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("before"), []byte("included")); err != nil {
		t.Fatalf("put before: %v", err)
	}

	backupDone := make(chan error, 1)
	go func() {
		_, backupErr := db.Backup(context.Background(), "debt-snapshot.fbak")
		backupDone <- backupErr
	}()
	select {
	case <-fs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not begin segment write")
	}
	if err := db.Put([]byte("during"), []byte("excluded")); err != nil {
		t.Fatalf("put during checkpoint: %v", err)
	}
	if err := db.Inc([]byte("counter"), 1); err != nil {
		t.Fatalf("inc during checkpoint: %v", err)
	}
	close(fs.release)
	if err := <-backupDone; err != nil {
		t.Fatalf("backup: %v", err)
	}
	if debt := db.walBytes.Load(); debt == 0 {
		t.Fatal("checkpoint erased WAL debt written after its freeze boundary")
	}

	if _, err := backup.Restore(context.Background(), base, "debt-snapshot.fbak", "restored-debt"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := OpenDB(DBOptions{Dir: "restored-debt", FS: base, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	if _, found, err := restored.Get([]byte("during")); err != nil || found {
		t.Fatalf("post-freeze write = (found=%v, err=%v), want excluded", found, err)
	}
	if _, found, err := restored.Get([]byte("counter")); err != nil || found {
		t.Fatalf("post-freeze counter = (found=%v, err=%v), want excluded", found, err)
	}

	if err := db.CloseWithoutCheckpoint(); err != nil {
		t.Fatalf("simulate shutdown without another checkpoint: %v", err)
	}
	reopened, err := OpenDB(DBOptions{Dir: "checkpoint-debt", FS: base, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("reopen live database: %v", err)
	}
	defer reopened.Close()
	raw, found, err := reopened.Get([]byte("counter"))
	if err != nil || !found {
		t.Fatalf("replayed counter = (%q,%v,%v)", raw, found, err)
	}
	count, err := value.DecodeInt64(raw)
	if err != nil || count != 1 {
		t.Fatalf("replayed counter = (%d,%v), want exactly 1", count, err)
	}
}

type manifestSyncFaultFS struct {
	disk.FS
	armed           atomic.Bool
	manifestRenamed atomic.Bool
}

func (f *manifestSyncFaultFS) Rename(oldName, newName string) error {
	if err := f.FS.Rename(oldName, newName); err != nil {
		return err
	}
	if f.armed.Load() && filepath.Base(newName) == "MANIFEST" {
		f.manifestRenamed.Store(true)
	}
	return nil
}

func (f *manifestSyncFaultFS) SyncDir(dir string) error {
	if f.manifestRenamed.CompareAndSwap(true, false) {
		f.armed.Store(false)
		return errors.New("injected manifest directory sync failure")
	}
	return f.FS.SyncDir(dir)
}

func TestCheckpointCommitUncertainRequiresReopenWithoutDoubleIncrement(t *testing.T) {
	base := disk.NewMemFS()
	faults := &manifestSyncFaultFS{FS: base}
	opts := DBOptions{Dir: "uncertain-checkpoint", FS: faults, ThresholdBytes: 1 << 30, WALSyncWrites: true}
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for range 3 {
		if err := db.Inc([]byte("counter"), 1); err != nil {
			t.Fatalf("inc: %v", err)
		}
	}

	faults.armed.Store(true)
	if err := db.Merge(); !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("merge error = %v, want ErrCommitUncertain", err)
	}
	if err := db.Close(); !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("close error = %v, want ErrCommitUncertain", err)
	}

	// Reopen from the visible post-rename namespace. The WAL was deliberately
	// not truncated; the per-leaf watermark must prevent replaying Inc again.
	reopened, err := OpenDB(DBOptions{Dir: "uncertain-checkpoint", FS: base, ThresholdBytes: 1 << 30, WALSyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	raw, found, err := reopened.Get([]byte("counter"))
	if err != nil || !found {
		t.Fatalf("get counter = (%q,%v,%v)", raw, found, err)
	}
	got, err := value.DecodeInt64(raw)
	if err != nil {
		t.Fatalf("decode counter: %v", err)
	}
	if got != 3 {
		t.Fatalf("counter after uncertain commit recovery = %d, want 3", got)
	}
}

func TestPartialLeafMergeDoesNotReplayIncrementTwice(t *testing.T) {
	fs := disk.NewMemFS()
	opts := DBOptions{
		Dir:            "partial-merge-replay",
		FS:             fs,
		ThresholdBytes: 1 << 30,
		WALSyncWrites:  true,
	}
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for range 5 {
		if err := db.Inc([]byte("counter"), 1); err != nil {
			t.Fatalf("inc: %v", err)
		}
	}

	// Force the same per-leaf path used by adaptive maintenance, without a
	// global checkpoint or WAL truncation afterward.
	leaf := db.tree.Leaves()[0]
	db.applyMu.Lock()
	watermark := db.walLastSeq()
	leaf.FreezeBuffer()
	db.applyMu.Unlock()
	if err := db.tree.MergeLeafThrough(leaf, watermark); err != nil {
		t.Fatalf("partial merge: %v", err)
	}
	manifestAfterMerge := db.tree.Manifest()
	if len(manifestAfterMerge.Leaves) != 1 || manifestAfterMerge.Leaves[0].AppliedSeq != 5 {
		t.Fatalf("per-leaf watermark not persisted: %+v", manifestAfterMerge)
	}

	// Stop components without DB.Close: Close would run a global checkpoint and
	// hide the recovery condition this test is exercising.
	_ = db.background.Close()
	db.resourceMonitor.Close()
	if err := db.walClose(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	if err := db.tree.Close(); err != nil {
		t.Fatalf("close tree: %v", err)
	}
	closeRegistry(db.registry)
	if err := db.lock.Close(); err != nil {
		t.Fatalf("release lock: %v", err)
	}

	reopened, err := OpenDB(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	raw, found, err := reopened.Get([]byte("counter"))
	if err != nil || !found {
		t.Fatalf("get counter = (%q,%v,%v)", raw, found, err)
	}
	got, err := value.DecodeInt64(raw)
	if err != nil {
		t.Fatalf("decode counter: %v", err)
	}
	if got != 5 {
		t.Fatalf("counter after recovery = %d, want 5", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reopened.Verify(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled verify = %v, want context.Canceled", err)
	}
}

func TestOpenRejectsWALBaseBeyondDurableManifest(t *testing.T) {
	fs := disk.NewMemFS()
	opts := DBOptions{
		Dir:                              "wal-base-gap",
		FS:                               fs,
		WALSyncWrites:                    true,
		DisableSchedulerModelPersistence: true,
		DictionaryTraining:               DictionaryTrainingConfig{Disabled: true},
		DictionaryGC:                     DictionaryGCConfig{Disabled: true},
	}
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(opts.Dir, "wal.log")
	if err := disk.WriteFileAtomically(fs, walPath, wal.EmptyFile(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDB(opts); !errors.Is(err, ErrCorruption) {
		t.Fatalf("open error = %v, want ErrCorruption", err)
	}
}

func TestOpenDBExcludesSecondOwnerAndReleasesOnClose(t *testing.T) {
	fs := disk.NewMemFS()
	first, err := OpenDB(DBOptions{Dir: "db", FS: fs, DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDB(DBOptions{Dir: "db", FS: fs, DisableWAL: true}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("second open error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDB(DBOptions{Dir: "db", FS: fs, DisableWAL: true})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDBCacheInvalidatesOnPutDeleteAndInc(t *testing.T) {
	db, err := OpenDB(DBOptions{
		Dir:            t.TempDir(),
		ThresholdBytes: 1 << 30,
		CacheEntries:   8,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	key := []byte("key")
	if err := db.Put(key, []byte("one")); err != nil {
		t.Fatalf("put one: %v", err)
	}
	got, ok, err := db.Get(key)
	if err != nil || !ok {
		t.Fatalf("get one ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("got %q, want one", got)
	}

	// Everything below is about invalidation, and invalidation is only tested if
	// there is something cached to invalidate. Without this check the rest of the
	// test would still pass against a Get that never used the cache at all.
	if cached, ok := cachedValue(db, key); !ok || !bytes.Equal(cached, []byte("one")) {
		t.Fatalf("Get did not cache the value: cached=%q ok=%v", cached, ok)
	}

	got[0] = 'x'
	got, ok, err = db.Get(key)
	if err != nil || !ok {
		t.Fatalf("get cached one ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("cache returned shared value: %q", got)
	}

	if err := db.Put(key, []byte("two")); err != nil {
		t.Fatalf("put two: %v", err)
	}
	if cached, ok := cachedValue(db, key); ok {
		t.Fatalf("cache still serves %q after an overwrite", cached)
	}
	got, ok, err = db.Get(key)
	if err != nil || !ok {
		t.Fatalf("get two ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, []byte("two")) {
		t.Fatalf("got %q, want two", got)
	}

	if err := db.Delete(key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if cached, ok := cachedValue(db, key); ok {
		t.Fatalf("cache still serves %q after a delete", cached)
	}
	if got, ok, err := db.Get(key); err != nil || ok {
		t.Fatalf("get deleted = %q ok=%v err=%v, want miss", got, ok, err)
	}

	counterKey := []byte("counter")
	if err := db.Inc(counterKey, 1); err != nil {
		t.Fatalf("inc one: %v", err)
	}
	got, ok, err = db.Get(counterKey)
	if err != nil || !ok {
		t.Fatalf("get counter one ok=%v err=%v", ok, err)
	}
	gotCounter, err := value.DecodeInt64(got)
	if err != nil {
		t.Fatalf("decode counter one: %v", err)
	}
	if gotCounter != 1 {
		t.Fatalf("counter = %d, want 1", gotCounter)
	}
	if _, ok := cachedValue(db, counterKey); !ok {
		t.Fatal("Get did not cache the counter, so the next Inc has nothing to invalidate")
	}

	if err := db.Inc(counterKey, 2); err != nil {
		t.Fatalf("inc two: %v", err)
	}
	got, ok, err = db.Get(counterKey)
	if err != nil || !ok {
		t.Fatalf("get counter three ok=%v err=%v", ok, err)
	}
	gotCounter, err = value.DecodeInt64(got)
	if err != nil {
		t.Fatalf("decode counter three: %v", err)
	}
	if gotCounter != 3 {
		t.Fatalf("counter = %d, want 3", gotCounter)
	}
}

// cachedValue reports what the value cache would serve for key right now.
//
// It reads the same shard epoch the read path reads, so an entry left behind
// with a stale epoch counts as a miss, exactly as it does in Get.
func cachedValue(db *DB, key []byte) ([]byte, bool) {
	return db.cache.get(key, db.cacheEpoch[cacheShard(key)].Load())
}

// TestCacheInvalidationIsSharded pins what the sharded epochs are actually for.
//
// A write drops the entry for its own key outright, so every read stays correct
// even if the epoch arithmetic is wrong: the correctness assertions elsewhere
// cannot see a bug here. What the sharding buys is that a write leaves other
// keys' entries alone, and losing that would only show up as a collapsed hit
// rate under mixed load. So this test asserts it directly, through the cache.
func TestCacheInvalidationIsSharded(t *testing.T) {
	db, err := OpenDB(DBOptions{
		Dir:            t.TempDir(),
		ThresholdBytes: 1 << 30,
		CacheBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	probe, neighbour, outsider := shardedKeys(t)
	for _, key := range [][]byte{probe, neighbour, outsider} {
		if err := db.Put(key, []byte("value")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}

	// A marker value distinguishes a cache hit from a fresh read of the stored
	// value, which are otherwise identical from the outside.
	const marker = "cached-marker"
	probeShard := cacheShard(probe)
	db.cache.set(probe, []byte(marker), db.cacheEpoch[probeShard].Load())
	if got, _, err := db.Get(probe); err != nil || string(got) != marker {
		t.Fatalf("get probe = %q err=%v, want the cached marker", got, err)
	}

	before := epochSnapshot(db)
	if err := db.Put(outsider, []byte("other")); err != nil {
		t.Fatalf("put outsider: %v", err)
	}
	after := epochSnapshot(db)
	for shard := range before {
		bumped := after[shard] != before[shard]
		if want := uint64(shard) == cacheShard(outsider); bumped != want {
			t.Fatalf("shard %d bumped=%v, want %v after writing one key", shard, bumped, want)
		}
	}
	if got, _, err := db.Get(probe); err != nil || string(got) != marker {
		t.Fatalf("get probe = %q err=%v, want the cached marker: a write to another shard evicted it", got, err)
	}

	// A write to a key in the same shard is the one that has to invalidate the
	// probe, even though the probe's own entry is never touched.
	if err := db.Put(neighbour, []byte("other")); err != nil {
		t.Fatalf("put neighbour: %v", err)
	}
	if got, _, err := db.Get(probe); err != nil || string(got) != "value" {
		t.Fatalf("get probe = %q err=%v, want the stored value: the shard was not invalidated", got, err)
	}
}

func epochSnapshot(db *DB) [cacheEpochShards]uint64 {
	var snapshot [cacheEpochShards]uint64
	for i := range snapshot {
		snapshot[i] = db.cacheEpoch[i].Load()
	}
	return snapshot
}

// shardedKeys returns a key, a second key in the same epoch shard, and a third
// in a different one.
func shardedKeys(t *testing.T) (probe, neighbour, outsider []byte) {
	t.Helper()

	probe = []byte("shard-probe")
	probeShard := cacheShard(probe)
	for i := 0; neighbour == nil || outsider == nil; i++ {
		if i > 1_000_000 {
			t.Fatal("could not find keys on both sides of the shard split")
		}
		candidate := fmt.Appendf(nil, "shard-candidate-%d", i)
		switch {
		case cacheShard(candidate) == probeShard && neighbour == nil:
			neighbour = candidate
		case cacheShard(candidate) != probeShard && outsider == nil:
			outsider = candidate
		}
	}
	return probe, neighbour, outsider
}
