package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/uchebnick/fusedb/internal/backup"
	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/dbformat"
	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
	"github.com/uchebnick/fusedb/internal/manifest"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/scheduler"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/tree"
	"github.com/uchebnick/fusedb/internal/value"
	"github.com/uchebnick/fusedb/internal/wal"
)

// Default configuration constants for the engine.
const (
	// DefaultMergeThresholdBytes is the default buffer threshold triggering a leaf merge.
	DefaultMergeThresholdBytes    = 8 << 20
	DefaultMaxLeafBytes           = 64 << 20
	DefaultCacheBytes             = 5 << 20
	DefaultWALGroupCommitInterval = 200 * time.Microsecond

	// DefaultWALCheckpointBytes is how much log is allowed to pile up before a
	// checkpoint merges every leaf and truncates the log. Without it the log
	// would grow without bound while individual leaves merge on their own
	// schedule, and replay after a crash would take proportionally longer.
	DefaultWALCheckpointBytes = 64 << 20
	DefaultLockFileName       = "LOCK"
	DefaultDictionaryDirName  = "dictionaries"
	DefaultDictionaryGroupID  = 1
	// MaxKeyBytes and MaxValueBytes are format-level availability limits. They
	// bound WAL replay and segment decoding as well as live writes.
	MaxKeyBytes        = limits.MaxKeyBytes
	MaxValueBytes      = limits.MaxValueBytes
	mutationLockShards = 4096
)

// ErrDatabaseLocked reports that another FuseDB instance owns the database
// directory. The lock is advisory and released automatically on process exit.
var (
	ErrDatabaseLocked = errors.New("fusedb: database directory is locked")
	// ErrCorruption wraps any failed integrity invariant found by Verify.
	ErrCorruption = errors.New("fusedb: corruption detected")
	// ErrSchedulerModelPersistence reports loss of optional learned load
	// history. It does not mean that user data or its commit is uncertain.
	ErrSchedulerModelPersistence = errors.New("fusedb: scheduler model persistence failed")
	// ErrFormatMismatch reports that FORMAT claims the current layout while a
	// required persisted file is absent or still uses a legacy representation.
	ErrFormatMismatch         = errors.New("fusedb: format descriptor does not match persisted files")
	ErrFormatTooNew           = dbformat.ErrFormatTooNew
	ErrFormatTooOld           = dbformat.ErrFormatTooOld
	ErrUnknownRequiredFeature = dbformat.ErrUnknownRequiredFeature
	ErrMissingRequiredFeature = dbformat.ErrMissingRequiredFeature
	ErrFormatCorrupt          = dbformat.ErrCorruptDescriptor
	ErrFormatCodec            = dbformat.ErrUnsupportedCodec
	// ErrWALPersistence marks a terminal WAL write/sync failure. The handle is
	// intentionally poisoned and must be closed and reopened.
	ErrWALPersistence = wal.ErrPersistence
	ErrKeyTooLarge    = errors.New("fusedb: key exceeds maximum size")
	ErrValueTooLarge  = errors.New("fusedb: value exceeds maximum size")
)

// FormatInfo describes the compatibility contract persisted in FORMAT.
type FormatInfo struct {
	Epoch            uint32
	MinReaderEpoch   uint32
	MinWriterEpoch   uint32
	RequiredFeatures uint64
	OptionalFeatures uint64
}

// HealthStatus is a point-in-time lifecycle view suitable for readiness
// checks. TerminalError means this handle is fenced until it is closed and the
// database is reopened.
type HealthStatus struct {
	CheckedAt     time.Time
	Ready         bool
	Closed        bool
	TerminalError bool
}

// DB is an embedded key-value store backed by a tree of leaves.
//
// Writes land in the buffer of the leaf that owns the key, and a background
// scheduler merges leaves whose buffers cross the threshold. Because merges are
// per leaf, the volume rewritten by a merge is bounded by the leaf size rather
// than by the size of the database.
type DB struct {
	dir    string
	fs     disk.FS
	lock   disk.Lock
	format dbformat.Descriptor

	tree                   *tree.Tree
	wal                    *wal.WAL
	walPath                string
	cache                  *valueCache
	cacheEpoch             [cacheEpochShards]atomic.Uint64
	registry               *compression.Registry
	catalog                *compression.DictionaryGroupCatalog
	dictionaryTrainer      *dictionaryTrainer
	dictionaryGC           normalizedDictionaryGC
	schedulerModelDisabled bool

	thresholdBytes          int64
	checkpointBytes         int64
	telemetry               *enginemetrics.Recorder
	resourceMonitor         *enginemetrics.ResourceMonitor
	background              *scheduler.Scheduler
	mergeTask               scheduler.Job
	checkpointTask          scheduler.Job
	dictionaryGCTask        scheduler.Job
	dictionaryGCDirty       atomic.Bool
	schedulerModelTask      scheduler.Job
	schedulerModelDirty     atomic.Bool
	schedulerModelPersisted atomic.Uint64
	manualTaskID            atomic.Uint64

	// applyMu orders log writes against buffer freezing. Writers hold the read
	// side across "append to log, apply to tree", and a checkpoint takes the
	// write side just long enough to read the last sequence number and freeze
	// every buffer. That makes the recorded watermark exact: every operation at
	// or below it is in the frozen set, and nothing above it is. An inexact
	// watermark would replay an increment that a segment already contains and
	// silently double it.
	applyMu sync.RWMutex
	// visibilityMu gives multi-key transactions an atomic publication boundary.
	// Point operations share the read side. Transactions take the write side
	// only while publishing their already-durable mutations, so disjoint-key
	// transactions may still join the same synchronous WAL group commit.
	visibilityMu sync.RWMutex
	// mutationMu keeps the durable WAL order and in-memory order identical for
	// operations on the same key. It also makes Inc type validation atomic with
	// its WAL append. Different shards remain fully concurrent.
	mutationMu [mutationLockShards]sync.Mutex

	walBytes atomic.Int64

	closeOnce          sync.Once
	closed             atomic.Bool
	terminalMetricOnce sync.Once

	// mergeErr keeps the first background merge failure so it surfaces on the
	// next user call instead of silently killing the worker.
	mergeErr atomic.Pointer[error]
}

type DBOptions struct {
	Dir string

	// FS overrides the filesystem the database runs on.
	//
	// It defaults to the OS filesystem. Tests use it to run on an in-memory
	// filesystem or to account for the bytes the engine actually writes.
	FS                               disk.FS
	ThresholdBytes                   int64
	MaxLeafBytes                     int64
	Seed                             uint64
	SegmentID                        uint64
	Dictionary                       *compression.Dictionary
	WALPath                          string
	WALGroupCommitInterval           time.Duration
	WALSyncWrites                    bool
	WALCheckpointBytes               int64
	SchedulerConfig                  scheduler.Config
	MetricsConfig                    enginemetrics.Config
	ResourceConfig                   enginemetrics.ResourceConfig
	DictionaryTraining               DictionaryTrainingConfig
	DictionaryGC                     DictionaryGCConfig
	DisableSchedulerModelPersistence bool

	// DisableWAL runs without a write-ahead log, so writes are durable only
	// once a merge has written them into a segment.
	//
	// It exists for benchmarks that measure the storage engine on its own. It
	// is not a supported mode for real data: an unclean shutdown loses every
	// write made since the last merge.
	DisableWAL   bool
	CacheBytes   int64
	CacheEntries int
}

// OpenDB opens an existing database or creates a new one.
//
// Opening an existing directory restores the leaf tree from the manifest and
// replays the tail of the write-ahead log that no segment covers yet.
func OpenDB(opts DBOptions) (*DB, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("fusedb: Dir is required")
	}

	telemetry := enginemetrics.NewRecorder(opts.MetricsConfig)
	fs := opts.FS
	if fs == nil {
		fs = disk.DefaultFS
	}
	fs = disk.Meter(fs, telemetry)
	if err := fs.MkdirAll(opts.Dir); err != nil {
		return nil, fmt.Errorf("fusedb: create dir: %w", err)
	}
	dirLock, err := fs.Lock(filepath.Join(opts.Dir, DefaultLockFileName))
	if err != nil {
		if errors.Is(err, disk.ErrLocked) {
			return nil, fmt.Errorf("%w: %s", ErrDatabaseLocked, opts.Dir)
		}
		return nil, fmt.Errorf("fusedb: lock database directory: %w", err)
	}
	keepLock := false
	defer func() {
		if !keepLock {
			_ = dirLock.Close()
		}
	}()

	formatDescriptor, formatErr := dbformat.Load(fs, opts.Dir)
	formatPresent := formatErr == nil
	switch {
	case formatErr == nil:
		if err := dbformat.CheckCompatible(formatDescriptor); err != nil {
			return nil, fmt.Errorf("fusedb: incompatible database format: %w", err)
		}
	case errors.Is(formatErr, os.ErrNotExist):
		formatDescriptor = dbformat.Current()
	default:
		return nil, fmt.Errorf("fusedb: load database format: %w", formatErr)
	}

	manifestPath := manifest.FileName(opts.Dir)
	loaded, err := manifest.Load(fs, manifestPath)
	switch {
	case err == nil:
	case manifest.IsNotExist(err):
		loaded = nil
	default:
		return nil, fmt.Errorf("fusedb: load manifest: %w", err)
	}
	if formatPresent {
		if loaded == nil {
			return nil, fmt.Errorf("%w: FORMAT exists but MANIFEST is missing", ErrFormatMismatch)
		}
		if loaded.SourceVersion() != manifest.CurrentFormatVersion {
			return nil, fmt.Errorf("%w: FORMAT epoch %d requires manifest v%d, found v%d",
				ErrFormatMismatch, formatDescriptor.Epoch, manifest.CurrentFormatVersion, loaded.SourceVersion())
		}
		_, err := compression.LoadDictionaryGroupSnapshot(fs, filepath.Join(opts.Dir, compression.DefaultGroupCatalogFileName))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("%w: FORMAT exists but %s is missing",
					ErrFormatMismatch, compression.DefaultGroupCatalogFileName)
			}
			return nil, fmt.Errorf("fusedb: inspect dictionary group catalog: %w", err)
		}
	}

	threshold := opts.ThresholdBytes
	if threshold <= 0 {
		threshold = DefaultMergeThresholdBytes
	}
	maxLeaf := opts.MaxLeafBytes
	if maxLeaf <= 0 {
		maxLeaf = DefaultMaxLeafBytes
	}
	checkpoint := opts.WALCheckpointBytes
	if checkpoint <= 0 {
		checkpoint = DefaultWALCheckpointBytes
	}
	seed := opts.Seed
	if seed == 0 {
		seed = 42
	}

	cacheBytes := opts.CacheBytes
	if cacheBytes == 0 && opts.CacheEntries > 0 {
		cacheBytes = int64(opts.CacheEntries) * 160
	}
	if cacheBytes == 0 {
		cacheBytes = DefaultCacheBytes
	}

	compressionKind := segment.CompressionNone
	registry, err := compression.NewPersistentRegistry(fs, filepath.Join(opts.Dir, DefaultDictionaryDirName))
	if err != nil {
		return nil, fmt.Errorf("fusedb: open dictionary registry: %w", err)
	}
	catalog, err := compression.OpenDictionaryGroupCatalog(fs, opts.Dir)
	if err != nil {
		closeRegistry(registry)
		return nil, fmt.Errorf("fusedb: open dictionary group catalog: %w", err)
	}
	activeDictionary := opts.Dictionary
	if activeDictionary != nil {
		if err := catalog.EnsureNextAfter(activeDictionary.ID()); err != nil {
			closeRegistry(registry)
			return nil, fmt.Errorf("fusedb: reserve configured dictionary id: %w", err)
		}
		compressionKind = segment.CompressionLZ4Dict
		if err := registry.Save(activeDictionary); err != nil {
			_ = registry.Close()
			return nil, fmt.Errorf("fusedb: persist dictionary: %w", err)
		}
		if _, err := catalog.Publish(DefaultDictionaryGroupID, activeDictionary.ID()); err != nil {
			_ = registry.Close()
			return nil, fmt.Errorf("fusedb: publish configured dictionary: %w", err)
		}
	} else if activeID, ok := catalog.ActiveDictionaryID(DefaultDictionaryGroupID); ok {
		activeDictionary, err = registry.Load(activeID)
		if err != nil {
			closeRegistry(registry)
			return nil, fmt.Errorf("fusedb: load active dictionary %d: %w", activeID, err)
		}
		compressionKind = segment.CompressionLZ4Dict
	}

	// Databases created before the group catalog existed stored the exact
	// dictionary in each segment but lost the active compression policy on
	// reopen. A single referenced ID is unambiguous and can be migrated safely;
	// mixed legacy IDs remain readable but future output stays uncompressed
	// until an operator supplies or runtime training publishes a winner.
	if activeDictionary == nil && loaded != nil {
		inferredID, found, inferErr := inferSingleDictionaryID(fs, opts.Dir, loaded)
		if inferErr != nil {
			closeRegistry(registry)
			return nil, fmt.Errorf("fusedb: infer legacy active dictionary: %w", inferErr)
		}
		if found {
			if err := catalog.EnsureNextAfter(inferredID); err != nil {
				closeRegistry(registry)
				return nil, fmt.Errorf("fusedb: migrate legacy dictionary allocator: %w", err)
			}
			if _, err := catalog.Publish(DefaultDictionaryGroupID, inferredID); err != nil {
				closeRegistry(registry)
				return nil, fmt.Errorf("fusedb: migrate legacy dictionary policy: %w", err)
			}
			activeDictionary, err = registry.Load(inferredID)
			if err != nil {
				closeRegistry(registry)
				return nil, fmt.Errorf("fusedb: load migrated dictionary %d: %w", inferredID, err)
			}
			compressionKind = segment.CompressionLZ4Dict
		}
	}

	trainingConfig := normalizeDictionaryTraining(opts.DictionaryTraining)
	if err := trainingConfig.validate(); err != nil {
		closeRegistry(registry)
		return nil, fmt.Errorf("fusedb: dictionary training config: %w", err)
	}
	var runtimeTrainer *dictionaryTrainer
	treeOpts := tree.Options{
		FS:                  fs,
		Dir:                 opts.Dir,
		Seed:                seed,
		MaxLeafBytes:        maxLeaf,
		MergeThresholdBytes: threshold,
		TargetBlockSize:     segment.DefaultTargetBlockSize,
		BloomFalsePositive:  segment.DefaultBloomFilterFalseRate,
		Compression:         compressionKind,
		Dictionary:          activeDictionary,
		DictionaryForGroup: func(groupID uint64) (*compression.Dictionary, error) {
			id, ok := catalog.ActiveDictionaryID(groupID)
			if !ok {
				return nil, nil
			}
			return registry.Load(id)
		},
		DictionaryGroupLeaves: trainingConfig.GroupLeaves,
		ObserveDictionarySample: func(groupID uint64, raw []byte) {
			if runtimeTrainer != nil {
				runtimeTrainer.observe(groupID, raw)
			}
		},
		Registry: registry,
	}

	var leafTree *tree.Tree
	if loaded == nil {
		leafTree, err = tree.New(treeOpts)
	} else {
		leafTree, err = tree.Open(treeOpts, loaded)
	}
	if err != nil {
		closeRegistry(registry)
		return nil, fmt.Errorf("fusedb: open tree: %w", err)
	}
	if err := reconcileOrphanSegments(fs, opts.Dir, leafTree.Manifest()); err != nil {
		_ = leafTree.Close()
		closeRegistry(registry)
		return nil, fmt.Errorf("fusedb: reconcile orphan segments: %w", err)
	}
	if err := leafTree.RebalanceDictionaryGroups(); err != nil {
		_ = leafTree.Close()
		closeRegistry(registry)
		return nil, fmt.Errorf("fusedb: rebalance dictionary groups: %w", err)
	}
	if !formatPresent {
		// FORMAT is the commit marker for the directory-wide migration. Persist
		// the current manifest first even when no logical fields changed, then
		// publish FORMAT last. A crash before the final rename is retryable as a
		// legacy open and never advertises a partially upgraded database.
		if err := leafTree.SaveManifest(); err != nil {
			_ = leafTree.Close()
			closeRegistry(registry)
			return nil, fmt.Errorf("fusedb: migrate manifest to v%d: %w", manifest.CurrentFormatVersion, err)
		}
	}

	db := &DB{
		dir:             opts.Dir,
		fs:              fs,
		lock:            dirLock,
		format:          formatDescriptor,
		tree:            leafTree,
		cache:           newValueCache(cacheBytes),
		registry:        registry,
		catalog:         catalog,
		thresholdBytes:  threshold,
		checkpointBytes: checkpoint,
		telemetry:       telemetry,
	}
	db.dictionaryGC = normalizeDictionaryGC(opts.DictionaryGC)
	runtimeTrainer = newDictionaryTrainer(
		opts.DictionaryTraining,
		catalog,
		registry,
		db.recordBackgroundErr,
		db.requestDictionaryGC,
	)
	db.dictionaryTrainer = runtimeTrainer
	if !opts.DisableWAL {
		walPath := opts.WALPath
		if walPath == "" {
			walPath = filepath.Join(opts.Dir, "wal.log")
		}
		log, err := wal.Open(wal.Options{
			FS:                  fs,
			Path:                walPath,
			GroupCommitInterval: opts.WALGroupCommitInterval,
			SyncWrites:          opts.WALSyncWrites,
		})
		if err != nil {
			_ = leafTree.Close()
			closeRegistry(registry)
			return nil, fmt.Errorf("fusedb: open wal: %w", err)
		}
		db.wal = log
		db.walPath = walPath

		appliedSeq := uint64(0)
		if loaded != nil {
			appliedSeq = loaded.AppliedSeq
		}
		if err := db.replayWAL(walPath, appliedSeq); err != nil {
			_ = log.Close()
			_ = leafTree.Close()
			closeRegistry(registry)
			return nil, err
		}
	}
	if !formatPresent || formatDescriptor.Epoch < dbformat.CurrentEpoch {
		// FORMAT is the final commit marker for both legacy-manifest and epoch
		// migrations. Publish it only after every existing segment, catalog, and
		// WAL record has opened successfully under the new bounded-size rules.
		if err := dbformat.SaveCurrent(fs, opts.Dir); err != nil {
			_ = db.walClose()
			_ = leafTree.Close()
			closeRegistry(registry)
			return nil, fmt.Errorf("fusedb: publish database format: %w", err)
		}
		formatDescriptor = dbformat.Current()
		db.format = formatDescriptor
	}

	db.schedulerModelDisabled = opts.DisableSchedulerModelPersistence
	schedulerConfig := opts.SchedulerConfig
	if !db.schedulerModelDisabled {
		loadedModel, modelErr := loadSchedulerModel(db.fs, db.dir)
		switch {
		case modelErr == nil:
			schedulerConfig.InitialModel = &loadedModel
			db.schedulerModelPersisted.Store(loadedModel.Generation)
		case errors.Is(modelErr, os.ErrNotExist):
		case modelErr != nil:
			started := telemetry.BackgroundStarted(enginemetrics.BackgroundSchedulerModelPersist)
			telemetry.BackgroundFinished(enginemetrics.BackgroundSchedulerModelPersist, started, true, false)
			db.schedulerModelDirty.Store(true)
		}
		previousChanged := schedulerConfig.ModelChanged
		schedulerConfig.ModelChanged = func(generation uint64) {
			if previousChanged != nil {
				previousChanged(generation)
			}
			db.requestSchedulerModelPersist(generation)
		}
	}

	db.resourceMonitor = enginemetrics.StartResourceMonitor(telemetry, opts.ResourceConfig)
	background, err := scheduler.New(schedulerConfig, db.telemetry)
	if err != nil {
		db.resourceMonitor.Close()
		_ = db.walClose()
		_ = leafTree.Close()
		closeRegistry(registry)
		return nil, fmt.Errorf("fusedb: create scheduler: %w", err)
	}
	db.background = background
	db.checkpointTask = db.newCheckpointJob()
	db.mergeTask = db.newMergeJob()
	db.dictionaryGCTask = db.newDictionaryGCJob()
	db.schedulerModelTask = db.newSchedulerModelPersistJob()
	runtimeTrainer.attach(background)
	db.scheduleBackgroundWork()
	db.requestDictionaryGC()
	if db.schedulerModelDirty.Load() {
		db.requestSchedulerModelPersist(background.ModelSnapshot().Generation)
	}
	keepLock = true
	return db, nil
}

func inferSingleDictionaryID(fs disk.FS, dir string, catalog *manifest.Manifest) (uint32, bool, error) {
	var inferred uint32
	for _, leaf := range catalog.Leaves {
		if leaf.SegmentID == 0 {
			continue
		}
		path := segment.SegmentFileName(dir, leaf.SegmentID, leaf.SegmentVersion)
		opened, err := segment.OpenSegment(fs, path)
		if err != nil {
			return 0, false, err
		}
		id := opened.Header.DictionaryID
		if id == 0 {
			continue
		}
		if inferred != 0 && inferred != id {
			return 0, false, nil
		}
		inferred = id
	}
	return inferred, inferred != 0, nil
}

// Format returns a detached description of the on-disk compatibility
// contract accepted when this handle was opened.
func (db *DB) Format() FormatInfo {
	if db == nil {
		return FormatInfo{}
	}
	return FormatInfo{
		Epoch:            db.format.Epoch,
		MinReaderEpoch:   db.format.MinReaderEpoch,
		MinWriterEpoch:   db.format.MinWriterEpoch,
		RequiredFeatures: uint64(db.format.RequiredFeatures),
		OptionalFeatures: uint64(db.format.OptionalFeatures),
	}
}

// VerifyReport summarizes a successful full integrity scan.
type VerifyReport struct {
	FormatEpoch              uint32
	Leaves                   uint64
	Segments                 uint64
	Blocks                   uint64
	Keys                     uint64
	SegmentBytes             uint64
	AppliedSeq               uint64
	WALBaseSeq               uint64
	WALLastSeq               uint64
	WALRecords               uint64
	DictionaryGroups         uint64
	Dictionaries             uint64
	SchedulerModelGeneration uint64
}

// BackupReport describes one durable point-in-time backup archive.
type BackupReport struct {
	AppliedSeq   uint64
	Files        uint64
	Segments     uint64
	Dictionaries uint64
	Bytes        uint64
}

// Backup creates a verified point-in-time archive after an exact checkpoint.
// The request is admitted by current load and resource budgets, and serializes
// with merges and dictionary maintenance. Foreground reads and writes may
// continue once the short buffer freeze at the checkpoint boundary completes.
func (db *DB) Backup(ctx context.Context, archivePath string) (BackupReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil || db.background == nil || db.tree == nil {
		return BackupReport{}, errors.New("fusedb: database is not open")
	}
	if db.closed.Load() {
		return BackupReport{}, errors.New("fusedb: database is closed")
	}
	if err := db.backgroundErr(); err != nil {
		return BackupReport{}, err
	}
	inside, err := pathWithin(db.dir, archivePath)
	if err != nil {
		return BackupReport{}, fmt.Errorf("fusedb: resolve backup path: %w", err)
	}
	if inside {
		return BackupReport{}, errors.New("fusedb: backup archive must be outside the database directory")
	}

	var report BackupReport
	job := scheduler.Job{
		ID:          fmt.Sprintf("backup:%d", db.manualTaskID.Add(1)),
		Class:       scheduler.ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundBackup,
		Priority:    1e6,
		Cost: scheduler.Cost{
			CPUFraction:    0.15,
			DiskFraction:   0.70,
			MemoryFraction: 0.05,
			Interference:   0.85,
		},
		Run: func(jobCtx context.Context) error {
			created, backupErr := db.backupNow(jobCtx, archivePath)
			if backupErr == nil {
				report = created
			}
			return backupErr
		},
	}
	if err := db.background.Execute(ctx, job); err != nil {
		return BackupReport{}, err
	}
	return report, nil
}

func (db *DB) backupNow(ctx context.Context, archivePath string) (BackupReport, error) {
	if err := ctx.Err(); err != nil {
		return BackupReport{}, err
	}
	if err := db.checkpoint(); err != nil {
		db.recordBackgroundErr(err)
		return BackupReport{}, err
	}
	if _, err := db.tree.Verify(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return BackupReport{}, err
		}
		return BackupReport{}, fmt.Errorf("%w: backup source verification: %v", ErrCorruption, err)
	}
	if !db.schedulerModelDisabled {
		if err := db.persistSchedulerModelNow(ctx); err != nil {
			return BackupReport{}, fmt.Errorf("fusedb: persist scheduler model for backup: %w", err)
		}
	}
	formatData, err := disk.ReadFileLimited(db.fs, dbformat.FileName(db.dir), limits.MaxFormatBytes)
	if err != nil {
		return BackupReport{}, fmt.Errorf("fusedb: read database format for backup: %w", err)
	}
	formatDescriptor, err := dbformat.Decode(formatData)
	if err != nil {
		return BackupReport{}, fmt.Errorf("%w: database format for backup: %v", ErrCorruption, err)
	}
	if err := dbformat.CheckCompatible(formatDescriptor); err != nil {
		return BackupReport{}, fmt.Errorf("%w: database format for backup: %v", ErrCorruption, err)
	}
	if formatDescriptor != db.format {
		return BackupReport{}, fmt.Errorf("%w: database format changed while open", ErrCorruption)
	}

	snapshot := db.tree.Manifest()
	if snapshot.AppliedSeq == math.MaxUint64 {
		return BackupReport{}, fmt.Errorf("%w: applied WAL sequence overflow", ErrCorruption)
	}
	manifestData, err := snapshot.MarshalBinary()
	if err != nil {
		return BackupReport{}, fmt.Errorf("fusedb: encode backup manifest: %w", err)
	}
	catalogPath := filepath.Join(db.dir, compression.DefaultGroupCatalogFileName)
	catalogData, err := disk.ReadFileLimited(db.fs, catalogPath, limits.MaxGroupCatalogBytes)
	if err != nil {
		return BackupReport{}, fmt.Errorf("fusedb: read dictionary group catalog: %w", err)
	}
	if _, err := compression.DecodeDictionaryGroupCatalog(catalogData); err != nil {
		return BackupReport{}, fmt.Errorf("%w: dictionary group catalog: %v", ErrCorruption, err)
	}
	sources := []backup.Source{
		{Name: dbformat.DefaultFileName, Data: formatData},
		{Name: manifest.DefaultFileName, Data: manifestData},
		{Name: "wal.log", Data: wal.EmptyFile(snapshot.AppliedSeq + 1)},
		{Name: compression.DefaultGroupCatalogFileName, Data: catalogData},
	}
	if !db.schedulerModelDisabled {
		modelData, err := disk.ReadFileLimited(db.fs, schedulerModelPath(db.dir), limits.MaxSchedulerModelBytes)
		if err != nil {
			return BackupReport{}, fmt.Errorf("fusedb: read scheduler model for backup: %w", err)
		}
		if _, err := scheduler.DecodeModelSnapshot(modelData); err != nil {
			return BackupReport{}, fmt.Errorf("%w: scheduler model for backup: %v", ErrCorruption, err)
		}
		sources = append(sources, backup.Source{Name: DefaultSchedulerModelFileName, Data: modelData})
	}
	dictionaryIDs := make(map[uint32]struct{})
	for _, group := range db.catalog.Snapshot().Groups {
		if group.ActiveDictionaryID != 0 {
			dictionaryIDs[group.ActiveDictionaryID] = struct{}{}
		}
	}
	var segments uint64
	for _, leaf := range snapshot.Leaves {
		if leaf.SegmentID == 0 {
			continue
		}
		segmentPath := segment.SegmentFileName(db.dir, leaf.SegmentID, leaf.SegmentVersion)
		opened, err := segment.OpenSegment(db.fs, segmentPath)
		if err != nil {
			return BackupReport{}, fmt.Errorf("%w: inspect backup segment %d: %v", ErrCorruption, leaf.SegmentID, err)
		}
		if opened.Header.DictionaryID != 0 {
			dictionaryIDs[opened.Header.DictionaryID] = struct{}{}
		}
		sources = append(sources, backup.Source{Name: filepath.Base(segmentPath), Path: segmentPath})
		segments++
	}
	ids := make([]uint32, 0, len(dictionaryIDs))
	for id := range dictionaryIDs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		dictionaryPath := compression.DictionaryFileName(filepath.Join(db.dir, DefaultDictionaryDirName), id)
		persisted, err := compression.LoadDictionary(db.fs, dictionaryPath)
		if err != nil {
			return BackupReport{}, fmt.Errorf("%w: verify persisted dictionary %d: %v", ErrCorruption, id, err)
		}
		persistedID := persisted.ID()
		_ = persisted.Close()
		if persistedID != id {
			return BackupReport{}, fmt.Errorf("%w: persisted dictionary id %d, want %d", ErrCorruption, persistedID, id)
		}
		sources = append(sources, backup.Source{
			Name: filepath.ToSlash(filepath.Join(DefaultDictionaryDirName, filepath.Base(dictionaryPath))),
			Path: dictionaryPath,
		})
	}

	created, err := backup.Write(ctx, db.fs, archivePath, sources)
	if err != nil {
		return BackupReport{}, fmt.Errorf("fusedb: write backup: %w", err)
	}
	return BackupReport{
		AppliedSeq:   snapshot.AppliedSeq,
		Files:        uint64(len(created.Entries)),
		Segments:     segments,
		Dictionaries: uint64(len(ids)),
		Bytes:        created.Bytes,
	}, nil
}

func pathWithin(dir, name string) (bool, error) {
	if name == "" {
		return false, errors.New("empty path")
	}
	base, err := filepath.Abs(dir)
	if err != nil {
		return false, err
	}
	target, err := filepath.Abs(name)
	if err != nil {
		return false, err
	}
	relative, err := filepath.Rel(base, target)
	if err != nil {
		return false, err
	}
	return relative == "." || (relative != ".." && !filepath.IsAbs(relative) && !strings.HasPrefix(relative, ".."+string(filepath.Separator))), nil
}

// Verify performs a full checksum and structural scan as a preemptible
// maintenance job. Foreground pressure may cancel it with context.Canceled;
// callers can retry when the database is quiet.
func (db *DB) Verify(ctx context.Context) (VerifyReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil || db.background == nil || db.tree == nil {
		return VerifyReport{}, errors.New("fusedb: database is not open")
	}
	if db.closed.Load() {
		return VerifyReport{}, errors.New("fusedb: database is closed")
	}
	if err := db.backgroundErr(); err != nil {
		return VerifyReport{}, err
	}

	var report VerifyReport
	job := scheduler.Job{
		ID:          fmt.Sprintf("verify:%d", db.manualTaskID.Add(1)),
		Class:       scheduler.ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundVerify,
		Priority:    1,
		Cost: scheduler.Cost{
			CPUFraction:    0.10,
			DiskFraction:   0.50,
			MemoryFraction: 0.05,
			Interference:   0.70,
		},
		Preemptible: true,
		Run: func(jobCtx context.Context) error {
			verified, err := db.verifyNow(jobCtx)
			if err == nil {
				report = verified
			}
			return err
		},
	}
	if err := db.background.Execute(ctx, job); err != nil {
		return VerifyReport{}, err
	}
	return report, nil
}

func (db *DB) verifyNow(ctx context.Context) (VerifyReport, error) {
	formatDescriptor, err := dbformat.Load(db.fs, db.dir)
	if err != nil {
		return VerifyReport{}, fmt.Errorf("%w: verify database format: %v", ErrCorruption, err)
	}
	if err := dbformat.CheckCompatible(formatDescriptor); err != nil {
		return VerifyReport{}, fmt.Errorf("%w: verify database format: %v", ErrCorruption, err)
	}
	if formatDescriptor != db.format {
		return VerifyReport{}, fmt.Errorf("%w: database format changed while open", ErrCorruption)
	}
	if err := db.verifyPersistedManifest(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return VerifyReport{}, err
		}
		return VerifyReport{}, fmt.Errorf("%w: verify manifest: %v", ErrCorruption, err)
	}
	groups, dictionaries, err := db.verifyDictionaryCatalog(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return VerifyReport{}, err
		}
		return VerifyReport{}, fmt.Errorf("%w: verify dictionary catalog: %v", ErrCorruption, err)
	}
	modelGeneration, err := db.verifySchedulerModel(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return VerifyReport{}, err
		}
		return VerifyReport{}, fmt.Errorf("%w: verify scheduler model: %v", ErrCorruption, err)
	}
	treeReport, err := db.tree.Verify(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return VerifyReport{}, err
		}
		return VerifyReport{}, fmt.Errorf("%w: %v", ErrCorruption, err)
	}
	report := VerifyReport{
		FormatEpoch:              formatDescriptor.Epoch,
		Leaves:                   treeReport.Leaves,
		Segments:                 treeReport.Segments,
		Blocks:                   treeReport.Blocks,
		Keys:                     treeReport.Keys,
		SegmentBytes:             treeReport.DataBytes,
		AppliedSeq:               treeReport.AppliedSeq,
		DictionaryGroups:         groups,
		Dictionaries:             dictionaries,
		SchedulerModelGeneration: modelGeneration,
	}
	if db.wal == nil {
		return report, nil
	}
	if treeReport.AppliedSeq == math.MaxUint64 {
		return VerifyReport{}, fmt.Errorf("%w: applied WAL sequence overflow", ErrCorruption)
	}
	if err := db.wal.Sync(); err != nil {
		return VerifyReport{}, fmt.Errorf("fusedb: verify sync wal: %w", err)
	}

	walResult, err := wal.Iterate(db.fs, db.walPath, func(record wal.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Kind == ops.OpBatch {
			if len(record.Key) != 0 {
				return errors.New("batch record has key")
			}
			_, err := wal.DecodeBatch(record.Payload)
			return err
		}
		if len(record.Key) == 0 {
			return errors.New("empty key")
		}
		switch record.Kind {
		case ops.OpPut:
			return nil
		case ops.OpDelete:
			if len(record.Payload) != 0 {
				return errors.New("delete record has payload")
			}
			return nil
		case ops.OpInc:
			_, n := binary.Varint(record.Payload)
			if n <= 0 || n != len(record.Payload) {
				return errors.New("increment record has invalid payload")
			}
			return nil
		default:
			return fmt.Errorf("unknown record kind %d", record.Kind)
		}
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return VerifyReport{}, err
		}
		return VerifyReport{}, fmt.Errorf("%w: verify wal: %v", ErrCorruption, err)
	}
	if walResult.TruncatedTail {
		return VerifyReport{}, fmt.Errorf("%w: WAL has a truncated tail after sync", ErrCorruption)
	}
	expectedBase := treeReport.AppliedSeq + 1
	if walResult.BaseSeq > expectedBase {
		return VerifyReport{}, fmt.Errorf("%w: WAL base sequence %d is beyond durable sequence %d", ErrCorruption, walResult.BaseSeq, treeReport.AppliedSeq)
	}
	report.WALBaseSeq = walResult.BaseSeq
	report.WALLastSeq = walResult.LastSeq
	report.WALRecords = uint64(walResult.Count)
	return report, nil
}

func (db *DB) verifyPersistedManifest(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	persistedData, err := disk.ReadFileLimited(db.fs, manifest.FileName(db.dir), limits.MaxManifestBytes)
	if err != nil {
		return err
	}
	persisted, err := manifest.DecodeManifest(persistedData)
	if err != nil {
		return err
	}
	if persisted.SourceVersion() != manifest.CurrentFormatVersion {
		return fmt.Errorf("manifest version %d, want %d", persisted.SourceVersion(), manifest.CurrentFormatVersion)
	}
	liveData, err := db.tree.Manifest().MarshalBinary()
	if err != nil {
		return err
	}
	if !bytes.Equal(persistedData, liveData) {
		return errors.New("persisted manifest differs from live tree")
	}
	return nil
}

func (db *DB) verifyDictionaryCatalog(ctx context.Context) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	snapshot, err := compression.LoadDictionaryGroupSnapshot(db.fs, filepath.Join(db.dir, compression.DefaultGroupCatalogFileName))
	if err != nil {
		return 0, 0, err
	}
	seen := make(map[uint32]struct{}, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if _, ok := seen[group.ActiveDictionaryID]; ok {
			continue
		}
		persisted, err := compression.LoadDictionary(db.fs, compression.DictionaryFileName(
			filepath.Join(db.dir, DefaultDictionaryDirName), group.ActiveDictionaryID,
		))
		if err != nil {
			return 0, 0, err
		}
		id := persisted.ID()
		_ = persisted.Close()
		if id != group.ActiveDictionaryID {
			return 0, 0, fmt.Errorf("dictionary id %d, want %d", id, group.ActiveDictionaryID)
		}
		seen[id] = struct{}{}
	}
	return uint64(len(snapshot.Groups)), uint64(len(seen)), nil
}

// replayWAL re-applies log records that no segment covers yet.
//
// Records at or below appliedSeq are already durable in segments; replaying
// them would double every increment they contain.
func (db *DB) replayWAL(path string, appliedSeq uint64) error {
	_, err := wal.Iterate(db.fs, path, func(record wal.Record) error {
		if record.Seq <= appliedSeq {
			return nil
		}
		if record.Kind == ops.OpBatch {
			mutations, err := wal.DecodeBatch(record.Payload)
			if err != nil {
				return err
			}
			for _, mutation := range mutations {
				if record.Seq <= db.tree.AppliedSeqForKey(mutation.Key) {
					continue
				}
				switch mutation.Kind {
				case ops.OpPut:
					err = db.tree.Put(mutation.Key, mutation.Payload)
				case ops.OpDelete:
					err = db.tree.Delete(mutation.Key)
				case ops.OpInc:
					err = db.tree.Inc(mutation.Key, ops.DecodeInc(ops.Op{Kind: ops.OpInc, Data: mutation.Payload}))
				}
				if err != nil {
					return err
				}
			}
			return nil
		}
		if record.Seq <= db.tree.AppliedSeqForKey(record.Key) {
			return nil
		}
		switch record.Kind {
		case ops.OpPut:
			return db.tree.Put(record.Key, record.Payload)
		case ops.OpDelete:
			return db.tree.Delete(record.Key)
		case ops.OpInc:
			return db.tree.Inc(record.Key, ops.DecodeInc(record.Op()))
		default:
			return fmt.Errorf("fusedb: unknown wal record kind %d", record.Kind)
		}
	})
	if err != nil {
		return fmt.Errorf("fusedb: replay wal: %w", err)
	}
	return nil
}

// The log is optional: DisableWAL leaves db.wal nil, and every call site goes
// through these helpers so the nil case stays in one place.
func (db *DB) appendPut(key, value []byte) error {
	if db.wal == nil {
		return nil
	}
	_, err := db.wal.AppendPut(key, value)
	return err
}

func (db *DB) appendDelete(key []byte) error {
	if db.wal == nil {
		return nil
	}
	_, err := db.wal.AppendDelete(key)
	return err
}

func (db *DB) appendInc(key []byte, delta int64) error {
	if db.wal == nil {
		return nil
	}
	_, err := db.wal.AppendInc(key, delta)
	return err
}

func (db *DB) walLastSeq() uint64 {
	if db.wal == nil {
		return 0
	}
	return db.wal.LastSeq()
}

func (db *DB) walTruncate(seq uint64) error {
	if db.wal == nil {
		return nil
	}
	return db.wal.Truncate(seq)
}

func (db *DB) walClose() error {
	if db.wal == nil {
		return nil
	}
	return db.wal.Close()
}

func closeRegistry(registry *compression.Registry) {
	if registry != nil {
		_ = registry.Close()
	}
}

func (db *DB) Put(key, value []byte) error {
	started := db.telemetry.BeginWrite()
	defer db.telemetry.EndWrite(started)

	if err := db.backgroundErr(); err != nil {
		return err
	}
	if err := validateMutationInput(key, value); err != nil {
		return err
	}
	mutation := &db.mutationMu[mutationShard(key)]
	mutation.Lock()
	defer mutation.Unlock()
	db.visibilityMu.RLock()
	defer db.visibilityMu.RUnlock()

	db.applyMu.RLock()
	err := db.appendPut(key, value)
	if err != nil {
		if errors.Is(err, wal.ErrPersistence) {
			db.recordTerminalError(err)
		}
		db.applyMu.RUnlock()
		return err
	}
	err = db.tree.Put(key, value)
	db.applyMu.RUnlock()
	if err != nil {
		return err
	}

	db.invalidate(key)
	db.noteWALGrowth(len(key) + len(value))
	db.scheduleBackgroundWork()
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	started := db.telemetry.BeginRead()
	defer db.telemetry.EndRead(started)

	if err := db.backgroundErr(); err != nil {
		return nil, false, err
	}
	db.visibilityMu.RLock()
	defer db.visibilityMu.RUnlock()

	shard := cacheShard(key)
	epoch := db.cacheEpoch[shard].Load()
	if value, ok := db.cache.get(key, epoch); ok {
		return value, true, nil
	}

	value, ok, err := db.tree.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		db.cache.delete(key)
		return nil, false, nil
	}

	// tree.Get hands back a view of live skiplist or block memory. Returning it
	// straight to the caller would let a caller-side write corrupt the
	// database, so the caller gets its own copy and the cache clones separately.
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	if db.cacheEpoch[shard].Load() == epoch {
		db.cache.set(key, value, epoch)
	}
	return valueCopy, true, nil
}

func (db *DB) Delete(key []byte) error {
	started := db.telemetry.BeginWrite()
	defer db.telemetry.EndWrite(started)

	if err := db.backgroundErr(); err != nil {
		return err
	}
	if err := validateMutationInput(key, nil); err != nil {
		return err
	}
	mutation := &db.mutationMu[mutationShard(key)]
	mutation.Lock()
	defer mutation.Unlock()
	db.visibilityMu.RLock()
	defer db.visibilityMu.RUnlock()

	db.applyMu.RLock()
	err := db.appendDelete(key)
	if err != nil {
		if errors.Is(err, wal.ErrPersistence) {
			db.recordTerminalError(err)
		}
		db.applyMu.RUnlock()
		return err
	}
	err = db.tree.Delete(key)
	db.applyMu.RUnlock()
	if err != nil {
		return err
	}

	db.invalidate(key)
	db.noteWALGrowth(len(key))
	db.scheduleBackgroundWork()
	return nil
}

func (db *DB) Inc(key []byte, delta int64) error {
	started := db.telemetry.BeginWrite()
	defer db.telemetry.EndWrite(started)

	if err := db.backgroundErr(); err != nil {
		return err
	}
	if err := validateMutationInput(key, nil); err != nil {
		return err
	}
	mutation := &db.mutationMu[mutationShard(key)]
	mutation.Lock()
	defer mutation.Unlock()
	db.visibilityMu.RLock()
	defer db.visibilityMu.RUnlock()

	db.applyMu.RLock()
	encoded, found, err := db.tree.GetEncoded(key)
	if err != nil {
		db.applyMu.RUnlock()
		return err
	}
	if found {
		kind, kindErr := value.KindOf(encoded)
		if kindErr != nil || kind != value.KindInt64 {
			db.applyMu.RUnlock()
			if kindErr != nil {
				return kindErr
			}
			return value.ErrKindMismatch
		}
	}
	err = db.appendInc(key, delta)
	if err != nil {
		if errors.Is(err, wal.ErrPersistence) {
			db.recordTerminalError(err)
		}
		db.applyMu.RUnlock()
		return err
	}
	err = db.tree.Inc(key, delta)
	db.applyMu.RUnlock()
	if err != nil {
		return err
	}

	db.invalidate(key)
	db.noteWALGrowth(len(key) + 8)
	db.scheduleBackgroundWork()
	return nil
}

func validateMutationInput(key, valueBytes []byte) error {
	return validateMutationLengths(len(key), len(valueBytes))
}

func validateMutationLengths(keyBytes, valueBytes int) error {
	if keyBytes == 0 {
		return tree.ErrEmptyKey
	}
	if keyBytes > MaxKeyBytes {
		return fmt.Errorf("%w: %d > %d", ErrKeyTooLarge, keyBytes, MaxKeyBytes)
	}
	if valueBytes > MaxValueBytes {
		return fmt.Errorf("%w: %d > %d", ErrValueTooLarge, valueBytes, MaxValueBytes)
	}
	return nil
}

func mutationShard(key []byte) uint64 {
	return xxhash.Sum64(key) % mutationLockShards
}

// invalidate drops the cached value for key and bumps only its shard counter.
func (db *DB) invalidate(key []byte) {
	db.cacheEpoch[cacheShard(key)].Add(1)
	db.cache.delete(key)
}

func (db *DB) noteWALGrowth(n int) {
	db.walBytes.Add(int64(n))
}

// Merge asks the background scheduler to merge every leaf and truncate the log
// up to the merged watermark. The call waits for that mandatory job so manual
// and automatic maintenance can never execute concurrently.
func (db *DB) Merge() error {
	if db == nil || db.background == nil {
		return errors.New("fusedb: database is not open")
	}
	if db.closed.Load() {
		return errors.New("fusedb: database is closed")
	}
	if err := db.backgroundErr(); err != nil {
		return err
	}

	done := make(chan error, 1)
	job := db.checkpointTask
	job.ID = fmt.Sprintf("checkpoint:manual:%d", db.manualTaskID.Add(1))
	job.Priority = 1e6
	job.Mandatory = func() bool { return true }
	job.Needed = nil
	job.OnDone = func(err error) {
		db.backgroundJobDone(err)
		done <- err
	}
	if !db.background.Submit(job) {
		return errors.New("fusedb: scheduler is closed")
	}
	return <-done
}

// checkpoint freezes all buffers at an exact log position, merges them, then
// records the watermark and truncates the log.
func (db *DB) checkpoint() error {
	db.applyMu.Lock()
	watermark := db.walLastSeq()
	checkpointDebt := db.walBytes.Swap(0)
	db.tree.FreezeAll()
	db.applyMu.Unlock()
	committed := false
	defer func() {
		if !committed {
			db.walBytes.Add(checkpointDebt)
		}
	}()

	if err := db.tree.MergeAllThrough(watermark); err != nil {
		return fmt.Errorf("fusedb: merge: %w", err)
	}

	if err := db.tree.SetAppliedSeq(watermark); err != nil {
		return fmt.Errorf("fusedb: set applied sequence: %w", err)
	}
	if err := db.tree.SaveManifest(); err != nil {
		return fmt.Errorf("fusedb: save manifest: %w", err)
	}
	if err := db.walTruncate(watermark); err != nil {
		return fmt.Errorf("fusedb: truncate wal: %w", err)
	}
	committed = true
	return nil
}

// mergeLeaves merges only the leaves that crossed the threshold.
//
// This is the common path and deliberately does not move the log watermark:
// only some leaves are merged, so records for the others are still needed.
func (db *DB) mergeLeaves() error {
	pending := db.tree.PendingMerge()
	for _, l := range pending {
		db.applyMu.Lock()
		watermark := db.walLastSeq()
		l.FreezeBuffer()
		db.applyMu.Unlock()
		if err := db.tree.MergeLeafThrough(l, watermark); err != nil {
			return fmt.Errorf("fusedb: merge leaf %d: %w", l.ID(), err)
		}
	}
	return nil
}

func (db *DB) Close() error {
	return db.close(true)
}

// CloseWithoutCheckpoint releases a database opened only for restore
// validation. It must not be used by normal callers because buffered writes
// would be discarded; the supported public package does not expose it.
func (db *DB) CloseWithoutCheckpoint() error {
	return db.close(false)
}

func (db *DB) close(runCheckpoint bool) error {
	if db == nil || db.tree == nil {
		return nil
	}

	var err error
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		if db.background != nil {
			_ = db.background.Close()
		}
		if !db.schedulerModelDisabled && db.background != nil {
			if modelErr := db.persistSchedulerModelNow(context.Background()); modelErr != nil {
				err = fmt.Errorf("fusedb: persist scheduler model on close: %w", modelErr)
			}
		}
		if db.dictionaryTrainer != nil {
			db.dictionaryTrainer.close()
		}
		if db.resourceMonitor != nil {
			db.resourceMonitor.Close()
		}

		backgroundErr := db.backgroundErr()
		// A commit-uncertain background failure requires reopen. Running another
		// checkpoint from the stale in-memory view could overwrite the complete
		// target that is already visible in the filesystem namespace.
		if runCheckpoint && backgroundErr == nil {
			if checkpointErr := db.checkpoint(); checkpointErr != nil {
				err = checkpointErr
			}
		} else if backgroundErr != nil {
			err = backgroundErr
		}
		if closeErr := db.walClose(); err == nil && closeErr != nil {
			err = closeErr
		}
		if closeErr := db.tree.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
		if db.registry != nil {
			if closeErr := db.registry.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
		if db.lock != nil {
			if closeErr := db.lock.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
			db.lock = nil
		}
		if backgroundErr := db.backgroundErr(); err == nil && backgroundErr != nil {
			err = backgroundErr
		}
	})
	return err
}

// BufferedBytes reports how many bytes of buffered operations are unmerged.
func (db *DB) BufferedBytes() int64 {
	if db == nil || db.tree == nil {
		return 0
	}
	return db.tree.BufferedBytes()
}

// LeafCount reports how many leaves partition the keyspace.
func (db *DB) LeafCount() int {
	if db == nil || db.tree == nil {
		return 0
	}
	return db.tree.LeafCount()
}

// WALBytes reports write-ahead-log bytes accumulated since the last exact
// checkpoint. It is an operational debt signal, not the physical WAL file
// size.
func (db *DB) WALBytes() int64 {
	if db == nil {
		return 0
	}
	return max(0, db.walBytes.Load())
}

// PendingMergeLeaves reports leaves currently above their merge threshold or
// waiting for a previously failed merge retry.
func (db *DB) PendingMergeLeaves() int {
	if db == nil || db.tree == nil {
		return 0
	}
	return len(db.tree.PendingMerge())
}

// Health returns readiness without performing filesystem I/O. Inspecting WAL
// terminal state also publishes the corresponding terminal metric once.
func (db *DB) Health() HealthStatus {
	status := HealthStatus{CheckedAt: time.Now()}
	if db == nil || db.tree == nil {
		status.Closed = true
		return status
	}
	status.Closed = db.closed.Load()
	status.TerminalError = db.backgroundErr() != nil
	status.Ready = !status.Closed && !status.TerminalError
	return status
}

// MetricsSnapshot returns cumulative engine telemetry suitable for scheduler
// diagnostics and future metrics exporters.
func (db *DB) MetricsSnapshot() enginemetrics.Snapshot {
	if db == nil || db.telemetry == nil {
		return enginemetrics.Snapshot{}
	}
	return db.telemetry.Snapshot()
}

// SchedulerState reports the current adaptive load classification.
func (db *DB) SchedulerState() scheduler.State {
	if db == nil || db.background == nil {
		return scheduler.StateNormal
	}
	return db.background.State()
}

// SchedulerLoad returns the current rolling foreground observation.
func (db *DB) SchedulerLoad() scheduler.Load {
	if db == nil || db.background == nil {
		return scheduler.Load{}
	}
	return db.background.Load()
}

// SchedulerRuntimeStats reports bounded-cardinality queue and executor state.
func (db *DB) SchedulerRuntimeStats() scheduler.RuntimeStats {
	if db == nil || db.background == nil {
		return scheduler.RuntimeStats{Closed: true}
	}
	return db.background.RuntimeStats()
}

func (db *DB) scheduleBackgroundWork() {
	if db == nil || db.closed.Load() || db.background == nil || db.backgroundErr() != nil {
		return
	}
	if db.walBytes.Load() >= db.checkpointBytes {
		db.background.Submit(db.checkpointTask)
	}
	if db.tree.BufferedBytes() >= db.thresholdBytes {
		db.background.Submit(db.mergeTask)
	}
}

func (db *DB) newCheckpointJob() scheduler.Job {
	return scheduler.Job{
		ID:          "checkpoint",
		Class:       scheduler.ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundCheckpoint,
		Priority:    100,
		Cost: scheduler.Cost{
			CPUFraction:    0.15,
			DiskFraction:   0.45,
			MemoryFraction: 0.10,
			Interference:   0.80,
		},
		Mandatory: func() bool {
			return db.walBytes.Load() >= db.checkpointBytes
		},
		Needed: func() bool {
			return db.backgroundErr() == nil && db.walBytes.Load() >= db.checkpointBytes
		},
		Urgency: func() float64 {
			return float64(db.walBytes.Load()) / float64(db.checkpointBytes)
		},
		Run: func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return db.checkpoint()
		},
		OnDone: db.backgroundJobDone,
	}
}

func (db *DB) newMergeJob() scheduler.Job {
	return scheduler.Job{
		ID:          "merge",
		Class:       scheduler.ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundMerge,
		Priority:    10,
		Cost: scheduler.Cost{
			CPUFraction:    0.10,
			DiskFraction:   0.35,
			MemoryFraction: 0.10,
			Interference:   0.60,
		},
		Mandatory: func() bool {
			return db.tree.BufferedBytes() >= 4*db.thresholdBytes
		},
		Needed: func() bool {
			return db.backgroundErr() == nil && len(db.tree.PendingMerge()) > 0
		},
		Urgency: func() float64 {
			return float64(db.tree.BufferedBytes()) / float64(db.thresholdBytes)
		},
		Run: func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return db.mergeLeaves()
		},
		OnDone: db.backgroundJobDone,
	}
}

func (db *DB) backgroundJobDone(err error) {
	if err == nil {
		db.requestDictionaryGC()
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	db.recordBackgroundErr(err)
}

// backgroundErr returns the first failure the merge worker hit, if any.
func (db *DB) backgroundErr() error {
	if db == nil {
		return nil
	}
	if err := db.mergeErr.Load(); err != nil && *err != nil {
		return *err
	}
	if db.wal != nil {
		if err := db.wal.Err(); err != nil {
			db.recordTerminalError(err)
			return err
		}
	}
	return nil
}

func (db *DB) recordBackgroundErr(err error) {
	if err == nil {
		return
	}
	if db.mergeErr.CompareAndSwap(nil, &err) {
		db.recordTerminalError(err)
	}
}

func (db *DB) recordTerminalError(err error) {
	if db == nil || err == nil {
		return
	}
	db.terminalMetricOnce.Do(func() {
		db.telemetry.RecordTerminalError(errors.Is(err, disk.ErrCommitUncertain))
	})
}
