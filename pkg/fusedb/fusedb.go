// Package fusedb provides an embedded key-value database for hot mutable state.
//
// FuseDB is designed for workloads with frequent writes and point reads,
// using a lock-free skiplist buffer and immutable compressed segments.
// DB values are safe for concurrent use by multiple goroutines.
//
// Basic usage:
//
//	db, err := fusedb.Open(fusedb.Options{
//	    Dir:        "/tmp/mydb",
//	    CacheSize:  5 << 20, // 5MB
//	    MergeSize:  5 << 20, // 5MB
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	// Put a value
//	err = db.Put([]byte("key"), []byte("value"))
//
//	// Get a value
//	value, found, err := db.Get([]byte("key"))
//
//	// Increment and read a counter
//	err = db.Inc([]byte("counter"), 1)
//	count, found, err := db.GetInt64([]byte("counter"))
//
//	// Delete a key
//	err = db.Delete([]byte("key"))
//
//	if err := db.Close(); err != nil {
//	    log.Fatal(err)
//	}
package fusedb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uchebnick/fusedb/internal/backup"
	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/dbformat"
	"github.com/uchebnick/fusedb/internal/disk"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
	"github.com/uchebnick/fusedb/internal/tree"
	"github.com/uchebnick/fusedb/internal/value"
	"github.com/uchebnick/fusedb/pkg/oneleafdb"
)

// ErrEmptyKey is returned by Put, Delete, and Inc for a zero-length key.
//
// The on-disk block format encodes a key length that cannot be zero, so the
// empty key has no representation in a segment and is refused on write rather
// than lost at merge time. Get treats it as a plain miss.
var ErrEmptyKey = tree.ErrEmptyKey

// ErrKeyTooLarge and ErrValueTooLarge reject mutations above the hard
// persisted-data limits before anything is appended to the WAL.
var ErrKeyTooLarge = oneleafdb.ErrKeyTooLarge
var ErrValueTooLarge = oneleafdb.ErrValueTooLarge

const MaxKeySize = oneleafdb.MaxKeyBytes
const MaxValueSize = oneleafdb.MaxValueBytes

// ErrInvalidBackup reports an unsupported or structurally invalid archive.
var ErrInvalidBackup = backup.ErrInvalidArchive

// ErrBackupChecksum reports archive corruption detected before extraction.
var ErrBackupChecksum = backup.ErrArchiveChecksum

// ErrRestoreDestinationNotEmpty requires restoring into a new or empty path.
var ErrRestoreDestinationNotEmpty = backup.ErrDestinationNotEmpty

// ErrDatabaseLocked is returned when another FuseDB instance already owns the
// same database directory.
var ErrDatabaseLocked = oneleafdb.ErrDatabaseLocked

// ErrCorruption is returned by Verify when persisted state violates a
// checksum, ordering, range, dictionary, manifest, or WAL invariant.
var ErrCorruption = oneleafdb.ErrCorruption

// ErrSchedulerModelPersistence reports that optional learned scheduler history
// could not be saved. User data durability is unaffected.
var ErrSchedulerModelPersistence = oneleafdb.ErrSchedulerModelPersistence

// ErrFormatTooNew and ErrFormatTooOld reject a database whose declared
// compatibility epoch cannot be safely opened by this binary.
var ErrFormatTooNew = oneleafdb.ErrFormatTooNew
var ErrFormatTooOld = oneleafdb.ErrFormatTooOld

// ErrUnknownRequiredFeature rejects a database using a mandatory feature this
// binary does not understand. Unknown optional features remain readable.
var ErrUnknownRequiredFeature = oneleafdb.ErrUnknownRequiredFeature

// ErrMissingRequiredFeature rejects a descriptor that omits a feature every
// database in its declared epoch must use.
var ErrMissingRequiredFeature = oneleafdb.ErrMissingRequiredFeature

// ErrFormatMismatch reports that FORMAT and required database files disagree.
var ErrFormatMismatch = oneleafdb.ErrFormatMismatch

// ErrFormatCorrupt and ErrFormatCodec report an invalid or unsupported FORMAT
// descriptor before any database files are mutated.
var ErrFormatCorrupt = oneleafdb.ErrFormatCorrupt
var ErrFormatCodec = oneleafdb.ErrFormatCodec

// ErrWALPersistence reports a terminal WAL write or sync failure. Stop using
// the handle, close it, and reopen before retrying or reconciling the mutation.
var ErrWALPersistence = oneleafdb.ErrWALPersistence

// ErrCGODisabled reports that an operation requiring the native LZ4
// dictionary codec was attempted in a binary built with CGO disabled.
var ErrCGODisabled = compression.ErrCGODisabled

// CurrentFormatEpoch and the feature bits identify values returned by
// DB.Format. Required features affect data interpretation; optional features
// may be ignored without risking user data.
const CurrentFormatEpoch = dbformat.CurrentEpoch

const (
	FormatFeatureManifestV4                = uint64(dbformat.FeatureManifestV4)
	FormatFeaturePerLeafWALWatermarks      = uint64(dbformat.FeaturePerLeafWALWatermarks)
	FormatFeatureDictionaryGroups          = uint64(dbformat.FeatureDictionaryGroups)
	FormatFeatureExternalDictionaryCatalog = uint64(dbformat.FeatureExternalDictionaryCatalog)
	FormatFeatureBoundedRecordSizes        = uint64(dbformat.FeatureBoundedRecordSizes)
	FormatFeatureAtomicBatches             = uint64(dbformat.FeatureAtomicBatches)
)

const FormatOptionalSchedulerModel = uint64(dbformat.OptionalSchedulerModel)

// ErrCommitUncertain reports that an atomic replacement was renamed into
// place but its parent directory could not be synced. Stop using the handle,
// close it, and reopen the database before deciding whether to retry.
var ErrCommitUncertain = disk.ErrCommitUncertain

// ErrValueType is returned when an operation expects an integer counter but
// finds an ordinary byte value, or vice versa.
var ErrValueType = value.ErrKindMismatch

// ErrIdempotencyConflict reports reuse of an ApplyOnce key for a different
// ordered mutation set. The original transaction remains unchanged.
var ErrIdempotencyConflict = oneleafdb.ErrIdempotencyConflict

var ErrEmptyTransaction = oneleafdb.ErrEmptyTransaction
var ErrTooManyMutations = oneleafdb.ErrTooManyMutations
var ErrDuplicateMutationKey = oneleafdb.ErrDuplicateMutationKey

// DB is an embedded key-value database safe for concurrent use.
type DB struct {
	db *oneleafdb.DB
}

// Options configures a FuseDB instance.
type Options struct {
	// Dir is the directory path for database files.
	// Required.
	Dir string

	// CacheSize is the value cache size in bytes.
	// Default: 5MB.
	CacheSize int64

	// MergeSize is the buffer size threshold that triggers merge.
	// Default: 8MB.
	MergeSize int64

	// MaxLeafSize is the size at which a leaf splits into two.
	// Default: 64MB.
	//
	// It bounds how much data a single merge rewrites, so it is the main knob
	// for write amplification: a smaller leaf costs less per merge but leaves
	// more leaves to track.
	MaxLeafSize int64

	// WALPath is the write-ahead log file path.
	// If empty, uses Dir + "/wal.log".
	WALPath string

	// WALSyncWrites enables synchronous WAL writes.
	// Default: false (async group commit every 200µs).
	WALSyncWrites bool

	// WALGroupCommitInterval is the maximum coalescing interval for async WAL
	// flushes and synchronous group commits.
	// Default: 200µs.
	WALGroupCommitInterval time.Duration

	// WALCheckpointBytes is the logical WAL growth that makes an exact
	// checkpoint mandatory. Zero uses 64 MiB. Lower values reduce recovery debt
	// at the cost of more frequent whole-tree checkpoints.
	WALCheckpointBytes int64

	// DictionaryPath is the path to a compression dictionary file (.zdict).
	// If empty, runtime training may publish dictionaries from real merged
	// blocks. A supplied dictionary seeds the default group immediately.
	DictionaryPath string

	// DictionaryTraining controls adaptive per-group dictionary evolution.
	// Zero values enable conservative bounded defaults.
	DictionaryTraining DictionaryTrainingOptions

	// DictionaryGC controls scheduler-driven cleanup of immutable dictionary
	// versions no longer referenced by an active group or live segment.
	DictionaryGC DictionaryGCOptions

	// Scheduler controls adaptive background merge and future dictionary jobs.
	Scheduler SchedulerOptions

	// Metrics controls foreground latency sampling. Counters are always exact.
	Metrics MetricsOptions
}

// SchedulerOptions configures background admission from foreground load.
// Zero fields select automatic or built-in defaults.
type SchedulerOptions struct {
	PollInterval         time.Duration
	ObservationWindow    time.Duration
	QuietConfirm         time.Duration
	RecoveryPeriod       time.Duration
	TargetReadP99        time.Duration
	TargetWriteP99       time.Duration
	QuietRateCeiling     float64
	QuietRateRatio       float64
	BusyRateRatio        float64
	QuietHeadroom        float64
	QuietMaxRateCV       float64
	OverloadRatio        float64
	MaxCPUUtilization    float64
	MaxDiskUtilization   float64
	MaxMemoryUtilization float64

	// CPUCores, DiskBytesPerSecond, and MemoryBytes are absolute capacities
	// available to background scheduling. Zero detects or learns the capacity.
	CPUCores           float64
	DiskBytesPerSecond float64
	MemoryBytes        uint64

	// DisableModelPersistence opts out of crash-safe learned UTC time-of-week
	// scheduler history. The default persists it inside the database directory.
	DisableModelPersistence bool
}

// MetricsOptions configures low-overhead latency sampling.
type MetricsOptions struct {
	// LatencySampleEvery records one latency for every N operations. Zero uses
	// the default of 64. Operation counters are never sampled.
	LatencySampleEvery uint64
}

// DurablePilotOptions returns fail-safer defaults for a rebuildable production
// pilot. It enables synchronous WAL durability, reserves host headroom, samples
// foreground latency, and disables adaptive dictionary training until it has
// been qualified on the deployment machine. Callers should still set explicit
// hardware capacities and latency targets for their environment.
func DurablePilotOptions(dir string) Options {
	return Options{
		Dir:           dir,
		WALSyncWrites: true,
		Scheduler: SchedulerOptions{
			TargetReadP99:        10 * time.Millisecond,
			TargetWriteP99:       50 * time.Millisecond,
			MaxCPUUtilization:    0.70,
			MaxDiskUtilization:   0.70,
			MaxMemoryUtilization: 0.70,
		},
		Metrics:            MetricsOptions{LatencySampleEvery: 16},
		DictionaryTraining: DictionaryTrainingOptions{Disabled: true},
	}
}

// DictionaryTrainingOptions controls bounded runtime LZ4 training. Training
// and held-out evaluation run only through the adaptive scheduler.
type DictionaryTrainingOptions struct {
	Disabled                 bool
	GroupLeaves              int
	DictionarySize           int
	ChunkBytes               int
	MaxChunks                int
	MinimumTrainingSamples   int
	MinimumEvaluationSamples int
	MaxSamplesPerGroup       int
	MaxSampleBytesPerGroup   int64
	MaxTotalSampleBytes      int64
	MinimumGain              float64
}

// DictionaryGCOptions bounds one automatic dictionary cleanup pass. Cleanup
// is enabled by default and is always admitted through the scheduler.
type DictionaryGCOptions struct {
	Disabled       bool
	MaxFilesPerRun int
}

// Open opens or creates a FuseDB database.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("fusedb: Dir is required")
	}

	var dict *compression.Dictionary
	if opts.DictionaryPath != "" {
		var err error
		dict, err = compression.LoadDictionary(disk.DefaultFS, opts.DictionaryPath)
		if err != nil {
			return nil, fmt.Errorf("fusedb: load dictionary: %w", err)
		}
	}

	db, err := oneleafdb.OpenDB(oneleafdb.DBOptions{
		Dir:                    opts.Dir,
		ThresholdBytes:         opts.MergeSize,
		MaxLeafBytes:           opts.MaxLeafSize,
		Dictionary:             dict,
		WALPath:                opts.WALPath,
		WALGroupCommitInterval: opts.WALGroupCommitInterval,
		WALSyncWrites:          opts.WALSyncWrites,
		WALCheckpointBytes:     opts.WALCheckpointBytes,
		CacheBytes:             opts.CacheSize,
		SchedulerConfig: scheduler.Config{
			PollInterval:         opts.Scheduler.PollInterval,
			ObservationWindow:    opts.Scheduler.ObservationWindow,
			QuietConfirm:         opts.Scheduler.QuietConfirm,
			RecoveryPeriod:       opts.Scheduler.RecoveryPeriod,
			TargetReadP99:        opts.Scheduler.TargetReadP99,
			TargetWriteP99:       opts.Scheduler.TargetWriteP99,
			QuietRateCeiling:     opts.Scheduler.QuietRateCeiling,
			QuietRateRatio:       opts.Scheduler.QuietRateRatio,
			BusyRateRatio:        opts.Scheduler.BusyRateRatio,
			QuietHeadroom:        opts.Scheduler.QuietHeadroom,
			QuietMaxRateCV:       opts.Scheduler.QuietMaxRateCV,
			OverloadRatio:        opts.Scheduler.OverloadRatio,
			MaxCPUUtilization:    opts.Scheduler.MaxCPUUtilization,
			MaxDiskUtilization:   opts.Scheduler.MaxDiskUtilization,
			MaxMemoryUtilization: opts.Scheduler.MaxMemoryUtilization,
		},
		MetricsConfig: enginemetrics.Config{
			LatencySampleEvery: opts.Metrics.LatencySampleEvery,
		},
		ResourceConfig: enginemetrics.ResourceConfig{
			CPUCores:           opts.Scheduler.CPUCores,
			DiskBytesPerSecond: opts.Scheduler.DiskBytesPerSecond,
			MemoryBytes:        opts.Scheduler.MemoryBytes,
			SampleInterval:     opts.Scheduler.PollInterval,
		},
		DisableSchedulerModelPersistence: opts.Scheduler.DisableModelPersistence,
		DictionaryTraining: oneleafdb.DictionaryTrainingConfig{
			Disabled:                 opts.DictionaryTraining.Disabled,
			GroupLeaves:              opts.DictionaryTraining.GroupLeaves,
			DictionarySize:           opts.DictionaryTraining.DictionarySize,
			ChunkBytes:               opts.DictionaryTraining.ChunkBytes,
			MaxChunks:                opts.DictionaryTraining.MaxChunks,
			MinimumTrainingSamples:   opts.DictionaryTraining.MinimumTrainingSamples,
			MinimumEvaluationSamples: opts.DictionaryTraining.MinimumEvaluationSamples,
			MaxSamplesPerGroup:       opts.DictionaryTraining.MaxSamplesPerGroup,
			MaxSampleBytesPerGroup:   opts.DictionaryTraining.MaxSampleBytesPerGroup,
			MaxTotalSampleBytes:      opts.DictionaryTraining.MaxTotalSampleBytes,
			MinimumGain:              opts.DictionaryTraining.MinimumGain,
		},
		DictionaryGC: oneleafdb.DictionaryGCConfig{
			Disabled:       opts.DictionaryGC.Disabled,
			MaxFilesPerRun: opts.DictionaryGC.MaxFilesPerRun,
		},
	})
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

// Put stores value under key.
//
// Put copies both slices before publishing the operation, so the caller may
// reuse or modify them after Put returns. An empty key returns ErrEmptyKey;
// oversized input returns ErrKeyTooLarge or ErrValueTooLarge before WAL append.
func (db *DB) Put(key, value []byte) error {
	return db.db.Put(key, value)
}

// Get retrieves a byte value by key.
//
// It returns (value, true, nil) when found and (nil, false, nil) when missing.
// The returned slice is owned by the caller and may be safely modified. Values
// created with Inc use an internal integer encoding; read those with GetInt64.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	return db.db.Get(key)
}

// GetInt64 retrieves a counter created with Inc.
//
// It returns (value, true, nil) when found and (0, false, nil) when missing.
// If key contains a byte value written by Put, the returned error wraps
// ErrValueType and found remains true.
func (db *DB) GetInt64(key []byte) (int64, bool, error) {
	raw, found, err := db.db.Get(key)
	if err != nil || !found {
		return 0, found, err
	}

	decoded, err := value.DecodeInt64(raw)
	if err != nil {
		return 0, true, fmt.Errorf("fusedb: decode counter: %w: %v", ErrValueType, err)
	}
	return decoded, true, nil
}

// Delete records a tombstone for key. Deleting a missing key succeeds. An
// empty or oversized key returns ErrEmptyKey or ErrKeyTooLarge before WAL append.
func (db *DB) Delete(key []byte) error {
	return db.db.Delete(key)
}

// Inc atomically increments an int64 counter by delta.
//
// A missing key is created with delta as its value. Use GetInt64 to read the
// counter. Incrementing a byte value written by Put returns ErrValueType and
// is not logged. An empty or oversized key returns ErrEmptyKey or
// ErrKeyTooLarge before WAL append.
func (db *DB) Inc(key []byte, delta int64) error {
	return db.db.Inc(key, delta)
}

// Close checkpoints buffered writes and releases database resources.
// It is safe to call Close more than once.
func (db *DB) Close() error {
	return db.db.Close()
}

// Merge flushes every buffered write into segments and truncates the log.
//
// It enters the same serialized scheduler queue as automatic maintenance and
// waits for completion. Call it explicitly only to force a checkpoint at a
// known point; Close also performs a final checkpoint.
func (db *DB) Merge() error {
	return db.db.Merge()
}

// Verify performs a full, read-only integrity scan of the manifest, persisted
// scheduler model, dictionary group catalog, active dictionaries, every
// referenced segment (including LZ4 dictionary decompression), and the WAL.
// It runs through the adaptive background scheduler and may return
// context.Canceled when foreground pressure preempts it.
func (db *DB) Verify(ctx context.Context) (VerifyReport, error) {
	report, err := db.db.Verify(ctx)
	return VerifyReport(report), err
}

// Format returns the on-disk compatibility epoch and feature masks accepted
// when the database was opened.
func (db *DB) Format() FormatInfo {
	return FormatInfo(db.db.Format())
}

// Backup creates a verified, checksummed point-in-time archive. It runs an
// exact checkpoint through the adaptive scheduler and serializes with all
// other maintenance. The archive path must be outside the database directory.
func (db *DB) Backup(ctx context.Context, archivePath string) (BackupReport, error) {
	report, err := db.db.Backup(ctx, archivePath)
	return BackupReport(report), err
}

// CollectDictionaryGarbage runs a complete, preemptible dictionary cleanup
// through the adaptive scheduler. Automatic bounded cleanup is enabled by
// default, so applications normally use this only for operational maintenance.
func (db *DB) CollectDictionaryGarbage(ctx context.Context) (DictionaryGCReport, error) {
	report, err := db.db.CollectDictionaryGarbage(ctx)
	return DictionaryGCReport(report), err
}

// Restore extracts a fully verified backup into a new or empty directory,
// opens it, and performs a semantic integrity scan before returning. A failed
// validation leaves the destination in place for inspection and never installs
// a partially written MANIFEST.
func Restore(ctx context.Context, archivePath, destination string) (BackupReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	verified, err := backup.Restore(ctx, disk.DefaultFS, archivePath, destination)
	if err != nil {
		return BackupReport{}, err
	}
	if err := ctx.Err(); err != nil {
		return BackupReport{}, err
	}

	restored, err := oneleafdb.OpenDB(oneleafdb.DBOptions{Dir: destination})
	if err != nil {
		if errors.Is(err, ErrFormatTooNew) || errors.Is(err, ErrFormatTooOld) ||
			errors.Is(err, ErrUnknownRequiredFeature) || errors.Is(err, ErrMissingRequiredFeature) ||
			errors.Is(err, ErrFormatCorrupt) || errors.Is(err, ErrFormatCodec) ||
			errors.Is(err, ErrFormatMismatch) {
			return BackupReport{}, fmt.Errorf("fusedb: open restored database: %w", err)
		}
		return BackupReport{}, fmt.Errorf("%w: open restored database: %v", ErrCorruption, err)
	}
	report, verifyErr := restored.Verify(ctx)
	closeErr := restored.CloseWithoutCheckpoint()
	if verifyErr != nil {
		if errors.Is(verifyErr, context.Canceled) || errors.Is(verifyErr, context.DeadlineExceeded) {
			return BackupReport{}, verifyErr
		}
		return BackupReport{}, fmt.Errorf("%w: restored database verification: %v", ErrCorruption, verifyErr)
	}
	if closeErr != nil {
		return BackupReport{}, fmt.Errorf("fusedb: close restored database after verification: %w", closeErr)
	}

	result := BackupReport{
		AppliedSeq: report.AppliedSeq,
		Files:      uint64(len(verified.Entries)),
		Bytes:      verified.Bytes,
	}
	for _, entry := range verified.Entries {
		switch {
		case strings.HasPrefix(entry.Name, "segment-") && strings.HasSuffix(entry.Name, ".seg"):
			result.Segments++
		case strings.HasPrefix(entry.Name, "dictionaries/") && strings.HasSuffix(entry.Name, ".zdict"):
			result.Dictionaries++
		}
	}
	return result, nil
}

// VerifyReport summarizes persisted state covered by a successful scan.
type VerifyReport = oneleafdb.VerifyReport

// FormatInfo describes the persisted database compatibility contract.
type FormatInfo = oneleafdb.FormatInfo

// BackupReport describes a durable backup or successful restore.
type BackupReport = oneleafdb.BackupReport

// DictionaryGCReport describes one dictionary lifecycle cleanup pass.
type DictionaryGCReport = oneleafdb.DictionaryGCReport

// HealthStatus is a point-in-time readiness view. A terminal error fences the
// current handle until close and reopen.
type HealthStatus = oneleafdb.HealthStatus

// Health reports whether this database handle may accept foreground traffic.
// It is safe to call from a readiness endpoint and performs no filesystem I/O.
func (db *DB) Health() HealthStatus {
	if db == nil || db.db == nil {
		return oneleafdb.HealthStatus{CheckedAt: time.Now(), Closed: true}
	}
	return db.db.Health()
}

// Stats returns database statistics.
func (db *DB) Stats() Stats {
	load := db.db.SchedulerLoad()
	runtime := db.db.SchedulerRuntimeStats()
	return Stats{
		BufferedBytes:      db.db.BufferedBytes(),
		WALBytes:           db.db.WALBytes(),
		Leaves:             db.db.LeafCount(),
		PendingMergeLeaves: db.db.PendingMergeLeaves(),
		Scheduler:          db.db.SchedulerState().String(),
		SchedulerQueued:    runtime.Queued,
		SchedulerRunning:   runtime.Running,
		RequestRate:        load.RequestRate,
		ReadP95:            load.ReadP95,
		ReadP99:            load.ReadP99,
		WriteP95:           load.WriteP95,
		WriteP99:           load.WriteP99,
	}
}

// Metrics returns cumulative telemetry suitable for monitoring adapters.
// Histogram bucket counts are non-cumulative; Bounds contains matching upper
// bounds, Count includes overflow, and Sum is exact for sampled observations.
// Calling Metrics is intended for monitoring, not the request path.
func (db *DB) Metrics() MetricsSnapshot {
	snapshot := db.db.MetricsSnapshot()
	return MetricsSnapshot{
		At:                    snapshot.At,
		LatencySampleEvery:    snapshot.LatencySampleEvery,
		ReadOps:               snapshot.ReadOps,
		WriteOps:              snapshot.WriteOps,
		TerminalErrors:        snapshot.TerminalErrors,
		CommitUncertainErrors: snapshot.CommitUncertainErrors,
		ReadLatency:           publicHistogram(snapshot.ReadLatency),
		WriteLatency:          publicHistogram(snapshot.WriteLatency),
		Resources: ResourceMetrics{
			CPUUtilization:     snapshot.Resources.CPUUtilization,
			DiskUtilization:    snapshot.Resources.DiskUtilization,
			MemoryUtilization:  snapshot.Resources.MemoryUtilization,
			CPUCores:           snapshot.Resources.CPUCores,
			DiskBytesPerSecond: snapshot.Resources.DiskBytesPerSecond,
			MemoryBytes:        snapshot.Resources.MemoryBytes,
			DiskReadBytes:      snapshot.Resources.DiskReadBytes,
			DiskWriteBytes:     snapshot.Resources.DiskWriteBytes,
			Capacity: ResourceCapacity{
				CPUCores:           snapshot.Resources.Capacity.CPUCores,
				DiskBytesPerSecond: snapshot.Resources.Capacity.DiskBytesPerSecond,
				MemoryBytes:        snapshot.Resources.Capacity.MemoryBytes,
				CPUAutomatic:       snapshot.Resources.Capacity.CPUAutomatic,
				DiskAutomatic:      snapshot.Resources.Capacity.DiskAutomatic,
				MemoryAutomatic:    snapshot.Resources.Capacity.MemoryAutomatic,
			},
		},
		Background: []BackgroundMetrics{
			publicBackground("merge", enginemetrics.BackgroundMerge, snapshot),
			publicBackground("checkpoint", enginemetrics.BackgroundCheckpoint, snapshot),
			publicBackground("dictionary_train", enginemetrics.BackgroundDictionaryTrain, snapshot),
			publicBackground("dictionary_evaluate", enginemetrics.BackgroundDictionaryEvaluate, snapshot),
			publicBackground("dictionary_gc", enginemetrics.BackgroundDictionaryGC, snapshot),
			publicBackground("scheduler_model_persist", enginemetrics.BackgroundSchedulerModelPersist, snapshot),
			publicBackground("verify", enginemetrics.BackgroundVerify, snapshot),
			publicBackground("backup", enginemetrics.BackgroundBackup, snapshot),
		},
	}
}

// Stats contains database statistics.
type Stats struct {
	// BufferedBytes is the current size of the in-memory buffers.
	BufferedBytes int64
	// WALBytes is checkpoint debt accumulated since the last exact checkpoint.
	WALBytes int64

	// Leaves is the number of leaves partitioning the keyspace. It grows as
	// the database splits, and each leaf owns one segment.
	Leaves int
	// PendingMergeLeaves is the number of leaves currently requiring merge.
	PendingMergeLeaves int

	// Scheduler is the adaptive background scheduler state.
	Scheduler string
	// SchedulerQueued and SchedulerRunning expose background executor pressure
	// without exporting high-cardinality task identifiers.
	SchedulerQueued  int
	SchedulerRunning bool

	// RequestRate is the rolling foreground operation rate per second.
	RequestRate float64

	ReadP95, ReadP99   time.Duration
	WriteP95, WriteP99 time.Duration
}

// MetricsSnapshot is a cumulative monitoring snapshot.
type MetricsSnapshot struct {
	At                    time.Time
	LatencySampleEvery    uint64
	ReadOps               uint64
	WriteOps              uint64
	TerminalErrors        uint64
	CommitUncertainErrors uint64
	ReadLatency           LatencyHistogram
	WriteLatency          LatencyHistogram
	Background            []BackgroundMetrics
	Resources             ResourceMetrics
}

// ResourceMetrics contains absolute usage, cumulative IO, effective capacity,
// and normalized [0,1] utilization observations.
type ResourceMetrics struct {
	CPUUtilization     float64
	DiskUtilization    float64
	MemoryUtilization  float64
	CPUCores           float64
	DiskBytesPerSecond float64
	MemoryBytes        uint64
	DiskReadBytes      uint64
	DiskWriteBytes     uint64
	Capacity           ResourceCapacity
}

// ResourceCapacity is the effective hardware or configured scheduler budget.
type ResourceCapacity struct {
	CPUCores           float64
	DiskBytesPerSecond float64
	MemoryBytes        uint64
	CPUAutomatic       bool
	DiskAutomatic      bool
	MemoryAutomatic    bool
}

// LatencyHistogram contains matching upper bounds and non-cumulative counts.
type LatencyHistogram struct {
	Bounds []time.Duration
	Counts []uint64
	// Overflow counts samples above the largest finite bound. Count includes
	// both finite buckets and overflow.
	Overflow uint64
	Count    uint64
	Sum      time.Duration
}

// BackgroundMetrics contains cumulative lifecycle counters for one job kind.
type BackgroundMetrics struct {
	Kind        string
	Started     uint64
	Completed   uint64
	Failed      uint64
	Cancelled   uint64
	Active      int64
	TotalTime   time.Duration
	LastSuccess time.Time
}

func publicHistogram(snapshot enginemetrics.HistogramSnapshot) LatencyHistogram {
	counts := make([]uint64, len(snapshot.Buckets))
	copy(counts, snapshot.Buckets[:])
	return LatencyHistogram{
		Bounds:   enginemetrics.LatencyBounds(),
		Counts:   counts,
		Overflow: snapshot.Overflow,
		Count:    snapshot.Count,
		Sum:      time.Duration(snapshot.SumNanos),
	}
}

func publicBackground(kind string, index enginemetrics.BackgroundKind, snapshot enginemetrics.Snapshot) BackgroundMetrics {
	var lastSuccess time.Time
	if nanos := snapshot.BackgroundLastSuccessUnixNanos[index]; nanos != 0 {
		lastSuccess = time.Unix(0, nanos)
	}
	return BackgroundMetrics{
		Kind:        kind,
		Started:     snapshot.BackgroundStarted[index],
		Completed:   snapshot.BackgroundCompleted[index],
		Failed:      snapshot.BackgroundFailed[index],
		Cancelled:   snapshot.BackgroundCancelled[index],
		Active:      snapshot.ActiveBackground[index],
		TotalTime:   time.Duration(snapshot.BackgroundNanos[index]),
		LastSuccess: lastSuccess,
	}
}
