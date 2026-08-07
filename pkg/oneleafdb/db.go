package oneleafdb

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/tree"
	"github.com/uchebnick/fusedb/internal/wal"
)

// Default configuration constants for oneleafdb.
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
)

// DB is an embedded key-value store backed by a tree of leaves.
//
// Writes land in the buffer of the leaf that owns the key, and a background
// worker merges leaves whose buffers cross the threshold. Because merges are
// per leaf, the volume rewritten by a merge is bounded by the leaf size rather
// than by the size of the database.
type DB struct {
	dir string
	fs  disk.FS

	tree       *tree.Tree
	wal        *wal.WAL
	cache      *valueCache
	cacheEpoch [cacheEpochShards]atomic.Uint64
	registry   *compression.Registry

	thresholdBytes  int64
	checkpointBytes int64

	// applyMu orders log writes against buffer freezing. Writers hold the read
	// side across "append to log, apply to tree", and a checkpoint takes the
	// write side just long enough to read the last sequence number and freeze
	// every buffer. That makes the recorded watermark exact: every operation at
	// or below it is in the frozen set, and nothing above it is. An inexact
	// watermark would replay an increment that a segment already contains and
	// silently double it.
	applyMu sync.RWMutex

	walBytes atomic.Int64

	mergeNotify chan struct{}
	mergeStop   chan struct{}
	mergeDone   chan struct{}
	closeOnce   sync.Once
	closed      atomic.Bool

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
	FS                     disk.FS
	ThresholdBytes         int64
	MaxLeafBytes           int64
	Seed                   uint64
	SegmentID              uint64
	Dictionary             *compression.Dictionary
	WALPath                string
	WALGroupCommitInterval time.Duration
	WALSyncWrites          bool
	WALCheckpointBytes     int64

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
		return nil, fmt.Errorf("oneleafdb: Dir is required")
	}

	fs := opts.FS
	if fs == nil {
		fs = disk.DefaultFS
	}
	if err := fs.MkdirAll(opts.Dir); err != nil {
		return nil, fmt.Errorf("oneleafdb: create dir: %w", err)
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
	var registry *compression.Registry
	if opts.Dictionary != nil {
		compressionKind = segment.CompressionLZ4Dict
		registry = compression.NewRegistry()
		if err := registry.Add(opts.Dictionary); err != nil {
			return nil, fmt.Errorf("oneleafdb: add dictionary: %w", err)
		}
	}

	treeOpts := tree.Options{
		FS:                  fs,
		Dir:                 opts.Dir,
		Seed:                seed,
		MaxLeafBytes:        maxLeaf,
		MergeThresholdBytes: threshold,
		TargetBlockSize:     segment.DefaultTargetBlockSize,
		BloomFalsePositive:  segment.DefaultBloomFilterFalseRate,
		Compression:         compressionKind,
		Dictionary:          opts.Dictionary,
		Registry:            registry,
	}

	manifestPath := manifest.FileName(opts.Dir)
	loaded, err := manifest.Load(fs, manifestPath)
	switch {
	case err == nil:
	case manifest.IsNotExist(err):
		loaded = nil
	default:
		closeRegistry(registry)
		return nil, fmt.Errorf("oneleafdb: load manifest: %w", err)
	}

	var leafTree *tree.Tree
	if loaded == nil {
		leafTree, err = tree.New(treeOpts)
	} else {
		leafTree, err = tree.Open(treeOpts, loaded)
	}
	if err != nil {
		closeRegistry(registry)
		return nil, fmt.Errorf("oneleafdb: open tree: %w", err)
	}

	db := &DB{
		dir:             opts.Dir,
		fs:              fs,
		tree:            leafTree,
		cache:           newValueCache(cacheBytes),
		registry:        registry,
		thresholdBytes:  threshold,
		checkpointBytes: checkpoint,
		mergeNotify:     make(chan struct{}, 1),
		mergeStop:       make(chan struct{}),
		mergeDone:       make(chan struct{}),
	}

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
			return nil, fmt.Errorf("oneleafdb: open wal: %w", err)
		}
		db.wal = log

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

	go db.mergeWorker()
	return db, nil
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
		switch record.Kind {
		case ops.OpPut:
			return db.tree.Put(record.Key, record.Payload)
		case ops.OpDelete:
			return db.tree.Delete(record.Key)
		case ops.OpInc:
			return db.tree.Inc(record.Key, ops.DecodeInc(record.Op()))
		default:
			return fmt.Errorf("oneleafdb: unknown wal record kind %d", record.Kind)
		}
	})
	if err != nil {
		return fmt.Errorf("oneleafdb: replay wal: %w", err)
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
	if err := db.backgroundErr(); err != nil {
		return err
	}

	db.applyMu.RLock()
	err := db.appendPut(key, value)
	if err != nil {
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
	db.notifyMergeWorker()
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	if err := db.backgroundErr(); err != nil {
		return nil, false, err
	}

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
	if err := db.backgroundErr(); err != nil {
		return err
	}

	db.applyMu.RLock()
	err := db.appendDelete(key)
	if err != nil {
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
	db.notifyMergeWorker()
	return nil
}

func (db *DB) Inc(key []byte, delta int64) error {
	if err := db.backgroundErr(); err != nil {
		return err
	}

	db.applyMu.RLock()
	err := db.appendInc(key, delta)
	if err != nil {
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
	db.notifyMergeWorker()
	return nil
}

// invalidate drops the cached value for key and bumps only its shard counter.
func (db *DB) invalidate(key []byte) {
	db.cacheEpoch[cacheShard(key)].Add(1)
	db.cache.delete(key)
}

func (db *DB) noteWALGrowth(n int) {
	db.walBytes.Add(int64(n))
}

// Merge merges every leaf and truncates the log up to the merged watermark.
func (db *DB) Merge() error {
	return db.checkpoint()
}

// checkpoint freezes all buffers at an exact log position, merges them, then
// records the watermark and truncates the log.
func (db *DB) checkpoint() error {
	db.applyMu.Lock()
	watermark := db.walLastSeq()
	db.tree.FreezeAll()
	db.applyMu.Unlock()

	if err := db.tree.MergeAll(); err != nil {
		return fmt.Errorf("oneleafdb: merge: %w", err)
	}

	db.tree.SetAppliedSeq(watermark)
	if err := db.tree.SaveManifest(); err != nil {
		return fmt.Errorf("oneleafdb: save manifest: %w", err)
	}
	if err := db.walTruncate(watermark); err != nil {
		return fmt.Errorf("oneleafdb: truncate wal: %w", err)
	}
	db.walBytes.Store(0)
	return nil
}

// mergeLeaves merges only the leaves that crossed the threshold.
//
// This is the common path and deliberately does not move the log watermark:
// only some leaves are merged, so records for the others are still needed.
func (db *DB) mergeLeaves() error {
	pending := db.tree.PendingMerge()
	for _, l := range pending {
		if err := db.tree.MergeLeaf(l); err != nil {
			return fmt.Errorf("oneleafdb: merge leaf %d: %w", l.ID(), err)
		}
	}
	if len(pending) > 0 {
		if err := db.tree.SaveManifest(); err != nil {
			return fmt.Errorf("oneleafdb: save manifest: %w", err)
		}
	}
	return nil
}

func (db *DB) Close() error {
	if db == nil || db.tree == nil {
		return nil
	}

	var err error
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		close(db.mergeStop)
		<-db.mergeDone

		if checkpointErr := db.checkpoint(); checkpointErr != nil {
			err = checkpointErr
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

func (db *DB) notifyMergeWorker() {
	if db == nil || db.closed.Load() {
		return
	}
	select {
	case db.mergeNotify <- struct{}{}:
	default:
	}
}

// backgroundErr returns the first failure the merge worker hit, if any.
func (db *DB) backgroundErr() error {
	if db == nil {
		return nil
	}
	if err := db.mergeErr.Load(); err != nil && *err != nil {
		return *err
	}
	return nil
}

func (db *DB) recordBackgroundErr(err error) {
	if err == nil {
		return
	}
	db.mergeErr.CompareAndSwap(nil, &err)
}

func (db *DB) mergeWorker() {
	defer close(db.mergeDone)

	for {
		select {
		case <-db.mergeStop:
			return
		case <-db.mergeNotify:
			if err := db.runMergeCycle(); err != nil {
				// Surfacing the failure is what keeps a broken merge from
				// looking like a healthy database that quietly stops merging.
				db.recordBackgroundErr(err)
				return
			}
		}
	}
}

func (db *DB) runMergeCycle() error {
	if db.walBytes.Load() >= db.checkpointBytes {
		return db.checkpoint()
	}
	if db.tree.BufferedBytes() < db.thresholdBytes {
		return nil
	}
	return db.mergeLeaves()
}
