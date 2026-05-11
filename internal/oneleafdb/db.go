package oneleafdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/leaf"
	"github.com/uchebnick/fusedb/internal/segment"
)

const (
	DefaultMergeThresholdBytes    = 5 << 20
	DefaultCacheBytes             = 5 << 20
	DefaultWALGroupCommitInterval = 200 * time.Microsecond
)

// DB is a tiny one-leaf database used for local experiments and benchmarks.
//
// It has no global scheduler. Writes go into the leaf buffer, and a local
// worker performs threshold-triggered merges outside the caller's Put path.
type DB struct {
	dir            string
	thresholdBytes int64
	segmentID      uint64
	version        uint64
	compression    segment.CompressionKind
	dictionary     *compression.Dictionary
	registry       *compression.Registry
	wal            *wal
	cache          *valueCache
	cacheEpoch     atomic.Uint64

	leaf    *leaf.Leaf
	mergeMu sync.Mutex

	mergeNotify chan struct{}
	mergeStop   chan struct{}
	mergeDone   chan struct{}
	closeOnce   sync.Once
	closed      atomic.Bool
}

type DBOptions struct {
	Dir                    string
	ThresholdBytes         int64
	Seed                   uint64
	SegmentID              uint64
	Dictionary             *compression.Dictionary
	WALPath                string
	WALGroupCommitInterval time.Duration
	WALSyncWrites          bool
	CacheBytes             int64
	CacheEntries           int
}

func OpenDB(opts DBOptions) (*DB, error) {
	threshold := opts.ThresholdBytes
	if threshold == 0 {
		threshold = DefaultMergeThresholdBytes
	}

	segmentID := opts.SegmentID
	if segmentID == 0 {
		segmentID = 1
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

	db := &DB{
		dir:            opts.Dir,
		thresholdBytes: threshold,
		segmentID:      segmentID,
		compression:    compressionKind,
		dictionary:     opts.Dictionary,
		registry:       registry,
		cache:          newValueCache(cacheBytes),
		leaf:           leaf.NewLeaf(segmentID, seed, nil, &leaf.Merger{Compression: registry}),
		mergeNotify:    make(chan struct{}, 1),
		mergeStop:      make(chan struct{}),
		mergeDone:      make(chan struct{}),
	}
	wal, err := openWALWithSync(opts.WALPath, opts.WALGroupCommitInterval, opts.WALSyncWrites)
	if err != nil {
		if registry != nil {
			_ = registry.Close()
		}
		return nil, fmt.Errorf("oneleafdb: open wal: %w", err)
	}
	db.wal = wal

	go db.mergeWorker()
	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	if err := db.wal.appendPut(key, value); err != nil {
		return err
	}
	db.cacheEpoch.Add(1)
	db.cache.delete(key)
	db.leaf.Put(key, value)
	db.cacheEpoch.Add(1)
	db.cache.delete(key)
	db.notifyMergeWorker()
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	epoch := db.cacheEpoch.Load()
	if value, ok := db.cache.get(key, epoch); ok {
		return value, true, nil
	}
	value, ok, err := db.leaf.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		db.cache.delete(key)
		return nil, false, nil
	}
	if db.cacheEpoch.Load() == epoch {
		// Copy value before caching to prevent sharing with leaf's internal data
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		db.cache.set(key, valueCopy, epoch)
	}
	return value, true, nil
}

func (db *DB) Delete(key []byte) error {
	if err := db.wal.appendDelete(key); err != nil {
		return err
	}
	db.cacheEpoch.Add(1)
	db.cache.delete(key)
	db.leaf.Delete(key)
	db.cacheEpoch.Add(1)
	db.cache.delete(key)
	db.notifyMergeWorker()
	return nil
}

func (db *DB) Inc(key []byte, delta int64) error {
	if err := db.wal.appendInc(key, delta); err != nil {
		return err
	}
	db.cacheEpoch.Add(1)
	db.cache.delete(key)
	db.leaf.Inc(key, delta)
	db.cacheEpoch.Add(1)
	db.cache.delete(key)
	db.notifyMergeWorker()
	return nil
}

func (db *DB) Merge() error {
	return db.merge(true)
}

func (db *DB) merge(force bool) error {
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	if db.leaf.BufferedLen() == 0 {
		return nil
	}
	if !force && db.leaf.BufferedBytes() < db.thresholdBytes {
		return nil
	}

	nextVersion := db.version + 1
	err := db.leaf.Merge(segment.Options{
		FS:                    disk.DefaultFS,
		Dir:                   db.dir,
		SegmentID:             db.segmentID,
		Version:               nextVersion,
		ExpectedKeys:          int(db.leaf.BufferedLen()),
		TargetBlockSize:       segment.DefaultTargetBlockSize,
		BloomFalsePositive:    segment.DefaultBloomFilterFalseRate,
		Compression:           db.compression,
		CompressionDictionary: db.dictionary,
	})
	if err != nil {
		return fmt.Errorf("oneleafdb: merge: %w", err)
	}
	db.version = nextVersion
	return nil
}

func (db *DB) Close() error {
	if db == nil || db.leaf == nil {
		return nil
	}

	var err error
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		close(db.mergeStop)
		<-db.mergeDone
		if mergeErr := db.Merge(); mergeErr != nil {
			err = mergeErr
		}
		if closeErr := db.leaf.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
		if closeErr := db.wal.close(); err == nil && closeErr != nil {
			err = closeErr
		}
		if db.registry != nil {
			if closeErr := db.registry.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
	})
	return err
}

func (db *DB) BufferedBytes() int64 {
	if db == nil || db.leaf == nil {
		return 0
	}
	return db.leaf.BufferedBytes()
}

func (db *DB) SegmentPath() string {
	if db == nil {
		return ""
	}
	return segment.SegmentFileName(db.dir, db.segmentID, db.version)
}

func (db *DB) InstallReaderForBench(reader *segment.Reader, version uint64) {
	if db == nil {
		return
	}
	db.leaf = leaf.NewLeaf(db.segmentID, 42, reader, &leaf.Merger{Compression: db.registry})
	db.version = version
	db.cacheEpoch.Add(1)
	db.cache.clear()
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

func (db *DB) mergeWorker() {
	defer close(db.mergeDone)

	for {
		select {
		case <-db.mergeStop:
			return
		case <-db.mergeNotify:
			for db.leaf.BufferedBytes() >= db.thresholdBytes {
				if err := db.merge(false); err != nil {
					return
				}
			}
		}
	}
}
