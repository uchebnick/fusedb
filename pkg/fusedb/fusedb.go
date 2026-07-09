// Package fusedb provides an embedded key-value database for hot mutable state.
//
// FuseDB is designed for workloads with frequent writes and point reads,
// using a lock-free skiplist buffer and immutable compressed segments.
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
//	defer db.Close()
//
//	// Put a value
//	err = db.Put([]byte("key"), []byte("value"))
//
//	// Get a value
//	value, found, err := db.Get([]byte("key"))
//
//	// Increment a counter
//	err = db.Inc([]byte("counter"), 1)
//
//	// Delete a key
//	err = db.Delete([]byte("key"))
package fusedb

import (
	"fmt"
	"time"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/pkg/oneleafdb"
)

// DB is an embedded key-value database.
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
	// Default: 5MB.
	MergeSize int64

	// WALPath is the write-ahead log file path.
	// If empty, uses Dir + "/wal.log".
	WALPath string

	// WALSyncWrites enables synchronous WAL writes.
	// Default: false (async group commit every 200µs).
	WALSyncWrites bool

	// WALGroupCommitInterval is the interval for async WAL flushes.
	// Default: 200µs.
	WALGroupCommitInterval time.Duration

	// DictionaryPath is the path to a compression dictionary file (.zdict).
	// If empty, compression is disabled.
	DictionaryPath string
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
		Dictionary:             dict,
		WALPath:                opts.WALPath,
		WALGroupCommitInterval: opts.WALGroupCommitInterval,
		WALSyncWrites:          opts.WALSyncWrites,
		CacheBytes:             opts.CacheSize,
	})
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

// Put stores a key-value pair.
func (db *DB) Put(key, value []byte) error {
	return db.db.Put(key, value)
}

// Get retrieves a value by key.
// Returns (value, true, nil) if found, (nil, false, nil) if not found.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	return db.db.Get(key)
}

// Delete removes a key.
func (db *DB) Delete(key []byte) error {
	return db.db.Delete(key)
}

// Inc increments a counter by delta.
// If the key doesn't exist, it's created with the delta value.
func (db *DB) Inc(key []byte, delta int64) error {
	return db.db.Inc(key, delta)
}

// Close closes the database and releases resources.
func (db *DB) Close() error {
	return db.db.Close()
}

// Stats returns database statistics.
func (db *DB) Stats() Stats {
	return Stats{
		BufferedBytes: db.db.BufferedBytes(),
	}
}

// Stats contains database statistics.
type Stats struct {
	// BufferedBytes is the current size of the in-memory buffer.
	BufferedBytes int64
}
