# Using FuseDB as a Library

FuseDB can be used as an embedded key-value database in your Go applications.

## Installation

```bash
go get github.com/uchebnick/fusedb
```

`pkg/fusedb` is the public API. `pkg/oneleafdb` is the engine behind it and is
not a supported entry point.

## Quick Start

```go
package main

import (
    "log"
    
    "github.com/uchebnick/fusedb/pkg/fusedb"
)

func main() {
    // Open database
    db, err := fusedb.Open(fusedb.Options{
        Dir:       "./data",
        CacheSize: 5 << 20, // 5MB
        MergeSize: 5 << 20, // 5MB
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Put a value
    err = db.Put([]byte("key"), []byte("value"))
    if err != nil {
        log.Fatal(err)
    }
    
    // Get a value
    value, found, err := db.Get([]byte("key"))
    if err != nil {
        log.Fatal(err)
    }
    if found {
        log.Printf("Value: %s", value)
    }
}
```

## API Reference

### Opening a Database

```go
db, err := fusedb.Open(fusedb.Options{
    Dir:         "./data",          // Required: database directory
    CacheSize:   5 << 20,           // Optional: value cache size (default: 5MB)
    MergeSize:   8 << 20,           // Optional: merge threshold (default: 8MB)
    MaxLeafSize: 64 << 20,          // Optional: leaf split size (default: 64MB)
    WALPath:     "./data/wal.log",  // Optional: log path (default: <Dir>/wal.log)
    WALSyncWrites:  false,          // Optional: sync writes (default: false)
    DictionaryPath: "dict.zdict",   // Optional: compression dictionary
})

Reopening the same directory restores the database: the leaf catalog is read
from the manifest and the tail of the log that no segment covers yet is
replayed.
```

### Basic Operations

**Put** - Store a key-value pair:
```go
err := db.Put([]byte("user:1"), []byte("Alice"))
```

**Get** - Retrieve a value:
```go
value, found, err := db.Get([]byte("user:1"))
if found {
    // value contains the data
}
```

**Delete** - Remove a key:
```go
err := db.Delete([]byte("user:1"))
```

**Inc** - Increment a counter:
```go
// Increment by 1
err := db.Inc([]byte("counter"), 1)

// Decrement by 1
err := db.Inc([]byte("counter"), -1)
```

### Statistics

```go
stats := db.Stats()
log.Printf("Buffered: %d bytes across %d leaves", stats.BufferedBytes, stats.Leaves)
```

`Leaves` is how many ranges the keyspace is split into. It grows as the database
grows; each leaf owns one segment file and merges independently.

### Closing

```go
err := db.Close()
```

## Configuration Options

### Cache Size

Controls the in-memory value cache size:

```go
fusedb.Options{
    CacheSize: 10 << 20, // 10MB cache
}
```

### Merge Threshold

Controls when a leaf's in-memory buffer is merged into its segment:

```go
fusedb.Options{
    MergeSize: 10 << 20, // Merge at 10MB
}
```

### Leaf Size

Controls when a leaf splits in two:

```go
fusedb.Options{
    MaxLeafSize: 64 << 20, // Split at 64MB
}
```

This is the main knob for write amplification. A merge rewrites one leaf, so the
bytes rewritten per merge are bounded by `MaxLeafSize` rather than by the size of
the whole database. A smaller leaf makes each merge cheaper but leaves more
leaves to track.

### Write-Ahead Log

Configure WAL behavior:

```go
fusedb.Options{
    WALPath: "./wal.log",
    WALSyncWrites: false,              // Async (default)
    WALGroupCommitInterval: 200 * time.Microsecond,
}
```

- `WALSyncWrites: false` - Async group commit (faster, less durable)
- `WALSyncWrites: true` - Sync every write (slower, more durable)

### Compression

Enable LZ4 dictionary compression:

```go
fusedb.Options{
    DictionaryPath: "./dict.zdict",
}
```

## Use Cases

### Session Store

```go
// Store session
sessionID := []byte("session:abc123")
sessionData := []byte(`{"user_id": 42, "expires": 1234567890}`)
db.Put(sessionID, sessionData)

// Retrieve session
data, found, _ := db.Get(sessionID)
if found {
    // Parse session data
}

// Delete expired session
db.Delete(sessionID)
```

### Rate Limiter

```go
// Increment request count
key := []byte("rate:user:123")
db.Inc(key, 1)

// Check current count
value, found, _ := db.Get(key)
if found {
    count := decodeInt64(value)
    if count > 100 {
        // Rate limit exceeded
    }
}
```

### Feature Flags

```go
// Set feature flag
db.Put([]byte("feature:new_ui"), []byte("enabled"))

// Check feature flag
value, found, _ := db.Get([]byte("feature:new_ui"))
enabled := found && string(value) == "enabled"
```

### Counters

```go
// Page views
db.Inc([]byte("page:home:views"), 1)

// Likes
db.Inc([]byte("post:123:likes"), 1)

// Inventory
db.Inc([]byte("product:456:stock"), -1)
```

## Performance Characteristics

See [benchmarks/RESULTS.md](benchmarks/RESULTS.md) for measured numbers and the
commands that produce them.

A point read touches one leaf: a binary search in memory, one bloom filter, one
block index, one block. Adding leaves does not add work to a read.

## Thread Safety

All operations are thread-safe and can be called concurrently from multiple goroutines.

## Durability

Writes go to the write-ahead log before they are applied. With
`WALSyncWrites: false` (the default) the log is flushed by group commit every
200µs, so a crash can lose at most that window. With `WALSyncWrites: true` each
write waits for its record to reach disk.

A crash is recovered on the next open: the manifest names the segments, and log
records past the recorded watermark are replayed. A partially written record at
the end of the log is discarded, which is the expected state after a crash.

## Limitations

- Single-node only (not distributed)
- No range scans yet
- No transactions or snapshots
- Experimental status (not production-ready)

## Examples

See [pkg/fusedb/example_test.go](pkg/fusedb/example_test.go) for more examples,
and [pkg/fusedb/durability_test.go](pkg/fusedb/durability_test.go) for the
restart and crash behaviour that is actually tested.
