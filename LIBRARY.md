# Using FuseDB as a Library

FuseDB can be used as an embedded key-value database in your Go applications.

## Installation

```bash
go get github.com/uchebnick/fusedb/pkg/fusedb
```

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
    Dir:       "./data",           // Required: database directory
    CacheSize: 5 << 20,            // Optional: value cache size (default: 5MB)
    MergeSize: 5 << 20,            // Optional: merge threshold (default: 5MB)
    WALPath:   "./data/wal.log",   // Optional: WAL file path
    WALSyncWrites: false,          // Optional: sync writes (default: false)
    DictionaryPath: "dict.zdict",  // Optional: compression dictionary
})
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
log.Printf("Buffered: %d bytes", stats.BufferedBytes)
```

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

Controls when the in-memory buffer is merged to disk:

```go
fusedb.Options{
    MergeSize: 10 << 20, // Merge at 10MB
}
```

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

- **Writes**: ~1µs (async WAL), ~300ns (in-memory only)
- **Reads**: ~1µs (cached), ~1.3µs (from segment)
- **Counters**: ~250ns (in-place increment)
- **Startup**: ~200ms (64K keys)

See [benchmarks/RESULTS.md](benchmarks/RESULTS.md) for detailed benchmarks.

## Thread Safety

All operations are thread-safe and can be called concurrently from multiple goroutines.

## Limitations

- Single-node only (not distributed)
- No range scans yet
- No transactions
- Experimental status (not production-ready)

## Examples

See [example_test.go](example_test.go) for more examples.
