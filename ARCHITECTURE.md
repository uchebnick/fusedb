# FuseDB Architecture

This document provides detailed technical documentation of FuseDB's internal architecture, data structures, and algorithms.

## Table of Contents

- [Overview](#overview)
- [Segment Format](#segment-format)
- [Compression System](#compression-system)
- [Skiplist Design](#skiplist-design)
- [WAL Design](#wal-design)
- [Merge Process](#merge-process)
- [Cache Design](#cache-design)
- [Value Encoding](#value-encoding)
- [Read Path](#read-path)
- [Write Path](#write-path)

---

## Overview

FuseDB is built around the concept of **leaves** - independent units that own:
1. **In-memory mutation buffer** (lock-free skiplist)
2. **Immutable segment on disk** (compressed blocks with index and bloom filter)
3. **Write-ahead log** (WAL) for durability

The current implementation (`internal/oneleafdb`) is a single-leaf database used for research and benchmarking.

### Key Design Principles

- **Immutability**: Segments are never modified after creation
- **Lock-free reads**: Skiplist allows concurrent reads without locks
- **Local merge**: Each leaf merges independently (no global compaction)
- **Zero-copy**: Pooled buffers and borrowed slices minimize allocations
- **Dictionary compression**: Pre-trained LZ4 dictionaries for small blocks

---

## Segment Format

**File**: `internal/segment/segment_file.go`, `header.go`, `footer.go`, `index.go`

### On-Disk Layout

```
┌─────────────────────────────────────────────────────────────┐
│ Header (headerSize bytes)                                   │
├─────────────────────────────────────────────────────────────┤
│ Data Blocks (variable length, compressed or raw)            │
│   Block 0: [4-byte size][compressed payload]                │
│   Block 1: [4-byte size][compressed payload]                │
│   ...                                                        │
├─────────────────────────────────────────────────────────────┤
│ Bloom Filter (fixed size, calculated from key count)        │
├─────────────────────────────────────────────────────────────┤
│ Index (variable length)                                     │
│   Entry 0: [first key][last key][offset][length]            │
│   Entry 1: [first key][last key][offset][length]            │
│   ...                                                        │
├─────────────────────────────────────────────────────────────┤
│ Footer (footerSize bytes)                                   │
│   - BlockCount                                              │
│   - Data section: [offset, length]                          │
│   - Bloom section: [offset, length]                         │
│   - Index section: [offset, length]                         │
└─────────────────────────────────────────────────────────────┘
```

### Header Format

```go
type Header struct {
    Version        uint32  // Format version
    CompressionKind uint8  // 0=none, 1=LZ4Dict
    DictionaryID   uint32  // Dictionary ID for compression
}
```

Location: `segment_file.go:228-258`

### Footer Format

```go
type Footer struct {
    BlockCount uint32
    Data       Section  // [offset, length]
    Bloom      Section  // [offset, length]
    Index      Section  // [offset, length]
}
```

The footer enables backward reading: read last `footerSize` bytes to locate all sections.

### Block Structure

Each data block contains multiple key-value pairs:

```
Block: [Entry 0][Entry 1]...[Entry N]
Entry: [keyLen:varint][key:bytes][valueLen:varint][value:bytes]
```

Blocks are compressed independently with LZ4 dictionary compression. Each compressed block has a 4-byte header storing the original uncompressed size.

**Target block size**: 4KB (configurable via `targetBlockSize`)

Location: `block.go:1-200`

### Index Structure

The index maps key ranges to block offsets:

```go
type IndexEntry struct {
    FirstKey []byte  // First key in block
    LastKey  []byte  // Last key in block
    Offset   uint64  // Block offset in data section
    Length   uint32  // Block length in bytes
}
```

Binary search on `FirstKey` locates the block containing a target key.

Location: `index.go:1-150`

### Bloom Filter

Probabilistic filter to skip blocks that definitely don't contain a key. Uses `github.com/bits-and-blooms/bloom` with optimal parameters calculated from expected key count.

Location: `segment_file.go:180-220`

---

## Compression System

**File**: `internal/compression/dict.go`, `dict_file.go`, `dict_registry.go`

### Dictionary Training

Dictionaries are trained offline from representative samples:

```go
samples := [][]byte{...}  // Representative KV blocks
dict, err := BuildRawDict(samples, 4096)  // 4KB dictionary
```

Training uses `github.com/klauspost/compress/zstd/dict.BuildRawDict` to extract common patterns.

Location: `dict.go:150-186`

### Dictionary Persistence

Dictionaries are stored as `.zdict` files:

```
┌────────────────────────────────────────┐
│ Magic:    [4 bytes] "ZSTD"             │
│ Version:  [4 bytes] 1                  │
│ ID:       [4 bytes] unique identifier  │
│ Level:    [4 bytes] compression level  │
│ RawLen:   [4 bytes] dictionary size    │
│ CRC32:    [4 bytes] checksum           │
│ RawBytes: [RawLen bytes] dictionary    │
└────────────────────────────────────────┘
```

Location: `dict_file.go:52-67`

### Compression Pipeline

1. **Load dictionary**: `LZ4_createStream()` + `LZ4_loadDict(dict)`
2. **Compress block**: `LZ4_compress_fast_continue(src, dst, level)`
3. **Prepend size**: `[4-byte original size][compressed payload]`

Decompression:
1. **Read size header**: First 4 bytes = original size
2. **Allocate buffer**: Get from pool (max 5KB)
3. **Decompress**: `LZ4_decompress_safe_usingDict(src, dst, dict)`
4. **Return pooled buffer**: Caller must call `Release()`

Location: `dict.go:228-305`

### Dictionary Registry

LRU cache of loaded dictionaries with optional disk backing:

```go
type Registry struct {
    cache    map[uint32]*Dictionary  // ID -> Dictionary
    lru      []uint32                // LRU order
    maxCount int                     // Max cached dictionaries
    diskDir  string                  // Optional disk backing
}
```

When a dictionary is requested:
1. Check in-memory cache
2. If not found and `diskDir` set → load from disk
3. Evict LRU entry if cache full

Location: `dict.go:379-414`

---

## Skiplist Design

**File**: `internal/skiplist/skiplist.go`

### Lock-Free Algorithm

The skiplist uses atomic pointers for lock-free concurrent access:

```go
type node struct {
    key  []byte
    op   atomic.Pointer[ops.Op]
    next []atomic.Pointer[node]
}
```

### Height Selection

Deterministic hash-based height selection:

```go
func (s *SkipList) randomHeight(key []byte) int32 {
    h := int32(1)
    hash := xxhash.Sum64(key) ^ s.seed
    
    for h < maxHeight && (hash&3) == 0 {
        h++
        hash >>= 2
    }
    
    return h
}
```

- Max height: 20 levels
- Probability: 25% chance of each additional level
- Deterministic: same key always gets same height

Location: `skiplist.go:175-185`

### Insert Algorithm

1. **Find splice**: Locate predecessor nodes at all levels
2. **Create node**: Allocate node with determined height
3. **Publish level 0**: CAS to link into bottom level
4. **Publish upper levels**: CAS each level with retry on conflict
5. **Update height**: Atomically grow skiplist height if needed

```go
func (s *SkipList) Apply(key []byte, op ops.Op) {
    for {
        s.findSplice(key, &prevList, &nextList)
        
        if next := nextList[0]; next != nil && bytes.Equal(next.key, key) {
            s.updateNode(next, op)  // Key exists, update in-place
            return
        }
        
        newNode := newNode(key, op, nodeHeight)
        if !s.publishBaseLevel(newNode, &prevList, &nextList) {
            continue  // Retry on conflict
        }
        
        s.publishUpperLevels(key, newNode, nodeHeight, &prevList, &nextList)
        return
    }
}
```

Location: `skiplist.go:77-102`

### Update Algorithm

When a key already exists, update the Op pointer in-place without relinking:

```go
func (s *SkipList) updateNode(n *node, op ops.Op) {
    for {
        oldPtr := n.op.Load()
        merged := coalesceToNew(*oldPtr, op)
        
        if n.op.CompareAndSwap(oldPtr, &merged) {
            s.dataBytes.Add(int64(len(merged.Data) - len(oldPtr.Data)))
            return
        }
    }
}
```

Location: `skiplist.go:222-232`

### Operation Coalescing

Inc operations coalesce to reduce memory:

```go
func coalesceToNew(old, next ops.Op) ops.Op {
    switch next.Kind {
    case ops.OpPut, ops.OpDelete:
        return next  // Overwrites previous op
    case ops.OpInc:
        if old.Kind == ops.OpInc {
            sum := ops.DecodeInc(old) + ops.DecodeInc(next)
            return ops.NewInc(sum)  // Merge increments
        }
        return next
    default:
        return next
    }
}
```

Location: `skiplist.go:295-308`

---

## WAL Design

**File**: `internal/leaf/wal.go`

### Double Buffering

The WAL uses two buffers to allow concurrent writes and flushes:

```go
type WAL struct {
    active   *bytes.Buffer  // Current write buffer
    flushing *bytes.Buffer  // Buffer being flushed
    mu       sync.Mutex     // Protects buffer swap
    file     *os.File       // WAL file
}
```

### Record Format

```
┌────────────────────────────────────────────────┐
│ Kind:       [1 byte]  OpPut/OpDelete/OpInc    │
│ KeyLen:     [8 bytes] varint                   │
│ PayloadLen: [8 bytes] varint                   │
│ Key:        [KeyLen bytes]                     │
│ Payload:    [PayloadLen bytes]                 │
└────────────────────────────────────────────────┘
```

Location: `wal.go:117-123`

### Group Commit

Background goroutine flushes batches every 200µs:

```go
func (w *WAL) flushLoop() {
    ticker := time.NewTicker(200 * time.Microsecond)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            w.swapAndFlush()
        case <-w.stopCh:
            w.finalFlush()
            return
        }
    }
}
```

Location: `wal.go:136-152`

### Sync Modes

- **Async (default)**: Returns immediately after appending to active buffer
- **Sync**: Blocks until flush completes

```go
func (w *WAL) Append(kind ops.OpKind, key, payload []byte) error {
    w.mu.Lock()
    // Write to active buffer
    w.mu.Unlock()
    
    if w.syncMode {
        return w.flushAndWait()
    }
    return nil
}
```

Location: `wal.go:111-115`

### Durability Guarantee

Each flush performs:
1. `file.Write(buffer)` - write data
2. `file.Sync()` - fsync to disk

Location: `wal.go:170-174`

---

## Merge Process

**File**: `internal/oneleafdb/db.go`, `internal/leaf/leaf.go`

### Trigger Conditions

Background worker checks:
- `BufferedBytes >= ThresholdBytes` (default 5MB)
- Wakes on `notifyMergeWorker` channel

Location: `db.go:277-292`

### Merge Algorithm

1. **Freeze buffer**: Stop new writes, create immutable snapshot
2. **Create iterator**: Merge frozen skiplist + current segment
3. **Build new segment**: Write merged data to new file
4. **Atomic swap**: Replace old segment reader with new one
5. **Retire old segment**: Add to TTL queue (2s grace period)

```go
func (db *DB) runMergeWorker() {
    for range db.notifyMergeWorker {
        if db.leaf.BufferedBytes() < db.thresholdBytes {
            continue
        }
        
        // Freeze current buffer
        snapshot := db.leaf.Freeze()
        
        // Build new segment
        newSegment, err := db.merger.merge(snapshot, db.leaf.CurrentSegment())
        
        // Atomic swap
        db.leaf.ReplaceSegment(newSegment)
        
        // Retire old segment after grace period
        db.retireSegment(oldSegment, 2*time.Second)
    }
}
```

Location: `db.go:186-211`

### Skiplist Handling

Frozen buffer remains readable during merge:

```go
type Leaf struct {
    active *Buffer      // Current writes go here
    frozen *Buffer      // Immutable snapshot during merge
    segment *Reader     // Current segment
}
```

After merge completes, frozen buffer is discarded and a fresh active buffer is created.

Location: `leaf.go:183-200`

---

## Cache Design

**File**: `internal/oneleafdb/cache.go`

### Structure

Simple map-based cache with epoch invalidation:

```go
type Cache struct {
    entries  map[string]cacheEntry
    maxBytes int64
    curBytes int64
    epoch    atomic.Uint64
}

type cacheEntry struct {
    value []byte
    epoch uint64
}
```

### Key Format

Uses `unsafe.String` pointer conversion for zero-copy lookups:

```go
func (c *Cache) Get(key []byte) ([]byte, bool) {
    keyStr := unsafe.String(unsafe.SliceData(key), len(key))
    entry, ok := c.entries[keyStr]
    // ...
}
```

Location: `cache.go:37`

### Eviction Policy

Random map iteration when size exceeds `maxBytes`:

```go
func (c *Cache) evictOne() {
    for k := range c.entries {
        delete(c.entries, k)
        return  // Evict first entry found
    }
}
```

Location: `cache.go:63-69`

### Epoch-Based Invalidation

Each write operation increments the global epoch. Cache entries store their creation epoch. On read, if entry epoch doesn't match current epoch, it's a cache miss.

```go
func (c *Cache) Set(key, value []byte) {
    currentEpoch := c.epoch.Load()
    c.entries[string(key)] = cacheEntry{
        value: value,
        epoch: currentEpoch,
    }
}

func (c *Cache) Get(key []byte) ([]byte, bool) {
    entry, ok := c.entries[keyStr]
    if !ok || entry.epoch != c.epoch.Load() {
        return nil, false  // Cache miss
    }
    return entry.value, true
}
```

Location: `cache.go:31-40, 124-127, 146-150`

---

## Value Encoding

**File**: `internal/value/value.go`

### Type Tag at End

Type tag is stored at the **end** of encoded data to enable prefix scanning without decoding values:

```go
type Kind byte

const (
    KindBytes Kind = 1
    KindInt64 Kind = 2
)
```

### Encoding Formats

**Bytes:**
```
[raw bytes][0x01]
```

```go
func EncodeBytes(data []byte) []byte {
    data = append(data, byte(KindBytes))
    return data
}
```

**Int64:**
```
[varint][0x02]
```

```go
func EncodeInt64(v int64) []byte {
    var buf [binary.MaxVarintLen64 + 1]byte
    n := binary.PutVarint(buf[:], v)
    buf[n] = byte(KindInt64)
    return buf[:n+1]
}
```

Location: `value.go:40-64`

### Decoding

Read last byte first to determine kind, then extract payload:

```go
func KindOf(data []byte) (Kind, error) {
    if len(data) == 0 {
        return 0, ErrEmptyValue
    }
    return Kind(data[len(data)-1]), nil
}

func DecodeBytes(data []byte) ([]byte, error) {
    kind, err := KindOf(data)
    if err != nil || kind != KindBytes {
        return nil, err
    }
    return data[:len(data)-1], nil  // Strip type tag
}
```

Location: `value.go:25-56`

### Design Rationale

End-tag design allows key prefix iteration to skip value parsing until needed, reducing CPU in range scans. The decoder can check the last byte without reading the entire value.

---

## Read Path

### Full Read Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Check skiplist buffer                                    │
│    - If found: return immediately                           │
│    - If Delete tombstone: return not found                  │
│    - If Inc: resolve against base value from segment        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Check cache                                              │
│    - If found and epoch matches: return cached value        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Check segment                                            │
│    a. Bloom filter: probably contains key?                  │
│    b. Index: binary search for block containing key         │
│    c. Read block: readFullAt() with pooled buffer           │
│    d. Decompress: LZ4 decompress with dictionary            │
│    e. Scan block: linear search for exact key               │
│    f. Cache result: store decompressed value                │
└─────────────────────────────────────────────────────────────┘
```

### Code Path

```go
func (db *DB) Get(key []byte) ([]byte, bool, error) {
    // 1. Check skiplist
    if op, ok := db.leaf.buffer.Read(key); ok {
        if op.Kind == ops.OpDelete {
            return nil, false, nil
        }
        if op.Kind == ops.OpPut {
            return op.Data, true, nil
        }
        // OpInc: need base value from segment
    }
    
    // 2. Check cache
    if val, ok := db.cache.Get(key); ok {
        return val, true, nil
    }
    
    // 3. Read from segment
    val, ok, err := db.leaf.segment.Get(key)
    if ok {
        db.cache.Set(key, val)
    }
    return val, ok, err
}
```

Location: `db.go:120-150`

---

## Write Path

### Full Write Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Append to WAL                                            │
│    - Serialize operation                                    │
│    - Append to active buffer                                │
│    - If sync mode: wait for flush                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Apply to skiplist                                        │
│    - If key exists: coalesce operations                     │
│    - If new key: insert new node                            │
│    - Update DataBytes counter                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Invalidate cache                                         │
│    - Increment global epoch                                 │
│    - All cached entries now stale                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. Check merge threshold                                    │
│    - If BufferedBytes >= ThresholdBytes:                    │
│      notify merge worker                                    │
└─────────────────────────────────────────────────────────────┘
```

### Code Path

```go
func (db *DB) Put(key, value []byte) error {
    // 1. WAL append
    if err := db.wal.Append(ops.OpPut, key, value); err != nil {
        return err
    }
    
    // 2. Apply to skiplist
    db.leaf.buffer.Apply(key, ops.Op{
        Kind: ops.OpPut,
        Data: value,
    })
    
    // 3. Invalidate cache
    db.cache.Invalidate()
    
    // 4. Check merge threshold
    if db.leaf.BufferedBytes() >= db.thresholdBytes {
        select {
        case db.notifyMergeWorker <- struct{}{}:
        default:
        }
    }
    
    return nil
}
```

Location: `db.go:80-110`

---

## Performance Optimizations

### Zero-Copy Buffer Management

Pooled buffers with explicit `Release()` calls:

```go
type PooledBuffer struct {
    Data   []byte
    bufPtr *[]byte
}

func (pb *PooledBuffer) Release() {
    if pb != nil && pb.bufPtr != nil {
        blockBufPool.Put(pb.bufPtr)
        pb.bufPtr = nil
    }
}
```

Caller pattern:
```go
pb, err := readFullAt(offset, length)
if err != nil {
    return err
}
defer pb.Release()  // Or explicit call after use
```

Location: `segment_file.go:50-80`

### Lock-Free Decompression

Uses `atomic.Bool` instead of mutex for closed flag:

```go
type Dictionary struct {
    raw    []byte
    closed atomic.Bool
}

func (d *Dictionary) Decompress(src []byte) (*PooledDecompressBuffer, error) {
    if d.closed.Load() {
        return nil, ErrDictionaryClosed
    }
    // Decompress without holding lock
}
```

Location: `dict.go:51-71, 262-305`

### Immutable Keys

Skiplist stores keys directly without cloning (keys are immutable):

```go
func newNode(key []byte, op ops.Op, height int32) *node {
    return &node{
        key:  key,  // No bytes.Clone - caller guarantees immutability
        next: make([]atomic.Pointer[node], height),
    }
}
```

Location: `skiplist.go:165-172`

---

## Future Work

### Multi-Leaf Routing

Planned routing tree to distribute keys across multiple leaves:

```
         Root
        /    \
    Leaf A  Leaf B
    [a-m]   [n-z]
```

Each leaf operates independently with its own buffer and segment.

### Range Scans

Expose iterator API at DB level:

```go
func (db *DB) Scan(start, end []byte) Iterator
```

Requires merging iterators from skiplist buffer and segment.

### Scheduler Policies

Prioritize merge work based on:
- Buffer size
- Read hotness
- Write rate
- Segment age

---

## References

- **Skiplist paper**: Pugh, William (1990). "Skip lists: a probabilistic alternative to balanced trees"
- **LZ4 compression**: https://github.com/lz4/lz4
- **Bloom filters**: https://github.com/bits-and-blooms/bloom
- **Pebble (comparison baseline)**: https://github.com/cockroachdb/pebble

---

**Last updated**: 2026-05-11
