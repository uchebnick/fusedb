# FuseDB Architecture

Technical documentation of FuseDB internals for contributors and researchers.

## Table of Contents

- [System Overview](#system-overview)
- [Segment Format](#segment-format)
- [Compression System](#compression-system)
- [Skiplist Implementation](#skiplist-implementation)
- [Write-Ahead Log](#write-ahead-log)
- [Merge Process](#merge-process)
- [Cache Layer](#cache-layer)
- [Value Encoding](#value-encoding)
- [Data Flow](#data-flow)

---

## System Overview

FuseDB organizes data into leaves. Each leaf is an independent unit containing an in-memory mutation buffer and an immutable on-disk segment. The current implementation (`internal/oneleafdb`) operates as a single-leaf database.

### Core Components

**Skiplist Buffer** (`internal/skiplist`)  
Lock-free ordered index storing recent mutations. Supports Put, Delete, and Inc operations with in-place coalescing for counters.

**Segment** (`internal/segment`)  
Immutable on-disk structure containing compressed data blocks, bloom filter, index, and metadata. Written once during merge, never modified.

**Write-Ahead Log** (`internal/leaf/wal.go`)  
Durability layer using double-buffered append with async group commit. Flushes batches every 200µs.

**Compression** (`internal/compression`)  
LZ4 dictionary compression with persistent registry. Dictionaries trained offline from representative samples, stored as `.zdict` files.

**Cache** (`internal/oneleafdb/cache.go`)  
Simple map-based value cache with epoch-based invalidation. Evicts randomly when size exceeds limit.

### Design Principles

- Immutability: segments never modified after creation
- Lock-free reads: skiplist allows concurrent access without locks
- Local merge: each leaf merges independently
- Zero-copy: pooled buffers minimize allocations
- Dictionary compression: pre-trained LZ4 dictionaries for small blocks

---

## Segment Format

File: `internal/segment/segment_file.go`, `header.go`, `footer.go`, `index.go`

### Layout

```
[Header][Data Blocks][Bloom Filter][Index][Footer]
```

**Header** (fixed size)  
Version, compression kind, dictionary ID.

**Data Blocks** (variable length)  
Sequence of compressed blocks. Each block: `[4-byte size][LZ4 payload]`. Size field stores original uncompressed length.

**Bloom Filter** (fixed size)  
Probabilistic filter calculated from key count. Uses `github.com/bits-and-blooms/bloom`.

**Index** (variable length)  
Array of entries mapping key ranges to block offsets:
```go
type IndexEntry struct {
    FirstKey []byte
    LastKey  []byte
    Offset   uint64
    Length   uint32
}
```

**Footer** (fixed size)  
Block count and section pointers (offset, length) for Data, Bloom, and Index.

### Block Structure

Each block contains multiple key-value pairs:
```
[keyLen:varint][key][valueLen:varint][value]...
```

Target block size: 4KB. Blocks compressed independently with LZ4 dictionary.

### Read Algorithm

1. Read footer to locate sections
2. Check bloom filter for key presence
3. Binary search index for block containing key
4. Read and decompress block
5. Linear scan block for exact key match

Location: `segment_file.go:228-258`, `reader.go:80-150`

---

## Compression System

File: `internal/compression/dict.go`, `dict_file.go`

### Dictionary Training

Dictionaries trained offline using `github.com/klauspost/compress/zstd/dict.BuildRawDict`:

```go
samples := [][]byte{...}  // Representative blocks
dict, err := BuildRawDict(samples, 4096)  // 4KB dictionary
```

Training extracts common patterns from sample data. Typical compression ratio: 62.6% space savings.

Location: `dict.go:150-186`

### Persistence Format

Dictionaries stored as `.zdict` files:
```
[Magic:4][Version:4][ID:4][Level:4][RawLen:4][CRC32:4][RawBytes]
```

Magic: "ZSTD"  
Version: 1  
ID: unique identifier  
Level: compression level  
RawLen: dictionary size in bytes  
CRC32: checksum  
RawBytes: dictionary data

Location: `dict_file.go:52-67`

### Compression Pipeline

**Compress:**
1. `LZ4_createStream()` - initialize stream
2. `LZ4_loadDict(dict)` - load dictionary
3. `LZ4_compress_fast_continue(src, dst, level)` - compress block
4. Prepend 4-byte size header

**Decompress:**
1. Read 4-byte size header
2. Allocate buffer from pool (max 5KB)
3. `LZ4_decompress_safe_usingDict(src, dst, dict)` - decompress
4. Return pooled buffer (caller must call `Release()`)

Location: `dict.go:228-305`

### Registry

LRU cache of loaded dictionaries:

```go
type Registry struct {
    cache    map[uint32]*Dictionary
    lru      []uint32
    maxCount int
    diskDir  string
}
```

On dictionary request:
1. Check in-memory cache
2. If not found and `diskDir` set, load from disk
3. Evict LRU entry if cache full

Location: `dict.go:379-414`

---

## Skiplist Implementation

File: `internal/skiplist/skiplist.go`

### Structure

```go
type node struct {
    key  []byte
    op   atomic.Pointer[ops.Op]
    next []atomic.Pointer[node]
}
```

Lock-free design using atomic pointers. Max height: 20 levels.

### Height Selection

Deterministic hash-based:

```go
func randomHeight(key []byte) int32 {
    h := int32(1)
    hash := xxhash.Sum64(key) ^ seed
    
    for h < maxHeight && (hash&3) == 0 {
        h++
        hash >>= 2
    }
    return h
}
```

Probability: 25% chance of each additional level. Same key always gets same height.

Location: `skiplist.go:175-185`

### Insert Algorithm

1. Find splice: locate predecessor nodes at all levels
2. Create node with determined height
3. Publish level 0 with CAS
4. Publish upper levels with retry on conflict
5. Update skiplist height atomically if needed

```go
func (s *SkipList) Apply(key []byte, op ops.Op) {
    for {
        s.findSplice(key, &prevList, &nextList)
        
        if next := nextList[0]; next != nil && bytes.Equal(next.key, key) {
            s.updateNode(next, op)
            return
        }
        
        newNode := newNode(key, op, nodeHeight)
        if !s.publishBaseLevel(newNode, &prevList, &nextList) {
            continue
        }
        
        s.publishUpperLevels(key, newNode, nodeHeight, &prevList, &nextList)
        return
    }
}
```

Location: `skiplist.go:77-102`

### Update Algorithm

When key exists, update Op pointer in-place without relinking:

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

Inc operations merge to reduce memory:

```go
func coalesceToNew(old, next ops.Op) ops.Op {
    switch next.Kind {
    case ops.OpPut, ops.OpDelete:
        return next
    case ops.OpInc:
        if old.Kind == ops.OpInc {
            sum := ops.DecodeInc(old) + ops.DecodeInc(next)
            return ops.NewInc(sum)
        }
        return next
    }
}
```

Location: `skiplist.go:295-308`

---

## Write-Ahead Log

File: `internal/leaf/wal.go`

### Double Buffering

Two buffers enable concurrent writes and flushes:

```go
type WAL struct {
    active   *bytes.Buffer
    flushing *bytes.Buffer
    mu       sync.Mutex
    file     *os.File
}
```

Active buffer accumulates writes under mutex. Background goroutine swaps buffers and flushes to disk.

### Record Format

```
[kind:1][keyLen:8][payloadLen:8][key][payload]
```

Kind: OpPut/OpDelete/OpInc  
KeyLen: varint  
PayloadLen: varint  
Key: raw bytes  
Payload: operation data

Location: `wal.go:117-123`

### Group Commit

Background goroutine flushes every 200µs:

```go
func (w *WAL) flushLoop() {
    ticker := time.NewTicker(200 * time.Microsecond)
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

**Async (default):** Returns immediately after appending to active buffer.  
**Sync:** Blocks until flush completes.

Each flush performs `Write()` followed by `Sync()` for durability.

Location: `wal.go:170-174`

---

## Merge Process

File: `internal/oneleafdb/db.go`, `internal/leaf/leaf.go`

### Trigger

Background worker checks `BufferedBytes >= ThresholdBytes` (default 5MB). Wakes on notification channel.

Location: `db.go:277-292`

### Algorithm

1. Freeze buffer: stop new writes, create immutable snapshot
2. Create iterator: merge frozen skiplist with current segment
3. Build new segment: write merged data to new file
4. Atomic swap: replace old segment reader with new one
5. Retire old segment: add to TTL queue (2s grace period)

```go
func (db *DB) runMergeWorker() {
    for range db.notifyMergeWorker {
        if db.leaf.BufferedBytes() < db.thresholdBytes {
            continue
        }
        
        snapshot := db.leaf.Freeze()
        newSegment, err := db.merger.merge(snapshot, db.leaf.CurrentSegment())
        db.leaf.ReplaceSegment(newSegment)
        db.retireSegment(oldSegment, 2*time.Second)
    }
}
```

Location: `db.go:186-211`

### Buffer Handling

Frozen buffer remains readable during merge. After merge completes, frozen buffer discarded and fresh active buffer created.

```go
type Leaf struct {
    active  *Buffer
    frozen  *Buffer
    segment *Reader
}
```

Location: `leaf.go:183-200`

---

## Cache Layer

File: `internal/oneleafdb/cache.go`

### Structure

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

Zero-copy lookup using `unsafe.String`:

```go
keyStr := unsafe.String(unsafe.SliceData(key), len(key))
entry, ok := c.entries[keyStr]
```

Location: `cache.go:37`

### Eviction

Random map iteration when size exceeds `maxBytes`:

```go
func (c *Cache) evictOne() {
    for k := range c.entries {
        delete(c.entries, k)
        return
    }
}
```

Location: `cache.go:63-69`

### Epoch-Based Invalidation

Each write increments global epoch. Cache entries store creation epoch. On read, mismatched epoch triggers cache miss.

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
        return nil, false
    }
    return entry.value, true
}
```

Location: `cache.go:31-40, 124-127, 146-150`

---

## Value Encoding

File: `internal/value/value.go`

### Type Tag Placement

Type tag stored at end of encoded data to enable prefix scanning without decoding values.

```go
type Kind byte

const (
    KindBytes Kind = 1
    KindInt64 Kind = 2
)
```

### Formats

**Bytes:**
```
[raw bytes][0x01]
```

**Int64:**
```
[varint][0x02]
```

### Encoding

```go
func EncodeBytes(data []byte) []byte {
    data = append(data, byte(KindBytes))
    return data
}

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
    return data[:len(data)-1], nil
}
```

Location: `value.go:25-56`

### Rationale

End-tag design allows key prefix iteration to skip value parsing until needed, reducing CPU in range scans.

---

## Data Flow

### Read Path

```
1. Check skiplist buffer
   - If found: return immediately
   - If Delete tombstone: return not found
   - If Inc: resolve against base value from segment

2. Check cache
   - If found and epoch matches: return cached value

3. Read from segment
   a. Bloom filter: probably contains key?
   b. Index: binary search for block
   c. Read block with pooled buffer
   d. Decompress with LZ4 dictionary
   e. Scan block for exact key
   f. Cache result
```

Code path:

```go
func (db *DB) Get(key []byte) ([]byte, bool, error) {
    if op, ok := db.leaf.buffer.Read(key); ok {
        if op.Kind == ops.OpDelete {
            return nil, false, nil
        }
        if op.Kind == ops.OpPut {
            return op.Data, true, nil
        }
    }
    
    if val, ok := db.cache.Get(key); ok {
        return val, true, nil
    }
    
    val, ok, err := db.leaf.segment.Get(key)
    if ok {
        db.cache.Set(key, val)
    }
    return val, ok, err
}
```

Location: `db.go:120-150`

### Write Path

```
1. Append to WAL
   - Serialize operation
   - Append to active buffer
   - If sync mode: wait for flush

2. Apply to skiplist
   - If key exists: coalesce operations
   - If new key: insert new node
   - Update DataBytes counter

3. Invalidate cache
   - Increment global epoch
   - All cached entries now stale

4. Check merge threshold
   - If BufferedBytes >= ThresholdBytes: notify merge worker
```

Code path:

```go
func (db *DB) Put(key, value []byte) error {
    if err := db.wal.Append(ops.OpPut, key, value); err != nil {
        return err
    }
    
    db.leaf.buffer.Apply(key, ops.Op{
        Kind: ops.OpPut,
        Data: value,
    })
    
    db.cache.Invalidate()
    
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
defer pb.Release()
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

Skiplist stores keys directly without cloning:

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

## References

Pugh, William (1990). "Skip lists: a probabilistic alternative to balanced trees"  
LZ4 compression: https://github.com/lz4/lz4  
Bloom filters: https://github.com/bits-and-blooms/bloom  
Pebble: https://github.com/cockroachdb/pebble

---

Last updated: 2026-05-11
