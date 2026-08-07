# FuseDB Architecture

Technical documentation of FuseDB internals for contributors and researchers.

[Русский](./ARCHITECTURE.ru.md)

## Table of Contents

- [System Overview](#system-overview)
- [Leaf Tree](#leaf-tree)
- [Segment Format](#segment-format)
- [Compression System](#compression-system)
- [Skiplist Implementation](#skiplist-implementation)
- [Leaf Buffer](#leaf-buffer)
- [Write-Ahead Log](#write-ahead-log)
- [Manifest](#manifest)
- [Merge and Split](#merge-and-split)
- [Recovery and Checkpoints](#recovery-and-checkpoints)
- [Cache Layer](#cache-layer)
- [Value Encoding](#value-encoding)
- [Data Flow](#data-flow)
- [Performance Optimizations](#performance-optimizations)
- [References](#references)

---

## System Overview

FuseDB partitions the keyspace into an ordered set of leaves. A leaf owns a
half-open key range, an in-memory mutation buffer, and at most one immutable
on-disk segment. Each leaf merges independently, so the volume rewritten by one
merge is bounded by the leaf size rather than by the size of the database.

The public entry point is `pkg/oneleafdb` (`DB`, `OpenDB`).

### Core Components

**Leaf Tree** (`internal/tree`)
The ordered leaf set of one database directory. Published copy-on-write through
an `atomic.Pointer`, searched by binary search, and mutated structurally only
under a single mutex that lookups and ordinary writes never take.

**Leaf** (`internal/leaf`)
One key range: a two-layer mutation buffer plus an atomically swappable segment
reader. Owns the merge/split pass over its own data.

**Skiplist Buffer** (`internal/skiplist`)
Lock-free ordered index storing recent mutations. Supports Put, Delete, and Inc
operations with in-place coalescing for counters.

**Segment** (`internal/segment`)
Immutable on-disk structure containing data blocks, a bloom filter, a block
index, and metadata. Written once during merge, never modified.

**Write-Ahead Log** (`internal/wal`)
Append-only log of sequence-numbered, checksummed records with group commit.
Readable: it supports replay, torn-tail recovery, and truncation after a
checkpoint.

**Manifest** (`internal/manifest`)
Persistent catalog: leaf ranges, segment ids and versions, and the log watermark
already covered by segments. Rewritten in full and atomically on every update.

**Compression** (`internal/compression`)
Optional LZ4 dictionary compression with a persistent registry. Dictionaries are
trained offline from representative samples and stored as `.zdict` files.

**Cache** (`pkg/oneleafdb/cache.go`)
Map-based value cache with sharded epoch invalidation. Evicts arbitrary entries
when the byte budget is exceeded.

### Design Principles

- Immutability: segments are never modified after creation
- Lock-free reads: neither a lookup nor an ordinary buffered write takes a lock
- Local merge: only the leaf that crossed its threshold is rewritten
- Copy-on-write structure: the leaf slice is replaced whole, never edited
- Zero-copy: pooled buffers minimize allocations
- Dictionary compression: pre-trained LZ4 dictionaries for small blocks

### Key Constraints

The empty key is rejected by `Tree.Put`, `Tree.Delete`, and `Tree.Inc` with
`ErrEmptyKey`. An empty key cannot be a block separator in the segment index
(`ErrEmptySeparator`), and an empty low key already means "leftmost leaf", so
accepting one would surface later as a merge that fails for the whole leaf.

---

## Leaf Tree

File: `internal/tree/tree.go`

### Structure

Leaves are held in a sorted slice published through an atomic pointer:

```go
type Tree struct {
    leaves atomic.Pointer[[]*leaf.Leaf]
    gen    atomic.Uint64
    mu     sync.Mutex
    manifest *manifest.Manifest
    // ...
}
```

A leaf covers `[LowKey, LowKey of the next leaf)`; the last leaf is unbounded on
the right and the leftmost leaf has an empty `LowKey`, so the leaves together
cover the whole keyspace with no hole. A leaf never widens or narrows: it is
replaced by new leaves when it splits.

The leaf set stays small even for a very large database, which is why a sorted
slice is used instead of a concurrent B-tree. At the default 64 MiB per leaf, a
terabyte of data is roughly sixteen thousand pointers.

### Lookup

`findLeaf` returns the last leaf whose low key is not greater than the target:

```go
func searchLeaf(leaves []*leaf.Leaf, key []byte) int {
    lo, hi := 0, len(leaves)
    for lo < hi {
        mid := int(uint(lo+hi) >> 1)
        if bytes.Compare(leaves[mid].LowKey(), key) <= 0 {
            lo = mid + 1
        } else {
            hi = mid
        }
    }
    return lo - 1
}
```

### Generation Counter

`Tree.Get` takes no lock. It brackets the lookup with the merge generation
instead. `gen` is odd while a merge result is being installed and is bumped on
both sides of that window, so a lookup that observed a torn state sees a
different generation and retries.

```go
gen := t.gen.Load()
if gen&1 != 0 { runtime.Gosched(); continue }

target := t.leafFor(key)
value, ok, err := target.Get(key)
if t.gen.Load() == gen {
    return value, ok, err
}
```

### Write Retry

Buffered writes take the read side of the leaf write lock, so writers never
block each other. `TryPut` / `TryDelete` / `TryInc` return false when the leaf
has been detached by a split, which tells the tree to resolve the key against
the updated leaf set and retry. The retry terminates because a split publishes
its new leaves before it releases the lock that rejected the write.

### Options

| Option | Default | Meaning |
|---|---|---|
| `MaxLeafBytes` | `64 MiB` | merged payload size at which a merge cuts a new output segment, and with it a new leaf |
| `MergeThresholdBytes` | `8 MiB` | buffered payload size that makes a leaf a merge candidate |
| `TargetBlockSize` | `4 KiB` | target size of one encoded segment block |
| `BloomFalsePositive` | `0.01` | target bloom filter false positive rate |
| `Compression` | `CompressionNone` | `CompressionLZ4Dict` when a dictionary is supplied |
| `Seed` | caller-provided | mixed into skiplist height selection; reused by split outputs |

---

## Segment Format

Files: `internal/segment/segment_file.go`, `header.go`/`header_codec.go`,
`footer.go`/`footer_codec.go`, `index.go`/`index_codec.go`,
`block.go`/`block_codec.go`, `bloom_filter.go`

### Layout

```
[Header][Data Blocks][Bloom Filter][Index][Footer]
```

Section boundaries are validated on open: the bloom section must start exactly
where the data section ends, the index where the bloom ends, and the footer must
end at the file size (`validateSegmentLayout`).

Segments are named `segment-<id>-v<version>.seg` (both zero-padded to 20
digits). They are written to a `.tmp` sibling and become visible only on
`Freeze`, which syncs the file, renames it, and syncs the directory.

**Header** (28 bytes)

```
[magic "FHDR":4][format version:4][segment version:8][compression:4][dictionary id:4][crc32:4]
```

Format version is 3. The dictionary id must be zero for a raw segment and
non-zero for an LZ4-dictionary segment.

**Data Blocks** (variable length)
A sequence of independently encoded blocks. With `CompressionLZ4Dict` each
encoded block is compressed as a whole and framed as
`[uncompressed size:4][LZ4 payload]`. With `CompressionNone` the encoded block is
stored as-is.

**Bloom Filter** (variable length)

```
[magic "FBLM":4][version:4][numBits:4][numHashes:4][bitset length:4][bitset][crc32:4]
```

The filter is implemented in this repository (`internal/segment/bloom_filter.go`),
not taken from an external package. It is a classic `m`/`k` bloom filter sized
from the expected key count and the target false positive rate, addressed by
double hashing: `h1 = xxhash.Sum64(key)` and `h2 = splitmix64(h1 ^ φ)`, with bit
`i` at `(h1 + i*h2) mod numBits`.

**Index** (variable length)

```
[entry count:4] then per entry: [separator length:4][separator][offset:8][length:4]
```

In memory:

```go
type BlockIndexEntry struct {
    Separator []byte // inclusive upper bound of the block key range
    Offset    uint64 // relative to the start of the data section
    Length    uint32 // encoded (and compressed) block length
}
```

A point lookup finds the first entry whose separator is greater than or equal to
the target key. Entries must be sorted by separator and must not overlap.

**Footer** (64 bytes)

```
[magic "FFTR":4][version:4][block count:4][Data section:16][Bloom section:16][Index section:16][crc32:4]
```

Each section is `[offset:8][length:8]`.

### Block Structure

One encoded block, checksummed with CRC-32 (IEEE):

```
[magic "FBLK":4][version:4][entry count:4]
  per entry: [keyLen:4][valueLen:4][key][value]
  offset table: [entry offset:4] * entry count
[crc32:4]
```

The trailing offset table is what makes a block randomly addressable: `BlockView`
binary searches the entries without decoding the block, and the entry payload is
delimited by the next offset.

Target block size is 4 KiB (`DefaultTargetBlockSize`). The writer flushes the
current block before appending an entry that would push it past the target, so a
block may overshoot only when a single entry is larger than the target.

### Read Algorithm

1. Check the segment-wide bloom filter; a negative answer ends the lookup
2. Binary search the in-memory index for the block whose separator covers the key
3. Read the block bytes into a pooled buffer
4. Decompress with the LZ4 dictionary when the segment is compressed
5. Binary search the block through `BlockView.Find`

Code path: `Reader.Get`, `Reader.readBlockValue`, `BlockView.Find`.

---

## Compression System

Files: `internal/compression/dict.go`, `dict_file.go`

Compression is optional and segment-wide. `OpenDB` uses `CompressionNone` unless
a dictionary is passed in `DBOptions.Dictionary`, in which case every block of
every newly written segment is compressed with `CompressionLZ4Dict`.

### Dictionary Training

Dictionaries are trained offline with `github.com/klauspost/compress/dict`:

```go
raw, err = dictbuilder.BuildRawDict(samples, dictbuilder.Options{
    MaxDictSize: opts.Size, // default 4 KiB
    HashBytes:   6,
})
```

Training extracts common patterns from sample data. On the corpus and machine
recorded in `benchmarks/RESULTS.md` (dated 2026-05-11, before the leaf tree
rewrite), the 4 KiB LZ4 dictionary saved 62.6% of block bytes.

Code path: `TrainDictionary`.

### Persistence Format

Dictionaries are stored as `.zdict` files (`dict-%08d.zdict`), written
atomically:

```
[Magic:4][Version:4][ID:4][Level:4][RawLen:4][CRC32:4][RawBytes]
```

Magic: `"FDDC"`
Version: 1
ID: unique identifier, must be non-zero
Level: LZ4 acceleration used by the codec
RawLen: dictionary size in bytes
CRC32: IEEE checksum over the raw dictionary bytes only
RawBytes: dictionary data

Code path: `EncodeDictionary`, `DecodeDictionary`.

### Compression Pipeline

**Compress:**
1. `LZ4_createStream()` — initialize stream
2. `LZ4_loadDict(dict)` — load dictionary
3. `LZ4_compress_fast_continue(src, dst, acceleration)` — compress block
4. Prepend a 4-byte header holding the uncompressed size

**Decompress:**
1. Read the 4-byte uncompressed size header
2. Take a buffer from the pool (5 KiB), or allocate when the block is larger
3. `LZ4_decompress_safe_usingDict(src, dst, dict)`
4. Return a pooled buffer; the caller must call `Release()`

Code path: `Dictionary.CompressInto`, `Dictionary.Decompress`.

### Registry

LRU cache of loaded dictionaries:

```go
type Registry struct {
    dicts map[uint32]*registryEntry
    lru   *list.List
    limit int
    fs    disk.FS
    dir   string
}
```

On a dictionary request (`MustGet`):

1. Check the in-memory map and move the entry to the front of the LRU list
2. If absent and the registry has storage configured, load it from disk
3. Evict the least recently used entry when the limit is exceeded

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

Lock-free design using atomic pointers. Max height: 20 levels. Deletes are
tombstones; nodes are never physically unlinked.

### Height Selection

Deterministic, derived from the key hash and the list seed:

```go
func (s *SkipList) randomHeight(key []byte) int32 {
    var h int32 = 1
    hash := xxhash.Sum64(key) ^ s.seed

    for h < maxHeight && (hash&3) == 0 {
        h++
        hash >>= 2
    }
    return h
}
```

Probability: 25% chance of each additional level. The same key always gets the
same height within one list.

### Insert Algorithm

1. Sample the current list height
2. Find the splice: locate predecessor nodes at all levels
3. If the key already exists, update its Op in place and return
4. Publish level 0 with CAS, retrying the whole loop on conflict
5. Publish upper levels, re-splicing per level on conflict
6. Grow the list height atomically if needed

```go
func (s *SkipList) Apply(key []byte, op ops.Op) {
    for {
        oldHeight := s.height.Load()
        s.findSplice(key, &prevList, &nextList)

        if next := nextList[0]; next != nil && bytes.Equal(next.key, key) {
            s.updateNode(next, op)
            return
        }

        nodeHeight := s.randomHeight(key)
        s.prepareNewLevels(oldHeight, nodeHeight, &prevList, &nextList)

        newNode := newNode(key, op, nodeHeight)
        if !s.publishBaseLevel(newNode, &prevList, &nextList) {
            continue
        }

        s.publishUpperLevels(key, newNode, nodeHeight, &prevList, &nextList)
        s.growHeight(oldHeight, nodeHeight)
        return
    }
}
```

The height must be sampled *before* `findSplice`. `findSplice` fills levels below
the height it observed; reading a larger height afterwards would make
`prepareNewLevels` treat the levels in between as already filled, and
`publishUpperLevels` would then dereference a nil predecessor.

### Update Algorithm

When the key exists, the Op pointer is replaced in place without relinking:

```go
func (s *SkipList) updateNode(n *node, op ops.Op) {
    for i := 0; ; i++ {
        oldPtr := n.op.Load()
        merged := coalesceToNew(*oldPtr, op)

        if n.op.CompareAndSwap(oldPtr, &merged) {
            s.dataBytes.Add(int64(len(merged.Data) - len(oldPtr.Data)))
            return
        }
        if i&7 == 7 {
            runtime.Gosched()
        }
    }
}
```

The periodic `Gosched` keeps a contended CAS loop from starving the goroutine
that would let it make progress.

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
    default:
        return next
    }
}
```

---

## Leaf Buffer

File: `internal/leaf/buffer.go`

A leaf buffer holds two skiplists — the active one that takes new writes and the
frozen one a merge is currently writing out — behind a single atomic pointer:

```go
type layers struct {
    active *skiplist.SkipList
    frozen *skiplist.SkipList
}
```

Holding both in one value is what makes freezing atomic for readers. With
separate pointers a reader could observe the moment between "active replaced"
and "frozen published" and find a key in neither layer, which reads as a missing
key and, for a counter, as a lost increment.

| Method | Effect |
|---|---|
| `Freeze` | moves active into the frozen slot and installs a fresh active list; returns false if a frozen layer is already present |
| `ClearFrozen` | drops the frozen layer after a successful merge |
| `TakeActive` | swaps in a fresh active list and returns the previous one, used for split handover |
| `ReadOp` | reads both layers from one snapshot and merges the result |
| `IterFrozen` | ordered zero-copy iterator over the frozen layer, consumed by merge |

`ReadOp` merges layers with the same rule the skiplist uses inside one layer: an
upper-layer Put or Delete wins outright, and an upper-layer Inc is added to a
lower-layer Inc rather than replacing it.

`Len` counts only the active layer, so it reads as zero right after a freeze even
though the frozen operations still have to reach a segment. Merge decisions use
`Leaf.PendingLen` (active + frozen) instead.

---

## Write-Ahead Log

Files: `internal/wal/wal.go`, `format.go`, `reader.go`

The log lives at `<dir>/wal.log` by default. Every record carries a WAL-assigned
sequence number and a checksum, so a log written by a process that was killed
mid-write can be replayed up to the last intact record.

`DBOptions.DisableWAL` runs without a log; it exists for benchmarks that measure
the storage engine alone and is not a supported mode for real data.

### File Header

```
[magic "FWAL":4][format version:4][baseSeq:8][crc32c:4]
```

`baseSeq` is the sequence number of the first record the file can hold. It moves
forward when the log is truncated after a checkpoint.

### Record Format

```
[kind:1][seq:uvarint][keyLen:uvarint][payloadLen:uvarint][key][payload][crc32c:4]
```

Kind is 1 (put), 2 (delete), or 3 (inc). Values start at 1 so that a zero byte is
never a valid record start: a run of zeros in a torn or preallocated region is
rejected instead of decoding into a plausible record.

Lengths and the sequence number are varint encoded, so a typical record spends
three header bytes instead of the twenty a fixed-width layout would need. The
record end stays unambiguous because the decoder learns both lengths before it
reads any variable-size field. Checksums use the Castagnoli polynomial, which has
hardware support on amd64/arm64 and matters because the log checksums every
mutation on the write path.

Code path: `appendRecord`.

### Group Commit

`Append` stages the encoded record in an in-memory buffer under a mutex and
assigns the next sequence number. A single background goroutine owns the file
handle and serves three events:

```go
func (w *WAL) run() {
    ticker := time.NewTicker(w.groupEvery) // default 200µs
    for {
        select {
        case <-ticker.C:
            _ = w.flushActive()
        case request := <-w.flushCh:
            request.done <- w.flushActive()
        case request := <-w.rotateCh:
            request.done <- w.rotate(request.upToSeq)
        case <-w.stop:
            err := w.flushActive()
            w.done <- w.closeFile(err)
            return
        }
    }
}
```

`flushActive` swaps the staging buffer, writes it at the tracked file offset, and
calls `Sync`.

### Sync Modes

**Async (default):** `Append` returns as soon as the record is staged. Durability
happens on the next group commit, on an explicit `Sync`, or on `Close`.
**Sync (`SyncWrites`):** `Append` requests a flush and waits for it.

### Reading and Recovery

`Cursor`, `Iterate`, and `ReadAll` walk the log from the beginning. A cursor stops
at the first record it cannot verify:

- if that record is the trailing one, iteration ends cleanly and `Result` reports
  `TruncatedTail` with `ValidEnd` pointing just past the last intact record —
  this is the expected state after a crash, not corruption
- if it sits earlier in the file, iteration fails with `ErrCorruptRecord` or
  `ErrSequenceGap`

`Open` runs this pass first. A torn tail is physically dropped by rewriting the
file, so leftover bytes from an interrupted write are never mistaken for a record
on a later pass.

### Truncation

`Truncate(upToSeq)` discards records at or below `upToSeq`. Survivors are
rewritten into a fresh file whose header declares `baseSeq = upToSeq+1`. The
replacement is installed through a temporary file, an atomic rename, and a
directory sync, so a crash at any point leaves either the old or the new complete
log in place.

---

## Manifest

Files: `internal/manifest/manifest.go`, `manifest_codec.go`

The manifest is the persistent catalog that lets an existing database be reopened
instead of rebuilt or refused. It is stored as `MANIFEST` in the database
directory.

```go
type Manifest struct {
    NextSegmentID uint64 // segment id allocator; 0 is reserved for "no segment"
    AppliedSeq    uint64 // highest WAL sequence number durable in these segments
    Leaves        []LeafRecord // sorted strictly ascending by LowKey
}

type LeafRecord struct {
    LeafID         uint64
    LowKey         []byte
    SegmentID      uint64
    SegmentVersion uint64
}
```

Leaf ranges are half-open and only the lower bound is stored: a leaf covers
`[LowKey, next leaf's LowKey)` and the last leaf covers everything to the right.
A `SegmentID` of zero means the leaf has no segment yet, which is the valid state
of a freshly created empty leaf.

### On-Disk Layout

```
header:  [magic "FMAN":4][format version:4]
body:    [NextSegmentID:8][AppliedSeq:8][leaf count:4]
         per leaf: [LeafID:8][SegmentID:8][SegmentVersion:8][low key length:4][low key]
trailer: [crc32:4]   // IEEE, over the body only
```

Low keys are length prefixed rather than delimited, so empty keys and keys
containing arbitrary bytes, including NUL, round-trip unchanged.

### Update Strategy

The manifest is rewritten in full on every update through
`disk.WriteFileAtomically`. This is deliberate: the manifest holds thousands of
leaf records at most, so a complete rewrite costs far less than an append-only
edit log with periodic compaction, and it makes every on-disk state a
self-contained, CRC-checked snapshot. If the leaf count ever grows past that
scale, this is the decision to revisit.

### Invariants

`Validate` runs before every save and after every load:

- leaves are strictly ascending by low key
- leaf ids are unique
- only the first leaf may carry an empty low key
- a non-empty leaf set must start at the empty low key, so no part of the
  keyspace is left uncovered

---

## Merge and Split

Files: `internal/tree/merge.go`, `internal/leaf/split.go`, `internal/leaf/merger.go`

### Trigger

Every write notifies a background worker through a one-slot channel. The worker
decides what kind of work is due:

```go
func (db *DB) runMergeCycle() error {
    if db.walBytes.Load() >= db.checkpointBytes { // default 64 MiB
        return db.checkpoint()
    }
    if db.tree.BufferedBytes() < db.thresholdBytes { // default 8 MiB
        return nil
    }
    return db.mergeLeaves()
}
```

`mergeLeaves` merges only the leaves `Tree.PendingMerge` reports — those whose
buffer crossed `MergeThresholdBytes`, plus any leaf whose previous merge failed
after freezing its buffer — and then saves the manifest. It deliberately does not
move the log watermark: only some leaves were merged, so the records for the
others are still needed.

The first failure of a background merge is stored and surfaced on the next user
call, so a broken merge cannot look like a healthy database that quietly stopped
merging.

### Merging One Leaf

`Tree.MergeLeaf` holds the structural mutex for the whole pass:

1. Verify the leaf is still part of the published tree; a leaf replaced by an
   earlier split is ignored, so a caller may hand back a leaf list it collected
   earlier
2. Freeze the leaf buffer
3. Build a merged stream: `Merger.MergeIter` walks the segment iterator and the
   frozen operation iterator in lockstep, resolving Put, Delete, and Inc against
   the segment value
4. Stream the result into output segments (see below)
5. Persist the manifest, then install the result

The source segment iterator is consulted through `IterWithErr`. Without checking
that accessor a single unreadable block would silently drop every remaining key,
and the truncated result would then replace the healthy segment.

### Splitting

`Leaf.MergeSplit` cuts the output on accumulated payload bytes rather than on a
precomputed median, which is what keeps the merge single-pass: the source is read
once, and a new output starts as soon as the current one is full.

```go
func (b *splitBuilder) add(key, value []byte) error {
    if b.current != nil && b.opts.MaxBytes > 0 && b.currentBytes >= b.opts.MaxBytes {
        if err := b.closeCurrent(); err != nil {
            return err
        }
        b.nextLow = bytes.Clone(key)
    }
    // ... open the next output if needed, then append
}
```

The cut lands on a key boundary, so an output can overshoot `MaxLeafBytes` by at
most one entry. The first key of the next output becomes that leaf's inclusive
lower bound.

Segment identities are allocated in output order. The first output continues the
range the leaf already owns, so it keeps the segment id and only moves to the
next version; every later output gets a freshly allocated id at version 1.

On any failure every output produced so far is removed, so a failed merge leaves
no partial segments behind. Segment ids consumed by a failed attempt are not
reused: a leftover file from a partially failed removal must never be hit by a
later merge.

### Bloom Filter Sizing

Every output is sized for the full merged key count:

```go
expectedKeys := l.segmentKeys.Load() + l.buffer.FrozenLen()
```

Sizing for the buffer alone, as the single-segment path used to, degrades the
filter a little more with every merge. A leaf reopened from the manifest starts
with `segmentKeys == 0` because the segment file does not record its key count,
so the first merge after reopening sizes its filter from the buffer alone.

### Installing the Result

| Outputs | Path | Effect |
|---|---|---|
| 0 | `installEmptyLocked` | every key resolved to a tombstone; the leaf keeps its range and loses its segment |
| 1 | `installMergeLocked` | the leaf keeps its identity, buffer, and place in the tree; only the segment underneath is exchanged |
| >1 | `installSplitLocked` | the leaf is replaced by one leaf per output |

The publication order in a split is what keeps writes from being lost. The new
leaves are built and the manifest is persisted first, while the old leaf is still
the one writers use. Then, under the old leaf's write lock, the operations
buffered after the merge froze are routed into the new leaves, and only after
that are the new leaves published. Publishing first would let a fresh write land
in a new leaf and then be overwritten by an older replayed operation for the same
key.

### Retiring Segments

A replaced reader is not closed immediately: lookups take no lock, so one may
already be inside the segment. Readers go to a queue drained by a single
per-tree worker (`internal/tree/retire.go`) that closes the reader and removes
the file after a 2s TTL, checking every 100ms. One shared worker replaces the
per-leaf goroutine a tree of thousands of leaves would otherwise need.

---

## Recovery and Checkpoints

File: `pkg/oneleafdb/db.go`

### Opening a Database

`OpenDB` restores a database from disk:

1. Load `MANIFEST`. A missing file is the normal state of a fresh directory and
   yields a new tree with one empty leaf covering the whole keyspace
2. `tree.Open` reopens the segment of every leaf record that names one, rebuilds
   the leaf set in stored order, and continues leaf id allocation past the
   highest id it found
3. Open the write-ahead log, which drops a torn trailing record if present
4. Replay the log tail

```go
func (db *DB) replayWAL(path string, appliedSeq uint64) error {
    _, err := wal.Iterate(db.fs, path, func(record wal.Record) error {
        if record.Seq <= appliedSeq {
            return nil
        }
        switch record.Kind {
        case ops.OpPut:    return db.tree.Put(record.Key, record.Payload)
        case ops.OpDelete: return db.tree.Delete(record.Key)
        case ops.OpInc:    return db.tree.Inc(record.Key, ops.DecodeInc(record.Op()))
        }
        // ...
    })
    // ...
}
```

Records at or below `AppliedSeq` are already durable in segments; replaying them
would double every increment they contain.

### Checkpoints

A checkpoint is what advances the watermark and lets the log be truncated. It is
run by `DB.Merge`, by `Close`, and by the background worker once the log has grown
past `WALCheckpointBytes`.

```go
func (db *DB) checkpoint() error {
    db.applyMu.Lock()
    watermark := db.walLastSeq()
    db.tree.FreezeAll()
    db.applyMu.Unlock()

    if err := db.tree.MergeAll(); err != nil { /* ... */ }

    db.tree.SetAppliedSeq(watermark)
    if err := db.tree.SaveManifest(); err != nil { /* ... */ }
    if err := db.walTruncate(watermark); err != nil { /* ... */ }
    db.walBytes.Store(0)
    return nil
}
```

### The Exact Barrier

`applyMu` orders log writes against buffer freezing. Writers hold the read side
across "append to log, apply to tree"; the checkpoint takes the write side just
long enough to read the last sequence number and freeze every leaf buffer.

That makes the recorded watermark exact: every operation at or below it is in the
frozen set, and nothing above it is. A watermark that is behind the data would
replay an increment a segment already contains and silently double it. Reading
the sequence number and freezing must happen in the same excluded window —
leaves created after the freeze are not covered, which is the other reason
writers are held off across both steps.

`MergeAll` then merges every leaf that holds pending operations. It uses
`PendingLen`, not `BufferedLen`: after `FreezeAll` the active layer is empty
while the frozen operations still need writing, and a merge pass that concluded
there was nothing to do would drop log records covering real data. The pass
repeats at most four times, because a split moves buffered writes into leaves the
pass has already visited.

---

## Cache Layer

File: `pkg/oneleafdb/cache.go`

### Structure

```go
type valueCache struct {
    mu       sync.RWMutex
    maxBytes int64
    used     int64
    items    map[string]cacheEntry
}

type cacheEntry struct {
    value []byte
    epoch uint64
    size  int64
}
```

Default budget is 5 MiB (`DefaultCacheBytes`); `CacheEntries` is translated into
a byte budget of 160 bytes per entry.

### Sharded Epoch Invalidation

The invalidation counter lives on the `DB` and is split into 256 shards selected
by key hash:

```go
const cacheEpochShards = 256

func cacheShard(key []byte) uint64 {
    return xxhash.Sum64(key) % cacheEpochShards
}

func (db *DB) invalidate(key []byte) {
    db.cacheEpoch[cacheShard(key)].Add(1)
    db.cache.delete(key)
}
```

A single global epoch, which is what this cache used to have, made every write
invalidate every cached entry: under any mixed read/write load the hit rate
collapsed to roughly zero. Sharding confines a write to one shard, so entries in
the other 255 shards stay usable. It costs one array of counters and keeps the
read path lock-free with respect to invalidation.

An entry is still only valid while its own shard's counter is unchanged, so a
write to an unrelated key in the same shard invalidates it too. That is the price
of not tracking per-key versions.

On a read, `Get` reads the shard counter first, uses it for the lookup, and only
stores a fresh entry if the counter has not moved in the meantime:

```go
shard := cacheShard(key)
epoch := db.cacheEpoch[shard].Load()
if value, ok := db.cache.get(key, epoch); ok {
    return value, true, nil
}
// ... read through the tree ...
if db.cacheEpoch[shard].Load() == epoch {
    db.cache.set(key, value, epoch)
}
```

### Key Format

Lookups borrow the key bytes as a map key without allocating; only `set`
materializes an owned string.

```go
func cacheKeyView(key []byte) string {
    if len(key) == 0 {
        return ""
    }
    return unsafe.String(&key[0], len(key))
}
```

The empty case is explicit because taking the address of `key[0]` panics on an
empty slice.

### Eviction

Arbitrary map iteration order until the entry fits the byte budget:

```go
for c.used+size > c.maxBytes {
    for evict := range c.items {
        c.used -= c.items[evict].size
        delete(c.items, evict)
        break
    }
}
```

A value larger than the whole budget is not cached at all.

### Ownership

The cache always clones on both `set` and `get`. Callers pass views of skiplist
or block memory that can be rewritten or released underneath it, and a caller
that mutated a returned slice would otherwise corrupt the cache.

---

## Value Encoding

File: `internal/value/value.go`

### Type Tag Placement

The type tag is stored at the end of the encoded data so that key prefix
iteration can skip value parsing until it is needed.

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
    encoded := make([]byte, len(data)+1)
    copy(encoded, data)
    encoded[len(data)] = byte(KindBytes)
    return encoded
}

func EncodeInt64(v int64) []byte {
    var buf [binary.MaxVarintLen64 + 1]byte
    n := binary.PutVarint(buf[:], v)
    buf[n] = byte(KindInt64)
    return buf[:n+1]
}
```

`EncodeBytes` never appends into the caller's backing array: appending the tag in
place would corrupt bytes past `len(data)` whenever the caller passed a slice
with spare capacity. Because the result is freshly allocated, the leaf hands it
straight to `ops.NewPutOwned` instead of paying for a second copy.

### Decoding

The last byte is read first to determine the kind, then the payload is extracted:

```go
func KindOf(data []byte) (Kind, error) {
    if len(data) == 0 {
        return 0, ErrEmptyValue
    }
    kind := Kind(data[len(data)-1])
    switch kind {
    case KindBytes, KindInt64:
        return kind, nil
    default:
        return 0, fmt.Errorf("%w: %d", ErrUnknownKind, kind)
    }
}
```

`DecodeBytes` and `DecodeInt64` both fail with `ErrKindMismatch` when the tag
does not match what the caller asked for, so a counter can never be silently read
as bytes.

### Rationale

The end-tag design allows key prefix iteration to skip value parsing until
needed, reducing CPU in range scans.

---

## Data Flow

### Read Path

```
1. Cache
   - Load the shard epoch for the key
   - If an entry exists with that epoch: return a copy

2. Tree
   - Bracket the lookup with the merge generation
   - Binary search the leaf slice for the owning leaf

3. Leaf buffer (both layers from one snapshot)
   - Put: return the buffered value
   - Delete tombstone: return not found
   - Inc: read the segment base value and add the delta

4. Segment (only if the buffer has no answer, or to resolve an Inc)
   a. Bloom filter: may contain the key?
   b. Index: binary search for the block
   c. Read the block into a pooled buffer
   d. Decompress with the LZ4 dictionary if compressed
   e. Binary search the block for the exact key

5. Decode the value tag, copy for the caller, cache if the shard epoch held
```

Code path:

```go
func (db *DB) Get(key []byte) ([]byte, bool, error) {
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

    valueCopy := make([]byte, len(value))
    copy(valueCopy, value)
    if db.cacheEpoch[shard].Load() == epoch {
        db.cache.set(key, value, epoch)
    }
    return valueCopy, true, nil
}
```

`tree.Get` hands back a view of live skiplist or block memory. Returning it
straight to the caller would let a caller-side write corrupt the database, so the
caller gets its own copy and the cache clones separately.

### Write Path

```
1. Take the read side of applyMu

2. Append to the WAL
   - Encode the record with a sequence number and CRC
   - Stage it in the active buffer
   - If SyncWrites: wait for the flush

3. Apply to the tree (still under applyMu)
   - Reject the empty key
   - Binary search the leaf slice for the owning leaf
   - Buffer the operation under the leaf read lock
   - If the leaf was detached by a split: re-resolve and retry

4. Release applyMu, then invalidate the cache
   - Bump only the key's shard counter
   - Delete the cached entry for the key

5. Account for log growth and notify the merge worker
```

Code path:

```go
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
```

---

## Performance Optimizations

### Lock-Free Read Path

A lookup takes no mutex anywhere: it loads the leaf snapshot atomically, binary
searches it, reads a lock-free skiplist, and reads an immutable segment. The only
synchronization is the merge generation counter, which costs the reader two
atomic loads and the writer two atomic adds per merge, and whose window covers
only the install itself, never the segment build.

### Bounded Write Amplification

A merge reads and rewrites one leaf. With a single leaf covering the whole
keyspace, every merge rewrote the entire database and write amplification grew
with the database size; with a leaf tree, the rewritten volume is bounded by
`MaxLeafBytes`.

### Single-Pass Split

The split cuts on accumulated bytes at a key boundary while the merged stream is
being written. There is no second pass and no median computation.

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
pb, err := readFullAt(f, offset, length)
if err != nil {
    return err
}
defer pb.Release()
```

Sections larger than the pooled buffer (index and bloom on open) fall back to a
plain allocation.

### Lock-Free Decompression

`Dictionary` guards its closed flag with an `atomic.Bool`, so `Decompress` never
takes the mutex; only `Compress` and `Close` do.

```go
func (d *Dictionary) Decompress(src []byte) (*PooledDecompressBuffer, error) {
    if d == nil {
        return nil, ErrNilDictionary
    }
    if d.closed.Load() {
        return nil, ErrDictionaryClosed
    }
    // Decompress without holding a lock
}
```

### Immutable Keys

The skiplist stores the key slice it is given, without cloning:

```go
func newNode(key []byte, op ops.Op, height int32) *node {
    n := &node{
        key:  key,
        next: make([]atomic.Pointer[node], height),
    }
    n.op.Store(&op)
    return n
}
```

Callers must therefore treat a key handed to the buffer as immutable for the
lifetime of the entry.

### Shared Retire Worker

One goroutine per tree drains retired segment readers, rather than one per leaf.
The work is identical for every leaf, and a tree may hold thousands of them.

---

## References

Pugh, William (1990). "Skip lists: a probabilistic alternative to balanced trees"
LZ4 compression: https://github.com/lz4/lz4
Dictionary builder: https://github.com/klauspost/compress
Pebble: https://github.com/cockroachdb/pebble

The bloom filter is implemented in this repository
(`internal/segment/bloom_filter.go`); FuseDB does not depend on an external bloom
filter package.

---

Last updated: 2026-08-04
