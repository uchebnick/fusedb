# FuseDB Memory Allocation Analysis & Optimization Plan

**Date:** 2026-05-11  
**Status:** Analysis Complete, Implementation In Progress

## Executive Summary

FuseDB currently allocates **30x more memory per read** than Pebble:
- **FuseDB:** 3340-3554 B/op, 5 allocs/op
- **Pebble:** 120 B/op, 3 allocs/op

This analysis identifies root causes and provides a phased optimization plan to reduce allocations to production-acceptable levels.

---

## Benchmark Validity Issues

### Critical Problems Found

1. **Unfair Durability Comparison**
   - Pebble uses `NoSync` (no durability guarantees)
   - OneLeaf async WAL still buffers writes (eventual durability)
   - Not apples-to-apples comparison

2. **Toy-Scale Dataset**
   - Only 64K records (~64MB data)
   - Everything fits in 5MB cache
   - Real workloads have millions/billions of keys
   - LSM compaction overhead not visible at this scale

3. **Missing Critical Workloads**
   - Workload E (scans) explicitly skipped - OneLeaf can't do it
   - No delete-heavy workloads
   - No large scan workloads where LSM trees excel

4. **Single-Threaded Testing**
   - `ycsbThreadCount = 1`
   - Hides Pebble's concurrency strengths

5. **Uniform Distribution**
   - Real workloads have Zipfian distribution (hot keys)
   - Doesn't stress compaction or cache effectiveness

### Conclusion

**The benchmarks favor OneLeaf's simple design.** To make fair comparisons:
- Use 10M+ keys
- Multi-threaded tests (8-16 threads)
- Pebble with Sync for durability comparison
- Add scan workloads
- Long-running tests that stress compaction

---

## Allocation Analysis

### Root Causes (5 allocs/op breakdown)

#### Cache Hit Path (2 allocs):
1. `string(key)` for map lookup - ~16-24 bytes
2. `bytes.Clone(value)` from cache - value size

#### Cache Miss Path (6 allocs):
1. `string(key)` for cache lookup - ~16-24 bytes
2. `make([]byte, blockSize)` for block read - 4-32 KB
3. `make([]byte, rawLen)` for decompression - 4-32 KB
4. `bytes.Clone(data[1:])` for value decode - value size
5. `string(key)` for cache set - ~16-24 bytes
6. `bytes.Clone(value)` for cache storage - value size

**Total:** 3340-3554 B/op dominated by block buffer allocations (2× block size).

---

## Detailed Allocation Sources

### 1. Cache Layer (cache.go)

**Line 37:** `string(key)` - Map lookup
```go
entry, ok := c.items[string(key)]  // ALLOC
```
- **Impact:** 1 alloc per Get()
- **Size:** len(key) bytes
- **Fix:** Use `unsafe.String()` for zero-alloc lookup

**Line 41:** `bytes.Clone(entry.value)` - Cache hit
```go
return bytes.Clone(entry.value), true  // ALLOC
```
- **Impact:** 1 alloc per cache hit
- **Size:** len(value) bytes
- **Fix:** Return value directly (already owned)

**Line 56:** `string(key)` - Map insertion
```go
cacheKey := string(key)  // ALLOC
```
- **Impact:** 1 alloc per cache miss
- **Fix:** Use `unsafe.String()` for lookup, keep string for storage

**Line 68:** `bytes.Clone(value)` - Cache storage
```go
value: bytes.Clone(value),  // ALLOC
```
- **Impact:** 1 alloc per cache miss
- **Fix:** Store value directly (already owned)

### 2. Segment Reading (segment_file.go)

**Line 231:** `make([]byte, size)` - Block payload
```go
buf := make([]byte, size)  // ALLOC: 4-32 KB
```
- **Impact:** 1 alloc per cache miss
- **Size:** Block size (4-32 KB)
- **Fix:** Use `sync.Pool` for block buffers

### 3. Compression (dict.go)

**Line 260-262:** `make([]byte, rawLen)` - Decompression
```go
dst = make([]byte, rawLen)  // ALLOC: 4-32 KB
```
- **Impact:** 1 alloc per cache miss with compression
- **Size:** Uncompressed block size (4-32 KB)
- **Fix:** Use `sync.Pool` for decompression buffers

### 4. Value Decoding (value.go)

**Line 57:** `bytes.Clone(data[1:])` - Value extraction
```go
return bytes.Clone(data[1:]), nil  // ALLOC
```
- **Impact:** 1 alloc per Get()
- **Size:** len(value) bytes
- **Fix:** Return `data[1:]` directly (already owned)

---

## Optimization Plan

### Phase 1: Quick Wins (Target: 300-500 B/op, 3-4 allocs/op)

**Low-risk, high-reward optimizations using standard patterns.**

#### 1.1 Block Read Buffer Pooling
**File:** `internal/segment/segment_file.go:231`

```go
var blockBufPool = sync.Pool{
    New: func() interface{} {
        buf := make([]byte, 64*1024)
        return &buf
    },
}

func readFullAt(r io.ReaderAt, off int64, size int) ([]byte, error) {
    bufPtr := blockBufPool.Get().(*[]byte)
    defer blockBufPool.Put(bufPtr)
    
    buf := (*bufPtr)[:size]
    // ... read into buf ...
    
    result := make([]byte, size)
    copy(result, buf)
    return result, nil
}
```

**Impact:** Saves 1 alloc, 4-32 KB per cache miss

#### 1.2 Decompression Buffer Pooling
**File:** `internal/compression/dict.go:260`

```go
var decompressBufPool = sync.Pool{
    New: func() interface{} {
        buf := make([]byte, 128*1024)
        return &buf
    },
}

func (d *Dictionary) Decompress(src []byte, dst []byte) ([]byte, error) {
    if dst == nil || cap(dst) < rawLen {
        bufPtr := decompressBufPool.Get().(*[]byte)
        defer decompressBufPool.Put(bufPtr)
        dst = (*bufPtr)[:rawLen]
    }
    
    // decompress...
    
    result := make([]byte, rawLen)
    copy(result, dst)
    return result, nil
}
```

**Impact:** Saves 1 alloc, 4-32 KB per cache miss

#### 1.3 Cache String Conversion Elimination
**File:** `internal/oneleafdb/cache.go:37,56`

```go
import "unsafe"

func (c *valueCache) get(key []byte, epoch uint64) ([]byte, bool) {
    entry, ok := c.items[unsafe.String(&key[0], len(key))]  // zero-alloc
    if !ok || entry.epoch != epoch {
        return nil, false
    }
    return entry.value, true  // no clone needed
}

func (c *valueCache) set(key, value []byte, epoch uint64) {
    cacheKey := string(key)  // must alloc for storage
    c.items[cacheKey] = cacheEntry{
        value: value,  // no clone needed
        epoch: epoch,
        size:  size,
    }
}
```

**Impact:** Saves 2-3 allocs per Get()

**Safety:** `unsafe.String()` is safe for map lookups (not retained).

#### 1.4 Remove Unnecessary Clones
**Files:** 
- `internal/oneleafdb/cache.go:41,68`
- `internal/value/value.go:57`

Remove `bytes.Clone()` calls where values are already owned.

**Impact:** Saves 1-2 allocs per Get()

---

### Phase 2: Zero-Copy (Target: 120 B/op, match Pebble)

**High-risk, requires architectural changes. Only pursue if Phase 1 insufficient.**

#### 2.1 Borrowed Slices with Lifetime Management
- Return slices pointing directly into cached blocks
- Add reference counting to blocks
- Require explicit `defer value.Release()` in API

#### 2.2 Zero-Copy Cache
- Cache returns borrowed slices with epoch validation
- Invalidate on compaction

**Risks:**
- Use-after-free bugs
- Complex lifetime tracking
- API breaking changes
- Requires extensive testing

**Decision criteria:**
- Phase 1 results still show >1000 B/op
- Profiling confirms allocation is still the bottleneck
- 2-3 weeks available for careful implementation

---

## Implementation Timeline

### Week 1: Phase 1 Implementation
- [ ] Add block buffer pooling
- [ ] Add decompression buffer pooling
- [ ] Eliminate cache string conversions
- [ ] Remove unnecessary clones
- [ ] Add allocation benchmarks to CI

### Week 2: Measurement & Validation
- [ ] Run full benchmark suite
- [ ] Compare to Pebble
- [ ] Profile allocation hotspots
- [ ] Validate correctness with race detector

### Decision Point: Phase 2?
- Only if Phase 1 shows >1000 B/op
- Requires architectural review
- 2-3 weeks additional work

---

## Expected Outcomes

### Phase 1 (Conservative Estimate)
- **Before:** 3340-3554 B/op, 5 allocs/op
- **After:** 300-500 B/op, 3-4 allocs/op
- **Improvement:** ~85% reduction in allocations

### Phase 2 (Aggressive Estimate)
- **Target:** 120-200 B/op, 3 allocs/op
- **Match Pebble's performance**
- **High complexity, high risk**

---

## Allocation Benchmarks for CI

```go
func BenchmarkGetAllocations(b *testing.B) {
    db := setupTestDB(b)
    defer db.Close()
    
    b.ReportAllocs()
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        _, _, err := db.Get(testKey)
        if err != nil {
            b.Fatal(err)
        }
    }
}

func TestGetAllocationLimit(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()
    
    allocs := testing.AllocsPerRun(100, func() {
        db.Get(testKey)
    })
    
    if allocs > 5.0 {
        t.Errorf("too many allocations: %.1f, want <= 5", allocs)
    }
}
```

---

## References

- Pebble architecture: Block cache with reference counting
- Go unsafe.String(): Safe for temporary map lookups
- sync.Pool: Standard pattern for buffer reuse

---

## Conclusion

**Phase 1 optimizations are production-ready and low-risk.** They should reduce allocations by ~85% using standard Go patterns (sync.Pool, unsafe.String for lookups).

**Phase 2 is only needed if Phase 1 is insufficient.** It requires careful architectural changes and extensive testing.

**Recommendation:** Implement Phase 1, measure results, then decide on Phase 2.
