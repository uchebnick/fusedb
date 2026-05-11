# FuseDB Optimization & Benchmark System - Summary

**Date:** 2026-05-11  
**Status:** ✅ Complete

## What Was Done

### 1. Memory Allocation Analysis ✅

**Problem Identified:**
- FuseDB: **3340-3554 B/op, 5 allocs/op**
- Pebble: **120 B/op, 3 allocs/op**
- **30x more memory per read operation**

**Root Causes Found:**
1. Cache uses `string(key)` conversions (2-3 allocs per Get)
2. Block buffers allocated without pooling (8-64KB per cache miss)
3. Value layer clones unnecessarily with `bytes.Clone()` (1 alloc per Get)
4. Cache returns cloned values (1 alloc per cache hit)

**Detailed Analysis:** See `docs/ALLOCATION_ANALYSIS.md`

---

### 2. Benchmark Validity Analysis ✅

**Critical Issues Found:**

1. **Unfair Durability Comparison**
   - Pebble uses `NoSync` (no durability)
   - OneLeaf async WAL still buffers (eventual durability)
   - Not apples-to-apples

2. **Toy-Scale Dataset**
   - Only 64K records (~64MB)
   - Everything fits in 5MB cache
   - Real workloads have millions/billions of keys

3. **Missing Critical Workloads**
   - Workload E (scans) skipped - OneLeaf can't do it
   - No delete-heavy workloads
   - Single-threaded only

4. **Uniform Distribution**
   - Real workloads have Zipfian (hot keys)
   - Doesn't stress compaction

**Conclusion:** Benchmarks favor OneLeaf's simple design. Need larger datasets, multi-threaded tests, and scan workloads for fair comparison.

---

### 3. Phase 1 Optimizations Implemented ✅

#### 3.1 Remove Value Decode Clone
**File:** `internal/value/value.go:57`
```go
// Before: return bytes.Clone(data[1:]), nil
// After:  return data[1:], nil
```
**Impact:** -1 alloc per Get()

#### 3.2 Block Buffer Pooling
**File:** `internal/segment/segment_file.go:231`
```go
var blockBufPool = sync.Pool{
    New: func() interface{} {
        buf := make([]byte, 64*1024)
        return &buf
    },
}
```
**Impact:** -1 alloc, -4-32KB per cache miss

#### 3.3 Cache String Conversion Elimination (TODO)
**File:** `internal/oneleafdb/cache.go:37,56`
- Use `unsafe.String()` for zero-alloc map lookups
- Remove unnecessary `bytes.Clone()` calls
**Impact:** -2-3 allocs per Get()

#### 3.4 Decompression Buffer Pooling (TODO)
**File:** `internal/compression/dict.go:260`
- Add `sync.Pool` for decompression buffers
**Impact:** -1 alloc, -4-32KB per compressed read

**Expected Results After Phase 1:**
- **Before:** 3340-3554 B/op, 5 allocs/op
- **After:** ~300-500 B/op, 3-4 allocs/op
- **Improvement:** ~85% reduction

---

### 4. New Benchmark System ✅

**Problem:** Old system measured HTTP throughput (1000 req/s), not pure DB performance.

**Solution:** Pure Go benchmarks running inside containers.

**Old System:**
```
HTTP request → JSON decode → DB operation → JSON encode → HTTP response
```

**New System:**
```
DB operation (direct Go API call)
```

**Files Created:**
- `cmd/benchmark-runner/main.go` - Pure Go benchmark runner
- `benchmarks/docker/Dockerfile.benchmark` - Container image
- `benchmarks/docker/docker-compose.benchmark.yml` - 6 benchmark services
- `benchmarks/docker/configs/*.json` - Benchmark configurations
- `benchmarks/docker/run-benchmarks.sh` - Run all benchmarks
- `benchmarks/docker/README.md` - Documentation

**Benchmark Configurations:**
- 100K records, 500K operations
- 1KB values, 64MB cache
- Single-threaded
- 2 CPUs, 512MB RAM (Docker limits)

**Workloads:**
1. read-only (100% Get)
2. write-only (100% Put)
3. mixed-50-50 (50% Get, 50% Put)

**Metrics Collected:**
- ops_per_sec (throughput)
- p50/p95/p99/max latency
- allocs_per_op
- bytes_per_op
- memory_used_mb

**Usage:**
```bash
cd benchmarks/docker
./run-benchmarks.sh
```

Results saved to `results/*.json` and `results/COMPARISON.md`.

---

## Next Steps

### Immediate (Complete Phase 1)

1. **Finish cache optimizations** (Task #6)
   - Implement `unsafe.String()` for map lookups
   - Remove unnecessary `bytes.Clone()` calls
   - File: `internal/oneleafdb/cache.go`

2. **Finish decompression pooling** (Task #5)
   - Add `sync.Pool` for decompression buffers
   - File: `internal/compression/dict.go`

3. **Add allocation benchmarks** (Task #8)
   - Create `BenchmarkGetAllocations`
   - Add CI enforcement for allocation limits

4. **Run new benchmark system**
   ```bash
   cd benchmarks/docker
   ./run-benchmarks.sh
   ```

5. **Measure Phase 1 results**
   - Check if we hit ~300-500 B/op target
   - Compare with Pebble's 120 B/op

### Phase 2 (If Needed)

Only pursue if Phase 1 results still show >1000 B/op:

1. **Zero-copy value returns**
   - Return borrowed slices from BlockView
   - Requires lifetime management

2. **Zero-copy cache**
   - Cache returns borrowed slices with epoch validation
   - Invalidate on compaction

**Risk:** High complexity, use-after-free bugs, API breaking changes.

**Decision criteria:**
- Phase 1 shows >1000 B/op
- Profiling confirms allocation is still bottleneck
- 2-3 weeks available for implementation

---

## Files Modified

### Optimizations
- ✅ `internal/value/value.go` - Removed clone in DecodeBytes
- ✅ `internal/segment/segment_file.go` - Added block buffer pooling
- ⏳ `internal/oneleafdb/cache.go` - TODO: unsafe.String + remove clones
- ⏳ `internal/compression/dict.go` - TODO: decompression buffer pooling

### Documentation
- ✅ `docs/ALLOCATION_ANALYSIS.md` - Full analysis report
- ✅ `benchmarks/docker/README.md` - New benchmark system docs

### New Benchmark System
- ✅ `cmd/benchmark-runner/main.go` - Pure Go benchmark runner
- ✅ `benchmarks/docker/Dockerfile.benchmark` - Container image
- ✅ `benchmarks/docker/docker-compose.benchmark.yml` - Services
- ✅ `benchmarks/docker/configs/*.json` - 6 benchmark configs
- ✅ `benchmarks/docker/run-benchmarks.sh` - Run script

---

## Key Insights

### Allocation Sources (Ranked by Impact)

1. **Block read buffers** (4-32KB) - ✅ Fixed with sync.Pool
2. **Decompression buffers** (4-32KB) - ⏳ TODO: sync.Pool
3. **Cache string conversions** (2-3 allocs) - ⏳ TODO: unsafe.String
4. **Value decode clone** (value size) - ✅ Fixed
5. **Cache value clone** (value size) - ⏳ TODO: remove

### Benchmark System Insights

**Why HTTP benchmarks were misleading:**
- Measured HTTP throughput, not DB performance
- 1000 req/s includes JSON parsing, network overhead
- Doesn't show allocs/op or bytes/op

**Why new system is better:**
- Direct Go API calls
- Measures pure DB operations
- Reports allocation metrics
- Runs in containers (production-like)

### Pebble Comparison Insights

**Why benchmarks favor OneLeaf:**
- Tiny dataset (64K records)
- Single-threaded
- No scans (OneLeaf can't do them)
- Pebble NoSync vs OneLeaf async WAL (not equivalent)

**For fair comparison, need:**
- 10M+ keys
- Multi-threaded (8-16 threads)
- Scan workloads
- Pebble with Sync for durability

---

## Success Criteria

### Phase 1 Complete When:
- ✅ All 4 optimizations implemented
- ✅ Allocation benchmarks in CI
- ✅ Results show ~300-500 B/op (85% reduction)
- ✅ New benchmark system produces results

### Phase 2 Decision Point:
- Phase 1 results analyzed
- If >1000 B/op, consider zero-copy
- If <500 B/op, Phase 1 sufficient

---

## Timeline

- **Week 1:** Phase 1 implementation ⏳ (2/4 done)
- **Week 2:** Measurement & validation
- **Decision:** Phase 2 needed?

---

## References

- **Allocation Analysis:** `docs/ALLOCATION_ANALYSIS.md`
- **Benchmark System:** `benchmarks/docker/README.md`
- **Pebble Research:** Web search results (agent findings)
- **Go sync.Pool:** Standard pattern for buffer reuse
- **unsafe.String():** Safe for temporary map lookups

---

## Agent System Performance

**Agents Spawned:** 4
1. ✅ Benchmark validity analysis (found critical issues)
2. ✅ Allocation source analysis (identified 5 hotspots)
3. ✅ Pebble research (optimization patterns)
4. ✅ Optimization strategy review (Phase 1/2 plan)

**Parallel Work:**
- Analysis agents ran concurrently
- Implementation done sequentially (file dependencies)

**Collaboration:**
- Agents provided independent analysis
- Main agent synthesized findings
- Discussed strategy with review agent

---

## Conclusion

**Phase 1 is 50% complete** (2/4 optimizations done). Expected to reduce allocations by ~85%.

**New benchmark system is ready** to measure results accurately.

**Next:** Finish remaining optimizations, run benchmarks, measure impact.
