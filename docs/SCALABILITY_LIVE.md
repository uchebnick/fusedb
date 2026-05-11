# FuseDB Scalability Investigation - Live Results

**Started:** 2026-05-11 07:10 UTC  
**Goal:** Find where Pebble becomes faster than OneLeaf

---

## Quick Results So Far

### 10K keys (1.28 MB)
- **97 ns/op** - cache hit
- **161 B/op, 2 allocs/op**
- Single segment, instant merge
- **vs Pebble 4000 ns/op: 41x faster** ✅

---

## What We're Testing

| Size | Keys | Data Size | Expected Merges | Prediction |
|------|------|-----------|-----------------|------------|
| 10K  | 10,000 | 1.28 MB | 1 | OneLeaf wins 40x |
| 50K  | 50,000 | 6.4 MB | 1 | OneLeaf wins 10x |
| 100K | 100,000 | 12.8 MB | 2 | OneLeaf wins 5x |
| 500K | 500,000 | 64 MB | 7 | OneLeaf wins 2x? |
| 1M   | 1,000,000 | 128 MB | 13 | **Crossover point?** |

---

## Why OneLeaf is Fast (Small Scale)

1. **Single segment lookup**
   - One bloom filter check
   - One binary search
   - One disk read
   - Total: ~100-200ns after cache warm-up

2. **Simple architecture**
   - No multi-level LSM overhead
   - No compaction between levels
   - Entire index fits in memory

3. **Aggressive caching**
   - 5MB cache
   - High hit rate on small datasets
   - Cache hit = ~100ns

---

## Where OneLeaf Will Slow Down
ты сделал buffer pooling? и добавь чтобы все графики в графане автоматически создовались и сделай их очень детальными gc ram cpu все
  latency p50 p95 p99
### At 500K keys:
- Multiple segments (7 merges)
- Each read checks multiple bloom filters
- Performance degrades linearly with segment count

### At 1M keys:
- 13 segments created
- 13 bloom filter checks per read
- Index size grows
- **Expected: 1000-2000 ns/op**

### Pebble's advantage kicks in:
- Multi-level LSM keeps data organized
- Compaction prevents segment explosion
- Bloom filters at each level
- **Stable: ~4000 ns/op regardless of size**

---

## The Crossover Formula

OneLeaf read time ≈ `base_latency + (num_segments × bloom_check_time)`

- base_latency: ~100ns (cache hit)
- bloom_check_time: ~50-100ns per segment
- num_segments: data_size / threshold

At 1M keys with 10MB threshold:
- num_segments ≈ 13
- read_time ≈ 100 + (13 × 75) ≈ 1075ns

**Still faster than Pebble's 4000ns!**

---

## Real Crossover Point

Pebble wins when:

1. **Dataset > 5M keys**
   - OneLeaf creates 50+ segments
   - Bloom filter overhead dominates
   - Read time > 4000ns

2. **Write-heavy workload**
   - OneLeaf rewrites entire buffer on merge
   - Pebble does incremental compaction
   - Write amplification matters

3. **Range scans needed**
   - OneLeaf doesn't support scans
   - Pebble's LSM tree excels at ranges

---

## Live Results

(Updating as benchmarks complete...)

### Read Latency (ns/op)

| Keys | OneLeaf | vs Pebble 4000ns | Speedup | Status |
|------|---------|------------------|---------|--------|
| 10K  | 97      | 4000             | 41x     | ✅ Done |
| 50K  | ?       | 4000             | ?       | 🔄 Running |
| 100K | ?       | 4000             | ?       | ⏳ Pending |
| 500K | ?       | 4000             | ?       | ⏳ Pending |
| 1M   | ?       | 4000             | ?       | ⏳ Pending |

### Allocations

| Keys | B/op | allocs/op | Notes |
|------|------|-----------|-------|
| 10K  | 161  | 2         | Excellent |
| 50K  | ?    | ?         | Running |
| 100K | ?    | ?         | Pending |
| 500K | ?    | ?         | Pending |
| 1M   | ?    | ?         | Pending |

---

## Hypothesis

**OneLeaf will stay faster than Pebble even at 1M keys** because:
- Modern SSDs are fast (~100µs random read)
- OS page cache keeps hot data in RAM
- 13 segments × 75ns bloom check = 975ns overhead
- Total: ~1000-1500ns vs Pebble's 4000ns

**Real crossover: 5M+ keys** where segment count becomes unmanageable.

---

## Next Steps

1. Wait for benchmark results
2. Analyze actual vs predicted performance
3. Test with 5M keys if needed
4. Document the sweet spot for OneLeaf

---

## Conclusion

(To be filled after benchmarks complete)
