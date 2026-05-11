# OneLeaf vs Pebble Scalability Analysis
проверь что все наши изменения соответствуют docs/OPTIMIZATION_SUMMARY.md если нет то просто скажи мне
**Goal:** Find the crossover point where Pebble's LSM architecture becomes more efficient than OneLeaf's single-segment design.


## Hypothesis

**OneLeaf advantages (small scale):**
- Single segment = one disk seek
- No compaction overhead
- Simple index structure
- Fast for < 100K keys

**Pebble advantages (large scale):**
- Multi-level LSM handles write amplification
- Bloom filters prevent unnecessary reads
- Compaction keeps data organized
- Scales to billions of keys

**Expected crossover:** 500K - 1M keys

---

## Test Methodology

### Dataset Sizes
- 10K keys × 128B = 1.28 MB
- 50K keys × 128B = 6.4 MB
- 100K keys × 128B = 12.8 MB
- 500K keys × 128B = 64 MB
- 1M keys × 128B = 128 MB

### Metrics
1. **Read latency** (ns/op)
2. **Allocations** (allocs/op, B/op)
3. **Merge count** (write amplification)
4. **Segment file size**

### OneLeaf Configuration
- Threshold: 10MB (triggers merge every ~80K keys)
- Cache: 5MB
- Value size: 128B (matches YCSB)

---

## Expected Results

### Read Performance

| Keys | OneLeaf (est) | Pebble (baseline) | Winner |
|------|---------------|-------------------|--------|
| 10K  | ~800 ns       | ~4000 ns          | OneLeaf 5x |
| 50K  | ~1000 ns      | ~4000 ns          | OneLeaf 4x |
| 100K | ~1200 ns      | ~4000 ns          | OneLeaf 3x |
| 500K | ~2000 ns?     | ~4000 ns          | OneLeaf 2x? |
| 1M   | ~3500 ns?     | ~4000 ns          | Even? |

**Hypothesis:** OneLeaf stays competitive up to 1M keys because:
- Single segment still fits in OS page cache
- Index is small enough to stay in memory
- No multi-level lookup overhead

---

## Write Amplification

OneLeaf's weakness: **no compaction between segments**

At 1M keys with 10MB threshold:
- ~8 merges triggered
- Each merge rewrites entire buffer
- Total write amplification: ~8x

Pebble's advantage:
- Incremental compaction
- Only rewrites overlapping ranges
- Better write amplification at scale

---

## Where OneLeaf Breaks Down

### Problem 1: Multiple Segments
If threshold is too small:
- 1M keys / 10MB threshold = ~8 segments
- Each read checks 8 bloom filters
- Performance degrades linearly

### Problem 2: Large Segment Size
If threshold is too large:
- Single 128MB segment
- Merge takes seconds
- Write latency spikes

### Problem 3: No Range Scans
OneLeaf doesn't support scans:
- Can't iterate over key ranges
- Pebble's LSM tree excels at this

---

## Pebble's Crossover Point

**Prediction:** Pebble wins when:
1. **Dataset > 1M keys** - multi-level LSM pays off
2. **Write-heavy workload** - compaction reduces amplification
3. **Range scans needed** - OneLeaf can't do this
4. **Hot/cold data** - Pebble's levels separate them

---

## Real-World Use Cases

### OneLeaf Sweet Spot
- Rate limiters (10K-100K buckets)
- Feature flags (1K-10K flags)
- Session storage (10K-100K sessions)
- Small caches (< 1M entries)

### Pebble Sweet Spot
- Full databases (1M+ rows)
- Time-series data (billions of points)
- Log storage (continuous writes)
- Any workload needing scans

---

## Running the Benchmarks

```bash
# Quick test (10K-100K)
go test ./internal/oneleafdb -bench=BenchmarkScalability -benchtime=1s

# Full test (10K-1M)
go test ./internal/oneleafdb -bench=BenchmarkScalability -benchtime=3s -timeout=30m

# With compression
go test ./internal/oneleafdb -bench=BenchmarkScalabilityWithCompression -benchtime=1s
```

---

## Results

(To be filled after benchmarks complete)

### Read Latency

| Keys | OneLeaf (ns/op) | vs Pebble 4000ns | Speedup |
|------|-----------------|------------------|---------|
| 10K  | TBD             | TBD              | TBD     |
| 50K  | TBD             | TBD              | TBD     |
| 100K | TBD             | TBD              | TBD     |
| 500K | TBD             | TBD              | TBD     |
| 1M   | TBD             | TBD              | TBD     |

### Allocations

| Keys | B/op | allocs/op | Notes |
|------|------|-----------|-------|
| 10K  | TBD  | TBD       | TBD   |
| 50K  | TBD  | TBD       | TBD   |
| 100K | TBD  | TBD       | TBD   |
| 500K | TBD  | TBD       | TBD   |
| 1M   | TBD  | TBD       | TBD   |

### Analysis

(To be filled after results)

---

## Conclusion

(To be determined based on benchmark results)
