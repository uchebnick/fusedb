# FuseDB Container Benchmarks

This directory contains a **pure Go benchmark system** that runs inside Docker containers, measuring actual database performance without HTTP overhead.

## What's Different from the Old System?

**Old System (HTTP-based):**
- HTTP servers exposing Get/Put endpoints
- Load generator making 1000 req/s HTTP calls
- Measured HTTP throughput (includes JSON parsing, network overhead)
- Prometheus + Grafana for metrics

**New System (Pure Go benchmarks):**
- Direct Go benchmark code running in containers
- Measures raw database operations (Get/Put)
- Reports ns/op, allocs/op, bytes/op
- No HTTP, no JSON, no network overhead
- **This is what you want for fair DB comparisons**

## Quick Start

```bash
cd benchmarks/docker
./run-benchmarks.sh
```

This will:
1. Build the benchmark image
2. Run 6 benchmarks (OneLeaf + Pebble × 3 workloads)
3. Save results to `results/*.json`
4. Generate comparison report in `results/COMPARISON.md`

## Benchmark Configurations

All benchmarks use:
- **100,000 records** (100K keys)
- **500,000 operations** (500K ops)
- **1KB value size**
- **64MB cache**
- **Single-threaded** (workers=1)
- **2 CPUs, 512MB RAM** (Docker limits)

### Workloads

1. **read-only**: 100% Get operations
2. **write-only**: 100% Put operations  
3. **mixed-50-50**: 50% Get, 50% Put

## Results Format

Each benchmark produces a JSON file with:

```json
{
  "engine": "oneleaf",
  "workload": "read-only",
  "total_ops": 500000,
  "duration": "5.2s",
  "ops_per_sec": 96153.8,
  "avg_latency_ns": 10400,
  "p50_latency_ns": 9800,
  "p95_latency_ns": 15200,
  "p99_latency_ns": 22100,
  "max_latency_ns": 125000,
  "allocs_per_op": 5.2,
  "bytes_per_op": 3400,
  "memory_used_mb": 45.3
}
```

## Key Metrics

- **ops_per_sec**: Throughput (higher is better)
- **p50/p95/p99_latency_ns**: Latency percentiles (lower is better)
- **allocs_per_op**: Memory allocations per operation (lower is better)
- **bytes_per_op**: Bytes allocated per operation (lower is better)

## Running Individual Benchmarks

```bash
# Run just OneLeaf read benchmark
docker-compose -f docker-compose.benchmark.yml run --rm oneleaf-read

# Run just Pebble write benchmark
docker-compose -f docker-compose.benchmark.yml run --rm pebble-write
```

## Customizing Benchmarks

Edit config files in `configs/`:
- `oneleaf-read.json`
- `oneleaf-write.json`
- `oneleaf-mixed.json`
- `pebble-read.json`
- `pebble-write.json`
- `pebble-mixed.json`

Example config:
```json
{
  "engine": "oneleaf",
  "record_count": 1000000,     // 1M records
  "operation_count": 5000000,  // 5M ops
  "value_size": 4096,          // 4KB values
  "cache_size_mb": 128,        // 128MB cache
  "workers": 1,
  "workload": "read-only"
}
```

## Architecture

```
┌─────────────────────────────────────┐
│  Docker Container (2 CPU, 512MB)   │
│                                     │
│  ┌───────────────────────────────┐ │
│  │   benchmark-runner            │ │
│  │                               │ │
│  │   1. Load records             │ │
│  │   2. Run workload             │ │
│  │   3. Measure latency          │ │
│  │   4. Track allocations        │ │
│  │   5. Output JSON results      │ │
│  └───────────────────────────────┘ │
│           │                         │
│           ▼                         │
│  ┌───────────────────────────────┐ │
│  │   OneLeaf or Pebble DB        │ │
│  │   (direct Go API calls)       │ │
│  └───────────────────────────────┘ │
└─────────────────────────────────────┘
         │
         ▼
    results/*.json
```

## Comparing with Local Benchmarks

To compare container results with local `go test -bench`:

```bash
# Run local benchmarks
cd ../../
go test -bench=BenchmarkGet -benchmem ./internal/oneleafdb/

# Compare with container results
cat benchmarks/docker/results/oneleaf-read.json
```

Container benchmarks should be **slower** due to:
- Docker overhead
- CPU/memory limits
- No CPU pinning

But the **relative performance** (OneLeaf vs Pebble) should be similar.

## Troubleshooting

**Container fails to start:**
```bash
# Check logs
docker-compose -f docker-compose.benchmark.yml logs oneleaf-read

# Run interactively
docker-compose -f docker-compose.benchmark.yml run --rm oneleaf-read sh
```

**Out of memory:**
- Reduce `record_count` in config
- Increase Docker memory limit in `docker-compose.benchmark.yml`

**Slow benchmarks:**
- Reduce `operation_count` for faster runs
- Use `record_count: 10000` and `operation_count: 50000` for quick tests

## Clean Up

```bash
# Remove all containers and volumes
docker-compose -f docker-compose.benchmark.yml down -v

# Remove results
rm -rf results/
```

## Next Steps

After running benchmarks:

1. **Check allocation metrics** - Are we still at 3000+ bytes/op?
2. **Compare latencies** - Is OneLeaf faster than Pebble?
3. **Profile if needed** - Add pprof to benchmark-runner
4. **Scale up** - Try 1M records, 10M operations
5. **Multi-threaded** - Set `workers: 8` in configs

## Why This Approach?

The old HTTP-based system was measuring:
```
HTTP request → JSON decode → DB operation → JSON encode → HTTP response
```

This new system measures:
```
DB operation
```

**Much cleaner, much more accurate.**

## Old HTTP-Based System

The old system is still available in:
- `docker-compose.yml` (HTTP servers + Prometheus + Grafana)
- `cmd/benchmark-server/` (HTTP server)
- `cmd/loadgen/` (HTTP load generator)

Use it if you need:
- Real-time monitoring with Grafana
- Long-running stress tests
- HTTP endpoint testing

But for **pure database performance comparison**, use the new benchmark system.
