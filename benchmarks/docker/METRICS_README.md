# FuseDB Benchmark System with Grafana Metrics

This system runs **pure Go benchmarks** inside containers and exports metrics to Prometheus/Grafana.

## Key Features

✅ **Pure Go benchmarks** (not HTTP requests)  
✅ **Linear RPS ramp-up** (0 → target over N minutes)  
✅ **Real-time Grafana dashboards**  
✅ **Allocation tracking** (allocs/op, bytes/op)  
✅ **Latency percentiles** (p50, p95, p99)  

## Quick Start

```bash
cd benchmarks/docker

# Start all services
docker-compose -f docker-compose.metrics.yml up -d

# View logs
docker-compose -f docker-compose.metrics.yml logs -f

# Open Grafana
open http://localhost:3000
# Login: admin / admin

# Stop all services
docker-compose -f docker-compose.metrics.yml down -v
```

## What Gets Measured

### Benchmark Metrics (NOT HTTP metrics!)

- `benchmark_operations_total{operation, status}` - Total ops (get/put, success/error/miss)
- `benchmark_latency_nanoseconds{operation}` - Latency histogram (get/put)
- `benchmark_allocations_per_op{operation}` - Memory allocations per operation
- `benchmark_bytes_allocated_per_op{operation}` - Bytes allocated per operation
- `benchmark_current_rps` - Current RPS (linearly increasing)
- `benchmark_target_rps` - Target RPS
- `benchmark_memory_used_bytes` - Current memory usage
- `benchmark_goroutines` - Number of goroutines

## Configuration

Edit `docker-compose.metrics.yml`:

```yaml
environment:
  - RECORD_COUNT=100000      # Number of keys to load
  - VALUE_SIZE=1024          # Value size in bytes
  - TARGET_RPS=10000         # Target requests per second
  - RAMP_UP_MINUTES=5        # Linear ramp-up duration
  - TEST_MINUTES=10          # Test duration after ramp-up
  - READ_RATIO=0.8           # 80% reads, 20% writes
  - CACHE_SIZE_MB=64         # Cache size
  - GOMAXPROCS=2             # CPU cores
```

## Linear RPS Ramp-Up

The benchmark starts at **0 RPS** and linearly increases to **TARGET_RPS** over **RAMP_UP_MINUTES**.

Example with `TARGET_RPS=10000` and `RAMP_UP_MINUTES=5`:

```
Time    RPS
0:00    0
1:00    2000
2:00    4000
3:00    6000
4:00    8000
5:00    10000  ← steady state begins
15:00   10000  ← test ends
```

This allows you to see how performance degrades under increasing load.

## Grafana Dashboard

The dashboard shows:

### Row 1: Throughput
- **Current RPS** (both engines)
- **Target RPS** (linear ramp-up line)
- **Operations Total** (success/error/miss)

### Row 2: Latency
- **p50 Latency** (median)
- **p95 Latency** (95th percentile)
- **p99 Latency** (99th percentile)

### Row 3: Allocations
- **Allocations per Op** (get/put)
- **Bytes Allocated per Op** (get/put)

### Row 4: System
- **Memory Usage** (heap)
- **Goroutines**

## Example Queries

### Current RPS
```promql
benchmark_current_rps{engine="oneleaf"}
benchmark_current_rps{engine="pebble"}
```

### p95 Latency (microseconds)
```promql
histogram_quantile(0.95, 
  rate(benchmark_latency_nanoseconds_bucket{operation="get"}[1m])
) / 1000
```

### Allocations per Get
```promql
rate(benchmark_allocations_per_op_sum{operation="get"}[1m]) 
/ 
rate(benchmark_allocations_per_op_count{operation="get"}[1m])
```

### Bytes per Get
```promql
rate(benchmark_bytes_allocated_per_op_sum{operation="get"}[1m]) 
/ 
rate(benchmark_bytes_allocated_per_op_count{operation="get"}[1m])
```

## Viewing Metrics

### Prometheus
- OneLeaf: http://localhost:9090/metrics
- Pebble: http://localhost:9091/metrics
- Prometheus UI: http://localhost:9092

### Grafana
- Dashboard: http://localhost:3000
- Login: admin / admin

## Architecture

```
┌─────────────────────────────────────┐
│  Docker Container (2 CPU, 512MB)   │
│                                     │
│  ┌───────────────────────────────┐ │
│  │   benchmark-runner-metrics    │ │
│  │                               │ │
│  │   • Linear RPS ramp-up        │ │
│  │   • Direct DB operations      │ │
│  │   • Track allocations         │ │
│  │   • Export Prometheus metrics │ │
│  └───────────────────────────────┘ │
│           │                         │
│           ▼                         │
│  ┌───────────────────────────────┐ │
│  │   OneLeaf or Pebble DB        │ │
│  └───────────────────────────────┘ │
└─────────────────────────────────────┘
         │
         ▼ :9090/metrics
    ┌──────────┐
    │Prometheus│
    └────┬─────┘
         │
         ▼
    ┌──────────┐
    │ Grafana  │
    └──────────┘
```

## Differences from Old System

### Old System (HTTP-based)
```
Load Generator → HTTP → JSON → DB → JSON → HTTP → Metrics
```
- Measured: HTTP throughput (1000 req/s)
- Included: JSON parsing, network overhead
- Missing: Allocation metrics

### New System (Pure Go benchmarks)
```
Benchmark Runner → DB → Metrics
```
- Measured: Pure DB operations
- Direct: Go API calls
- Tracked: allocs/op, bytes/op, latency

## Expected Results

After Phase 1 optimizations:

| Metric | OneLeaf (Before) | OneLeaf (After) | Pebble | Target |
|--------|------------------|-----------------|--------|--------|
| Bytes/op | 3340-3554 | ~300-500 | 120 | <500 |
| Allocs/op | 5 | 3-4 | 3 | ≤4 |
| p50 Latency | ~1.3µs | ? | ~4µs | <2µs |

## Troubleshooting

**No metrics in Grafana:**
```bash
# Check if services are running
docker-compose -f docker-compose.metrics.yml ps

# Check Prometheus targets
open http://localhost:9092/targets

# Check logs
docker-compose -f docker-compose.metrics.yml logs prometheus
```

**High memory usage:**
- Reduce `RECORD_COUNT`
- Reduce `TARGET_RPS`
- Increase Docker memory limit

**Benchmark finishes too quickly:**
- Increase `TEST_MINUTES`
- Increase `RAMP_UP_MINUTES`

## Clean Up

```bash
# Stop and remove everything
docker-compose -f docker-compose.metrics.yml down -v

# Remove images
docker-compose -f docker-compose.metrics.yml down --rmi all -v
```

## Next Steps

1. **Run the benchmark**
   ```bash
   docker-compose -f docker-compose.metrics.yml up -d
   ```

2. **Watch Grafana** (http://localhost:3000)
   - See RPS ramp up linearly
   - Compare OneLeaf vs Pebble latency
   - Check allocation metrics

3. **Analyze results**
   - Did Phase 1 optimizations reduce allocations?
   - Is OneLeaf still faster than Pebble?
   - Where is the bottleneck now?

4. **Iterate**
   - Adjust `TARGET_RPS` to find breaking point
   - Try different `READ_RATIO` values
   - Scale up `RECORD_COUNT` for larger datasets

## Why This Approach?

**Old system measured:**
```
HTTP request → JSON decode → DB operation → JSON encode → HTTP response
```

**New system measures:**
```
DB operation
```

**Plus:**
- Linear load increase (find breaking point)
- Real-time visualization (Grafana)
- Allocation tracking (find memory leaks)
- Production-like constraints (Docker limits)

**Much more accurate for database performance comparison.**
