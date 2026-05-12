# 🚀 Quick Start Guide

## Start the benchmark

```bash
cd /Users/uchebnick/projects/fusedb

# Build and start all services
docker-compose up --build

# Or run in background
docker-compose up --build -d
```

## Access dashboards

- **Grafana:** http://localhost:3000 (admin/admin)
- **Prometheus:** http://localhost:9092
- **OneLeaf Server:** http://localhost:8080
- **Pebble Server:** http://localhost:8081

## What's running

1. **OneLeaf Server** - Port 8080, metrics on 9090
2. **Pebble Server** - Port 8081, metrics on 9091  
3. **Prometheus** - Collecting metrics every 5s
4. **Grafana** - Real-time dashboard
5. **Load Generator** - Sending 1000 RPS to both

## Resource limits (equal for both)

- CPU: 2 cores max
- Memory: 512MB max
- Cache: 64MB
- GOMAXPROCS: 2

## What you'll see

The Grafana dashboard shows:
- **RPS comparison** (requests per second)
- **Latency comparison** (p50, p95, p99)
- **CPU usage** (%)
- **Memory usage** (heap, sys)
- **Goroutines** (count)
- **GC duration** (nanoseconds)

## Stop the benchmark

```bash
# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Troubleshooting

```bash
# View logs
docker-compose logs -f

# View specific service
docker-compose logs -f oneleaf
docker-compose logs -f pebble
docker-compose logs -f loadgen

# Restart a service
docker-compose restart oneleaf
```

## Expected results

Based on previous benchmarks with equal constraints:

| Metric | OneLeaf | Pebble |
|--------|---------|--------|
| p50 Latency | ~1500 ns | ~4000 ns |
| p95 Latency | ~3000 ns | ~6000 ns |
| CPU Usage | ~80% | ~90% |
| Memory | ~300MB | ~350MB |

The dashboard will show real-time comparison.
