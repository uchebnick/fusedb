# OneLeaf Benchmarks

Fresh run after block-view lookup, OneLeaf value cache, and async WAL changes.

## Environment

| Field | Value |
|---|---|
| Date | `2026-05-10` |
| Machine | `Apple M4` |
| OS/Arch | `darwin/arm64` |
| Package | `fusedb/benchmarks/oneleafdb` |
| Value size | `128 B` |
| Seeded read keys | `64K` |
| OneLeaf cache | `5 MB` value cache |
| Pebble cache | `5 MB` block cache |
| Auto-merge threshold | `5 MB` throughput benches, `10 MB` latency probes |
| WAL mode | async group commit, `200µs` interval |

## Commands

```bash
GOCACHE=$PWD/.gocache go test ./benchmarks/oneleafdb \
  -run '^$' -bench . -benchmem -benchtime=2s -count=3
```

```bash
GOCACHE=$PWD/.gocache FUSEDB_LATENCY_PROBE=1 go test ./benchmarks/oneleafdb \
  -run 'TestOneLeafPebble(WriteLatency10MB|ReadLatency64K|RateLimiterLatency10MB)' \
  -count=1 -v
```

## Throughput

### Put

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw | `821.5-1037` | `1573-1826` | `12-13` |
| OneLeaf | compressed | `1199-1234` | `1583-1652` | `11` |
| OneLeaf | compressed + async WAL | `1049-1066` | `1537-1673` | `10` |
| Pebble | NoSync | `857.1-872.1` | `45` | `2` |

### Read

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw, 64K keys | `1182-1196` | `3291-3296` | `5` |
| OneLeaf | compressed, 64K keys | `3225-3242` | `3534-3539` | `6` |
| Pebble | 5 MB block cache, 64K keys | `4021-4117` | `120` | `3` |
| Pebble | no block cache, 64K keys | `4023-4068` | `120` | `3` |

### Mixed Put/Get

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw | `1465-1541` | `4115-4237` | `12` |
| OneLeaf | compressed | `5341-5372` | `4540-4592` | `12` |
| Pebble | NoSync | `2844-2897` | `80` | `3` |

### Open + Get

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | open reader + get, 64K keys | `244360-255390` | `517320-517383` | `5312` |
| Pebble | open DB + get, no block cache | `26847385-26967667` | `370085-376218` | `839-842` |

## Latency

Columns are always ordered as `p50`, `p95`, `p99`, `avg`, `max`.

### Write

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `333ns` | `791ns` | `2.042µs` | `429ns` | `769.333µs` |
| OneLeaf | compressed | `292ns` | `542ns` | `1.291µs` | `346ns` | `124.542µs` |
| OneLeaf | compressed + async WAL | `292ns` | `500ns` | `1.042µs` | `378ns` | `3.803625ms` |
| Pebble | NoSync | `417ns` | `500ns` | `1.292µs` | `764ns` | `12.446542ms` |

### Read

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw, 64K keys | `1.333µs` | `1.958µs` | `4.166µs` | `1.318µs` | `237.25µs` |
| OneLeaf | compressed, 64K keys | `3.959µs` | `5.125µs` | `7.583µs` | `3.507µs` | `166.292µs` |
| Pebble | 5 MB block cache, 64K keys | `4.084µs` | `4.417µs` | `5.167µs` | `4.13µs` | `136.833µs` |
| Pebble | no block cache, 64K keys | `4.083µs` | `4.375µs` | `5.166µs` | `4.125µs` | `90.792µs` |

### Rate Limiter

Workload: `95% Inc`, `5% config update`.

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `250ns` | `417ns` | `542ns` | `263ns` | `116.917µs` |
| OneLeaf | compressed | `250ns` | `458ns` | `542ns` | `272ns` | `7.959µs` |
| OneLeaf | compressed + async WAL | `250ns` | `417ns` | `625ns` | `278ns` | `59.25µs` |
| Pebble | NoSync | `792ns` | `1µs` | `1.209µs` | `997ns` | `8.147583ms` |

## Notes

- OneLeaf and Pebble both use a `5 MB` cache budget in this run.
- OneLeaf read is now much faster than previous runs because point reads use the OneLeaf value cache plus encoded block lookup.
- Pebble `5 MB` block cache is not enough to materially improve this 64K-key read set; cached and no-cache rows are close.
- Async WAL returns after appending to the in-memory WAL buffer. Durability happens on group commit or close.
- Pebble rows use `NoSync`; they are a low-latency baseline, not durable-per-write.
- RocksDB benchmarks are behind `-tags rocksdb` and were not included in this run.
