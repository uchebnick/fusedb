# OneLeaf Benchmarks

Environment:

- Date: 2026-05-09
- Machine: Apple M4, darwin/arm64
- Package: `fusedb/benchmarks/oneleafdb`
- Commands:
  - `go test ./benchmarks/oneleafdb -run '^$' -bench . -benchmem -benchtime=2s -count=3`
  - `FUSEDB_LATENCY_PROBE=1 go test ./benchmarks/oneleafdb -run 'TestOneLeafPebble(WriteLatency10MB|ReadLatency64K|RateLimiterLatency10MB)' -count=1 -v`

## Throughput

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| OneLeaf Put raw | `979.9-1266` | `1553-1846` | `12-13` |
| OneLeaf Put compressed | `1316-1434` | `1676-1735` | `11-12` |
| OneLeaf Put compressed + WAL | `4898-5031` | `3596-3697` | `16` |
| OneLeaf Get 64K raw | `2489-2595` | `9885` | `56` |
| OneLeaf Get 64K compressed | `5186-5304` | `10125` | `57` |
| OneLeaf Mixed Put/Get raw | `2623-2682` | `7131-7229` | `37-38` |
| OneLeaf Mixed Put/Get compressed | `5707-5802` | `7082-7090` | `37` |
| OneLeaf Open+Get 64K | `254029-267210` | `522227-522238` | `5359` |
| Pebble Put NoSync | `862.3-912.7` | `45` | `2` |
| Pebble Get 64K cached | `800.6-809.8` | `120` | `3` |
| Pebble Get 64K no block cache | `4181-4367` | `120` | `3` |
| Pebble Open+Get 64K no block cache | `25575759-25836304` | `366061-370813` | `838-841` |
| Pebble Mixed Put/Get NoSync | `910.3-927.6` | `80` | `3` |

## Latency

| Workload | avg | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf Put raw | `462ns` | `333ns` | `834ns` | `2.25µs` | `996.625µs` |
| OneLeaf Put compressed | `347ns` | `291ns` | `542ns` | `1.542µs` | `491.916µs` |
| OneLeaf Put compressed + WAL | `4.096µs` | `3.833µs` | `5.584µs` | `8.375µs` | `4.032209ms` |
| Pebble Put NoSync | `841ns` | `417ns` | `500ns` | `1.334µs` | `17.763708ms` |
| OneLeaf Read 64K raw | `2.486µs` | `1.916µs` | `3.333µs` | `20.125µs` | `282.125µs` |
| OneLeaf Read 64K compressed | `4.926µs` | `4.584µs` | `5.708µs` | `8.375µs` | `886.333µs` |
| Pebble Read 64K cached | `808ns` | `750ns` | `875ns` | `1.959µs` | `240.791µs` |
| Pebble Read 64K no block cache | `4.176µs` | `4.125µs` | `4.417µs` | `5.208µs` | `250.833µs` |
| OneLeaf Rate limiter raw | `392ns` | `209ns` | `541ns` | `1.208µs` | `4.929166ms` |
| OneLeaf Rate limiter compressed | `246ns` | `209ns` | `375ns` | `542ns` | `22.125µs` |
| OneLeaf Rate limiter compressed + WAL | `4.015µs` | `3.792µs` | `5.208µs` | `8.667µs` | `644.375µs` |
| Pebble Rate limiter | `1.117µs` | `792ns` | `1µs` | `1.791µs` | `8.8235ms` |

## Notes

- OneLeaf compressed write latency stays close to raw because compression happens in background merge work.
- OneLeaf compressed read is slower because read path decompresses and decodes a whole block per point lookup.
- WAL is simple cmd-only append WAL. No recovery, checksum, or fsync policy yet.
- RocksDB benchmark file exists behind `-tags rocksdb`, but was not run here because `github.com/linxGnu/grocksdb` is not installed in `go.mod`.
