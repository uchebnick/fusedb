# OneLeaf Benchmarks

Run Date: `2026-05-30 11:40:55`

Fresh run after switching segment compression to `LZ4Dict4KB`.

## Throughput

### Put

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw | `740.7-838.8` | `1586-1679` | `11` |
| OneLeaf | LZ4Dict4KB | `795.7-843.1` | `1987-2107` | `13-14` |
| OneLeaf | LZ4Dict4KB + async WAL | `812.1-842.7` | `1855-1904` | `10-11` |
| Pebble | NoSync | `818.1-834.9` | `45` | `2` |

### Read

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw, 64K keys | `1274-1278` | `3229` | `5` |
| OneLeaf | LZ4Dict4KB, 64K keys | `1012-1061` | `334-335` | `5` |
| Pebble | 5 MB block cache, 64K keys | `4032-4074` | `120` | `3` |
| Pebble | no block cache, 64K keys | `3987-4083` | `120` | `3` |

### Mixed Put/Get

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw | `1475-1585` | `4096-4148` | `11` |
| OneLeaf | LZ4Dict4KB | `1321-1406` | `2552-2732` | `16-17` |
| Pebble | NoSync | `2802-2836` | `80` | `3` |

### Open + Get

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | open DB + get, 64K keys | `261339-276851` | `648564-648948` | `5318` |
| Pebble | open DB + get, no block cache | `26691794-26870280` | `370768-373968` | `838-840` |


## Latency

Columns always ordered as `p50`, `p95`, `p99`, `avg`, `max`.

### Write

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `333ns` | `833ns` | `2.125µs` | `433ns` | `357.708µs` |
| OneLeaf | LZ4Dict4KB | `292ns` | `625ns` | `1.333µs` | `364ns` | `426.417µs` |
| OneLeaf | LZ4Dict4KB + async WAL | `292ns` | `583ns` | `1.375µs` | `407ns` | `2.928166ms` |
| Pebble | NoSync | `417ns` | `583ns` | `1.291µs` | `784ns` | `14.057ms` |

### Read

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw, 64K keys | `1.334µs` | `2µs` | `4.084µs` | `1.335µs` | `160.792µs` |
| OneLeaf | LZ4Dict4KB, 64K keys | `1.25µs` | `1.5µs` | `1.75µs` | `1.106µs` | `171.5µs` |
| Pebble | 5 MB block cache, 64K keys | `4.125µs` | `4.459µs` | `5.208µs` | `4.17µs` | `198.167µs` |
| Pebble | no block cache, 64K keys | `4µs` | `4.375µs` | `5.125µs` | `4.058µs` | `122.459µs` |

### Rate Limiter

Workload: `95% Inc`, `5% config update`.

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `250ns` | `458ns` | `666ns` | `299ns` | `90.25µs` |
| OneLeaf | LZ4Dict4KB | `250ns` | `458ns` | `583ns` | `289ns` | `16.5µs` |
| OneLeaf | LZ4Dict4KB + async WAL | `250ns` | `417ns` | `625ns` | `285ns` | `69.875µs` |
| Pebble | NoSync | `792ns` | `1.042µs` | `1.458µs` | `1.031µs` | `8.427792ms` |


## Real go-ycsb Core Latency

These rows use `github.com/pingcap/go-ycsb v1.0.3` Core workload generation.

### Load

Workload: `100% insert`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `334ns` | `750ns` | `1.917µs` | `435ns` | `41.083µs` |
| OneLeaf LZ4Dict4KB | `334ns` | `667ns` | `1.334µs` | `428ns` | `117.667µs` |
| OneLeaf raw + async WAL | `333ns` | `792ns` | `2µs` | `640ns` | `3.603458ms` |
| OneLeaf LZ4Dict4KB + async WAL | `333ns` | `833ns` | `2.041µs` | `647ns` | `3.55775ms` |
| Pebble NoSync | `417ns` | `583ns` | `1.375µs` | `980ns` | `7.574541ms` |

### A

Workload: `50% read, 50% update`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `916ns` | `1.916µs` | `2.792µs` | `1.033µs` | `77.5µs` |
| OneLeaf LZ4Dict4KB | `916ns` | `1.916µs` | `2.75µs` | `1.038µs` | `83.291µs` |
| OneLeaf raw + async WAL | `833ns` | `1.916µs` | `2.75µs` | `1.048µs` | `2.354416ms` |
| OneLeaf LZ4Dict4KB + async WAL | `833ns` | `1.875µs` | `2.625µs` | `1.057µs` | `2.915708ms` |
| Pebble NoSync | `750ns` | `6.041µs` | `9.375µs` | `2.349µs` | `9.035875ms` |

### B

Workload: `95% read, 5% update`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `834ns` | `1.5µs` | `2.291µs` | `947ns` | `218.417µs` |
| OneLeaf LZ4Dict4KB | `875ns` | `1.542µs` | `2.417µs` | `968ns` | `114.667µs` |
| OneLeaf raw + async WAL | `833ns` | `1.667µs` | `2.583µs` | `1.009µs` | `2.879042ms` |
| OneLeaf LZ4Dict4KB + async WAL | `833ns` | `1.666µs` | `2.5µs` | `1.043µs` | `3.914083ms` |
| Pebble NoSync | `4.125µs` | `4.833µs` | `5.583µs` | `2.867µs` | `180.083µs` |

### C

Workload: `100% read`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `584ns` | `1.25µs` | `1.875µs` | `636ns` | `159.083µs` |
| OneLeaf LZ4Dict4KB | `583ns` | `1.208µs` | `1.583µs` | `598ns` | `153.417µs` |
| OneLeaf raw + async WAL | `625ns` | `1.292µs` | `1.916µs` | `649ns` | `77.833µs` |
| OneLeaf LZ4Dict4KB + async WAL | `625ns` | `1.292µs` | `2.042µs` | `658ns` | `303.333µs` |
| Pebble NoSync | `4.042µs` | `4.625µs` | `5.375µs` | `2.864µs` | `194.334µs` |

### F

Workload: `read-modify-write`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `625ns` | `1.459µs` | `2.125µs` | `737ns` | `345.75µs` |
| OneLeaf LZ4Dict4KB | `625ns` | `1.458µs` | `2.125µs` | `732ns` | `95.667µs` |
| OneLeaf raw + async WAL | `584ns` | `1.417µs` | `2.167µs` | `851ns` | `3.907208ms` |
| OneLeaf LZ4Dict4KB + async WAL | `583ns` | `1.416µs` | `2.25µs` | `765ns` | `3.154625ms` |
| Pebble NoSync | `625ns` | `8.708µs` | `9.667µs` | `2.392µs` | `10.341625ms` |

## 4KB Compression Probe

Input corpus: `3577` blocks, avg raw block `3940 B`.

### Ratio

| Codec | Avg compressed | Ratio | Saved |
|---|---:|---:|---:|
| LZ4Dict4KB | `1475 B` | `0.375` | `62.5%` |
| LZ4Dict8KB | `1449 B` | `0.368` | `63.2%` |
| LZ4Dict16KB | `1420 B` | `0.361` | `63.9%` |
| SnappyNoDict | `1650 B` | `0.419` | `58.1%` |
| S2Dict4KB | `1470 B` | `0.373` | `62.7%` |
| S2Dict8KB | `1431 B` | `0.363` | `63.7%` |
| S2Dict16KB | `1390 B` | `0.353` | `64.7%` |

### Compression Speed

Average of 5 benchmark runs.

| Codec | ns/op avg | ns/op range | MB/s avg | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| LZ4Dict4KB | `7366.6` | `7215-7507` | `535` | `0` | `0` |
| LZ4Dict8KB | `7890.2` | `7800-8117` | `499.5` | `0` | `0` |
| LZ4Dict16KB | `7873.4` | `7764-8009` | `500.5` | `0` | `0` |
| SnappyNoDict | `3110.4` | `3091-3140` | `1266.8` | `0` | `0` |
| S2Dict4KB | `5116` | `5076-5149` | `770.2` | `0` | `0` |
| S2Dict8KB | `5605.8` | `5546-5668` | `702.9` | `0` | `0` |
| S2Dict16KB | `5594` | `5578-5633` | `704.3` | `0` | `0` |

### Decompression Speed

Average of 5 benchmark runs.

| Codec | ns/op avg | ns/op range | MB/s avg | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| LZ4Dict4KB | `1030.2` | `1026-1033` | `3824.4` | `0` | `0` |
| LZ4Dict8KB | `1171` | `1166-1175` | `3364` | `0` | `0` |
| LZ4Dict16KB | `1324.8` | `1317-1336` | `2974.5` | `0` | `0` |
| SnappyNoDict | `1226.2` | `1220-1240` | `3213.2` | `0` | `0` |
| S2Dict4KB | `1925.2` | `1740-2186` | `2069` | `0` | `0` |
| S2Dict8KB | `2218.4` | `1850-3199` | `1852.1` | `0` | `0` |
| S2Dict16KB | `2035.2` | `1927-2250` | `1942.3` | `0` | `0` |

## Notes

- OneLeaf and Pebble both use a `5 MB` cache budget.
- `compressed` means `LZ4Dict4KB` trained from `internal/compression/kv_dict_samples_50k.jsonl`.
- Async WAL returns after appending to the in-memory WAL buffer.
- Pebble rows use `NoSync`; they are a low-latency baseline, not durable-per-write.
- go-ycsb workload E is not included because OneLeaf does not expose DB-level scan/range reads yet.
- RocksDB benchmarks are behind `-tags rocksdb` and were not included in this run.
