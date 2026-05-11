# OneLeaf Benchmarks

Run Date: `2026-05-10 19:45:52`

Fresh run after switching segment compression to `LZ4Dict4KB`.

## Throughput

### Put

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw | `1037` | `1677` | `12` |
| OneLeaf | LZ4Dict4KB | `906.4` | `2017` | `12` |
| OneLeaf | LZ4Dict4KB + async WAL | `1130` | `2226` | `12` |
| Pebble | NoSync | `846.8` | `45` | `2` |
| BadgerDB | NoSync | `8977` | `2181` | `41` |

### Read

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw, 64K keys | `1336` | `3340` | `5` |
| OneLeaf | LZ4Dict4KB, 64K keys | `1278` | `3554` | `5` |
| Pebble | 5 MB block cache, 64K keys | `4062` | `120` | `3` |
| Pebble | no block cache, 64K keys | `4036` | `120` | `3` |
| BadgerDB | NoSync, 64K keys | `1874` | `1192` | `20` |

### Inc

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw | `259` | `93` | `5` |
| OneLeaf | LZ4Dict4KB | `257` | `93` | `5` |
| OneLeaf | LZ4Dict4KB + async WAL | `319` | `544` | `5` |

### Mixed Put/Get

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | raw | `1588` | `4136` | `12` |
| OneLeaf | LZ4Dict4KB | `2321` | `4618` | `12` |
| Pebble | NoSync | `2748` | `80` | `3` |
| BadgerDB | NoSync | `5417` | `1841` | `32` |

### Open + Get

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| OneLeaf | open DB + get, 64K keys | `223647` | `517366` | `5312` |
| Pebble | open DB + get, no block cache | `26270470` | `362227` | `840` |


## Latency

Columns always ordered as `p50`, `p95`, `p99`, `avg`, `max`.

### Write

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `333ns` | `875ns` | `2.166µs` | `445ns` | `210.292µs` |
| OneLeaf | LZ4Dict4KB | `292ns` | `625ns` | `1.458µs` | `367ns` | `220.041µs` |
| OneLeaf | LZ4Dict4KB + async WAL | `292ns` | `583ns` | `1.375µs` | `442ns` | `5.113875ms` |
| Pebble | NoSync | `416ns` | `541ns` | `1.375µs` | `739ns` | `8.318584ms` |

### Read

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw, 64K keys | `1.25µs` | `1.917µs` | `3.583µs` | `1.238µs` | `240.291µs` |
| OneLeaf | LZ4Dict4KB, 64K keys | `1.334µs` | `1.959µs` | `4µs` | `1.32µs` | `214.334µs` |
| Pebble | 5 MB block cache, 64K keys | `3.959µs` | `4.375µs` | `5.208µs` | `3.985µs` | `157.917µs` |
| Pebble | no block cache, 64K keys | `4.042µs` | `4.584µs` | `5.459µs` | `4.104µs` | `85.209µs` |

### Rate Limiter

Workload: `95% Inc`, `5% config update`.

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `250ns` | `459ns` | `625ns` | `297ns` | `140.292µs` |
| OneLeaf | LZ4Dict4KB | `250ns` | `458ns` | `583ns` | `284ns` | `34.667µs` |
| OneLeaf | LZ4Dict4KB + async WAL | `250ns` | `459ns` | `1µs` | `303ns` | `1.025167ms` |
| Pebble | NoSync | `792ns` | `1.083µs` | `1.625µs` | `1.032µs` | `8.961333ms` |


## Real go-ycsb Core Latency

These rows use `github.com/pingcap/go-ycsb v1.0.3` Core workload generation.

### Load

Workload: `100% insert`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `375ns` | `792ns` | `1.917µs` | `450ns` | `55.167µs` |
| OneLeaf LZ4Dict4KB | `375ns` | `709ns` | `1.583µs` | `478ns` | `568.708µs` |
| OneLeaf raw + async WAL | `375ns` | `1.125µs` | `2.25µs` | `742ns` | `4.0935ms` |
| OneLeaf LZ4Dict4KB + async WAL | `375ns` | `1.083µs` | `2.125µs` | `622ns` | `3.346584ms` |
| Pebble NoSync | `417ns` | `625ns` | `1.5µs` | `1.031µs` | `8.021417ms` |

### A

Workload: `50% read, 50% update`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `1.041µs` | `2.125µs` | `3.459µs` | `1.191µs` | `90.416µs` |
| OneLeaf LZ4Dict4KB | `1.042µs` | `2.166µs` | `3.416µs` | `1.216µs` | `228.459µs` |
| OneLeaf raw + async WAL | `958ns` | `2.125µs` | `3.042µs` | `1.201µs` | `1.863916ms` |
| OneLeaf LZ4Dict4KB + async WAL | `959ns` | `2.208µs` | `3.459µs` | `1.31µs` | `2.607792ms` |
| Pebble NoSync | `750ns` | `6.417µs` | `9.417µs` | `2.412µs` | `9.018167ms` |

### B

Workload: `95% read, 5% update`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `916ns` | `1.625µs` | `2.709µs` | `1.012µs` | `83.417µs` |
| OneLeaf LZ4Dict4KB | `875ns` | `1.5µs` | `2.292µs` | `963ns` | `120.834µs` |
| OneLeaf raw + async WAL | `916ns` | `1.834µs` | `2.833µs` | `1.056µs` | `1.200833ms` |
| OneLeaf LZ4Dict4KB + async WAL | `875ns` | `1.833µs` | `2.667µs` | `1.125µs` | `4.28425ms` |
| Pebble NoSync | `4.125µs` | `4.791µs` | `5.25µs` | `2.788µs` | `178.792µs` |

### C

Workload: `100% read`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `625ns` | `1.209µs` | `1.583µs` | `640ns` | `295.75µs` |
| OneLeaf LZ4Dict4KB | `666ns` | `1.25µs` | `1.75µs` | `653ns` | `110.542µs` |
| OneLeaf raw + async WAL | `708ns` | `1.5µs` | `2.167µs` | `751ns` | `795.167µs` |
| OneLeaf LZ4Dict4KB + async WAL | `708ns` | `1.5µs` | `2.417µs` | `743ns` | `240.125µs` |
| Pebble NoSync | `4.125µs` | `4.75µs` | `5.583µs` | `3.023µs` | `155.5µs` |

### F

Workload: `read-modify-write`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `666ns` | `1.5µs` | `2.125µs` | `765ns` | `249.917µs` |
| OneLeaf LZ4Dict4KB | `666ns` | `1.5µs` | `2.333µs` | `768ns` | `800.167µs` |
| OneLeaf raw + async WAL | `667ns` | `1.542µs` | `2.667µs` | `884ns` | `3.700542ms` |
| OneLeaf LZ4Dict4KB + async WAL | `625ns` | `1.5µs` | `2.334µs` | `812ns` | `1.766667ms` |
| Pebble NoSync | `625ns` | `8.75µs` | `9.709µs` | `2.439µs` | `10.500666ms` |

## 4KB Compression Probe

Input corpus: `3577` blocks, avg raw block `3940 B`.

### Ratio

| Codec | Avg compressed | Ratio | Saved |
|---|---:|---:|---:|
| LZ4Dict4KB | `1474 B` | `0.374` | `62.6%` |
| LZ4Dict8KB | `1450 B` | `0.368` | `63.2%` |
| LZ4Dict16KB | `1425 B` | `0.362` | `63.8%` |
| SnappyNoDict | `1650 B` | `0.419` | `58.1%` |
| S2Dict4KB | `1477 B` | `0.375` | `62.5%` |
| S2Dict8KB | `1435 B` | `0.364` | `63.6%` |
| S2Dict16KB | `1394 B` | `0.354` | `64.6%` |

### Compression Speed

Average of 5 benchmark runs.

| Codec | ns/op avg | ns/op range | MB/s avg | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| LZ4Dict4KB | `6984` | `6930-7037` | `564.2` | `0` | `0` |
| LZ4Dict8KB | `7557.2` | `7510-7623` | `521.3` | `0` | `0` |
| LZ4Dict16KB | `7434` | `7410-7449` | `530` | `0` | `0` |
| SnappyNoDict | `3032.2` | `3009-3048` | `1299.5` | `0` | `0` |
| S2Dict4KB | `5056` | `5034-5096` | `779.3` | `0` | `0` |
| S2Dict8KB | `5565.6` | `5532-5586` | `707.9` | `0` | `0` |
| S2Dict16KB | `5590.6` | `5563-5611` | `704.8` | `0` | `0` |

### Decompression Speed

Average of 5 benchmark runs.

| Codec | ns/op avg | ns/op range | MB/s avg | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| LZ4Dict4KB | `974.5` | `968.1-977.4` | `4043.3` | `0` | `0` |
| LZ4Dict8KB | `1112.4` | `1105-1123` | `3542.2` | `0` | `0` |
| LZ4Dict16KB | `1254.2` | `1249-1262` | `3141.2` | `0` | `0` |
| SnappyNoDict | `1205.6` | `1203-1211` | `3268.2` | `0` | `0` |
| S2Dict4KB | `1673.8` | `1663-1689` | `2353.8` | `0` | `0` |
| S2Dict8KB | `1756.4` | `1752-1764` | `2243` | `0` | `0` |
| S2Dict16KB | `1838.8` | `1830-1850` | `2143` | `0` | `0` |

## Notes

- OneLeaf and Pebble both use a `5 MB` cache budget.
- `compressed` means `LZ4Dict4KB` trained from `internal/compression/kv_dict_samples_50k.jsonl`.
- Async WAL returns after appending to the in-memory WAL buffer.
- Pebble rows use `NoSync`; they are a low-latency baseline, not durable-per-write.
- go-ycsb workload E is not included because OneLeaf does not expose DB-level scan/range reads yet.
- RocksDB benchmarks are behind `-tags rocksdb` and were not included in this run.

## Memory Allocations Analysis

### Get Operation (340 B/op, 5 allocs/op)

Per-operation breakdown:
- Cache operations (get/set): ~109 B
- Block decompression: ~87 B
- Disk read (readFullAt): ~73 B
- Reader overhead: ~71 B

Main sources:
1. `cache.get` - copying from cache
2. `Reader.Get` - block reading
3. `Decompress` - LZ4 decompression
4. `readFullAt` - disk I/O
5. `cache.set` - cache storage

### Put Operation (1849 B/op, 12 allocs/op)

Per-operation breakdown (~154 B per allocation):
1. WAL buffer management
2. Data copying (skiplist ownership)
3. Block serialization
4. LZ4 compression
5. Value encoding
6. Block building
7. Skiplist node creation
8. Key formatting
9-12. Various overhead

Main sources:
- `wal.swapActive` (21.90%) - WAL buffers
- `bytes.Clone` (13.87%) - data ownership
- `Block.MarshalBinary` (12.82%) - serialization
- `Dictionary.CompressInto` (11.90%) - compression
- `value.EncodeBytes` (10.33%) - encoding
- `Block.AddUnsafe` (10.26%) - block building
- `skiplist.newNode` (4.36%) - index nodes

### Inc Operation (93 B/op, 5 allocs/op)

Per-operation breakdown (~19 B per allocation):
1. Skiplist node lookup
2. Value encoding (int64 varint)
3. Op coalescing
4. Atomic pointer swap
5. Counter update

Main sources:
- `skiplist.Apply` - node traversal and update
- `value.EncodeInt64` - varint encoding
- `coalesceToNew` - operation merging
- `ops.NewInc` - counter increment
- Atomic operations overhead
