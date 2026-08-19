# FuseDB Benchmarks

Two runs are recorded here.

**Throughput and write amplification (`2026-08-04`)** were measured against the
current engine: a key-ordered tree of leaves with a recovering write-ahead log.

**Latency, go-ycsb and compression** below still come from the `2026-05-11` run
against the previous single-segment engine. They have not been re-measured and
are kept for reference only; the sections are marked where they begin.

## Environment

| Field | Value |
|---|---|
| Date | `2026-08-04` (throughput), `2026-05-11` (latency, ycsb, compression) |
| Machine | `Apple M4` |
| OS/Arch | `darwin/arm64` |
| Module/package | `github.com/uchebnick/fusedb/benchmarks/oneleafdb` |
| Value size | `128 B` |
| Seeded read keys | `64K` |
| OneLeaf cache | `5 MB` value cache |
| Pebble cache | `5 MB` block cache |
| Auto-merge threshold | `5 MB` throughput benches, `10 MB` latency probes |
| Compression | `LZ4Dict4KB`, trained from `internal/compression/kv_dict_samples_50k.jsonl` |
| WAL mode | async group commit, `200us` interval |

## Commands

```bash
(cd benchmarks && GOCACHE=$PWD/.gocache go test ./oneleafdb -run '^$' \
  -bench 'OneLeafPutAutoMerge5MB$|OneLeafIncAutoMerge5MB$|OneLeafGet64K$|OneLeafMixedPutGet5MB$|OneLeafOpenGet64K$|PebblePutNoSync$|PebbleGet64K$|PebbleOpenGet64KNoBlockCache$' \
  -benchmem -benchtime=1s -count=3)
```

```bash
(cd benchmarks && GOCACHE=$PWD/.gocache go test ./oneleafdb \
  -run TestWriteAmplification -v -count=1)
```

```bash
(cd benchmarks && GOCACHE=$PWD/.gocache FUSEDB_LATENCY_PROBE=1 go test ./oneleafdb \
  -run 'TestOneLeafPebble(WriteLatency10MB|ReadLatency64K|RateLimiterLatency10MB)' \
  -count=1 -v)
```

```bash
(cd benchmarks && GOCACHE=$PWD/.gocache FUSEDB_REAL_YCSB=1 go test ./oneleafdb \
  -run TestGoYCSBCoreLatency -count=1 -v)
```

```bash
(cd benchmarks && GOCACHE=$PWD/.gocache go test ./oneleafdb \
  -run TestCompression4KProbeRatio -count=1 -v)
```

```bash
(cd benchmarks && GOCACHE=$PWD/.gocache go test ./oneleafdb \
  -run '^$' -bench 'BenchmarkCompression4K(Compress|Decompress)' \
  -benchmem -benchtime=2s -count=5)
```

## Throughput (`2026-08-04`, leaf tree)

Ranges span 3 runs. Both engines use `LZ4Dict4KB` and a `5 MB` cache; FuseDB
runs with the recovering WAL in async group-commit mode. The BadgerDB and
`raw`/no-WAL variants from the previous run were not re-measured and are
dropped rather than carried forward next to fresh numbers.

### Put

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| FuseDB | LZ4Dict4KB + async WAL | `1029-1032` | `1481-1586` | `9` |
| Pebble | NoSync | `831.0-878.9` | `45-46` | `2` |

### Read

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| FuseDB | LZ4Dict4KB, 64K keys | `1064-1129` | `411-412` | `5` |
| Pebble | 5 MB block cache, 64K keys | `4246-4389` | `120` | `3` |

### Inc

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| FuseDB | LZ4Dict4KB + async WAL | `407.5-481.5` | `239-252` | `6` |

### Mixed Put/Get

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| FuseDB | LZ4Dict4KB + async WAL | `1340-1358` | `1510-1629` | `10` |

### Open + Get

Both rows now close the database, reopen the directory from scratch, and read.

The previous run reported `233669-254753 ns` for this row, but that benchmark
never reopened anything: it installed a segment reader into a live database, so
it measured neither the manifest load nor the segment open. The number below is
roughly 30x larger because it is the first one that measures a cold start.

| Engine | Mode | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| FuseDB | reopen dir + get, 64K keys | `7354926-7764032` | `805263-805498` | `5343` |
| Pebble | open DB + get, no block cache | `26310078-26639042` | `388774-392132` | `844-852` |

## Write Amplification (`2026-08-04`)

Bytes counted at the filesystem `Write`/`WriteAt` call, so every rewrite is
included; summing surviving file sizes would only report live data. Merge runs
after every 4000 keys, which models a database kept continuously up to date.
`one leaf` holds the whole keyspace in one segment, reproducing the previous
engine; `splitting` lets leaves divide at 1 MiB.

| Keys | User MB | One leaf written MB | One leaf amp | Splitting written MB | Splitting amp | Leaves |
|---:|---:|---:|---:|---:|---:|---:|
| 20000 | `2.82` | `9.40` | `3.33x` | `5.01` | `1.78x` | 3 |
| 40000 | `5.65` | `34.47` | `6.11x` | `11.37` | `2.01x` | 6 |
| 80000 | `11.29` | `131.61` | `11.66x` | `23.68` | `2.10x` | 12 |
| 160000 | `22.58` | `513.88` | `22.76x` | `46.65` | `2.07x` | 23 |

A single segment must be rewritten in full on every merge, so its amplification
tracks the dataset size. Splitting keeps each merge inside one leaf, and
amplification stays flat while the data grows 8x.

## Reference: previous run (`2026-05-11`, single segment)

Everything below was measured against the earlier single-segment engine and has
not been re-run. Treat it as historical.

## Latency

Columns always ordered as `p50`, `p95`, `p99`, `avg`, `max`.

### Write

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `333ns` | `792ns` | `2.041us` | `441ns` | `309.042us` |
| OneLeaf | LZ4Dict4KB | `292ns` | `666ns` | `1.417us` | `376ns` | `944.375us` |
| OneLeaf | LZ4Dict4KB + async WAL | `292ns` | `500ns` | `1.167us` | `375ns` | `3.165584ms` |
| Pebble | NoSync | `417ns` | `583ns` | `1.417us` | `788ns` | `11.796792ms` |

### Read

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw, 64K keys | `1.334us` | `1.958us` | `3.625us` | `1.31us` | `132.542us` |
| OneLeaf | LZ4Dict4KB, 64K keys | `1.417us` | `2us` | `4.125us` | `1.368us` | `173.041us` |
| Pebble | 5 MB block cache, 64K keys | `4.084us` | `4.459us` | `5.084us` | `4.132us` | `146.584us` |
| Pebble | no block cache, 64K keys | `4.083us` | `4.458us` | `5.167us` | `4.146us` | `198us` |

### Rate Limiter

Workload: `95% Inc`, `5% config update`.

| Engine | Mode | p50 | p95 | p99 | avg | max |
|---|---|---:|---:|---:|---:|---:|
| OneLeaf | raw | `250ns` | `417ns` | `583ns` | `268ns` | `142.166us` |
| OneLeaf | LZ4Dict4KB | `250ns` | `458ns` | `542ns` | `271ns` | `16.917us` |
| OneLeaf | LZ4Dict4KB + async WAL | `250ns` | `417ns` | `583ns` | `278ns` | `55.25us` |
| Pebble | NoSync | `792ns` | `1us` | `1.25us` | `1.02us` | `8.673958ms` |

## Real go-ycsb Core Latency

These rows use `github.com/pingcap/go-ycsb v1.0.3` Core workload generation,
not a hand-written ratio probe. The adapters store one YCSB field per KV record,
use the same `5 MB` cache budget, and report measured DB adapter calls.

Columns: `p50`, `p95`, `p99`, `avg`, `max`.

### Load

Workload: `100% insert`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `375ns` | `792ns` | `1.917us` | `450ns` | `55.167us` |
| OneLeaf LZ4Dict4KB | `375ns` | `709ns` | `1.583us` | `478ns` | `568.708us` |
| OneLeaf raw + async WAL | `375ns` | `1.125us` | `2.25us` | `742ns` | `4.0935ms` |
| OneLeaf LZ4Dict4KB + async WAL | `375ns` | `1.083us` | `2.125us` | `622ns` | `3.346584ms` |
| Pebble NoSync | `417ns` | `625ns` | `1.5us` | `1.031us` | `8.021417ms` |

### Workload A

Workload: `50% read`, `50% update`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `1.041us` | `2.125us` | `3.459us` | `1.191us` | `90.416us` |
| OneLeaf LZ4Dict4KB | `1.042us` | `2.166us` | `3.416us` | `1.216us` | `228.459us` |
| OneLeaf raw + async WAL | `958ns` | `2.125us` | `3.042us` | `1.201us` | `1.863916ms` |
| OneLeaf LZ4Dict4KB + async WAL | `959ns` | `2.208us` | `3.459us` | `1.31us` | `2.607792ms` |
| Pebble NoSync | `750ns` | `6.417us` | `9.417us` | `2.412us` | `9.018167ms` |

### Workload B

Workload: `95% read`, `5% update`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `916ns` | `1.625us` | `2.709us` | `1.012us` | `83.417us` |
| OneLeaf LZ4Dict4KB | `875ns` | `1.5us` | `2.292us` | `963ns` | `120.834us` |
| OneLeaf raw + async WAL | `916ns` | `1.834us` | `2.833us` | `1.056us` | `1.200833ms` |
| OneLeaf LZ4Dict4KB + async WAL | `875ns` | `1.833us` | `2.667us` | `1.125us` | `4.28425ms` |
| Pebble NoSync | `4.125us` | `4.791us` | `5.25us` | `2.788us` | `178.792us` |

### Workload C

Workload: `100% read`.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `625ns` | `1.209us` | `1.583us` | `640ns` | `295.75us` |
| OneLeaf LZ4Dict4KB | `666ns` | `1.25us` | `1.75us` | `653ns` | `110.542us` |
| OneLeaf raw + async WAL | `708ns` | `1.5us` | `2.167us` | `751ns` | `795.167us` |
| OneLeaf LZ4Dict4KB + async WAL | `708ns` | `1.5us` | `2.417us` | `743ns` | `240.125us` |
| Pebble NoSync | `4.125us` | `4.75us` | `5.583us` | `3.023us` | `155.5us` |

### Workload F

Workload: read-modify-write.

| Engine | p50 | p95 | p99 | avg | max |
|---|---:|---:|---:|---:|---:|
| OneLeaf raw | `666ns` | `1.5us` | `2.125us` | `765ns` | `249.917us` |
| OneLeaf LZ4Dict4KB | `666ns` | `1.5us` | `2.333us` | `768ns` | `800.167us` |
| OneLeaf raw + async WAL | `667ns` | `1.542us` | `2.667us` | `884ns` | `3.700542ms` |
| OneLeaf LZ4Dict4KB + async WAL | `625ns` | `1.5us` | `2.334us` | `812ns` | `1.766667ms` |
| Pebble NoSync | `625ns` | `8.75us` | `9.709us` | `2.439us` | `10.500666ms` |

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

Average of 5 passes.

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

Average of 5 passes.

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
- LZ4Dict4KB adds ~`0.5us` to p99 read latency in this run, much lower than previous zstd-dict runs.
- Async WAL returns after appending to the in-memory WAL buffer. Durability happens on group commit or close.
- Pebble rows use `NoSync`; they are a low-latency baseline, not durable-per-write.
- go-ycsb workload E is not included because OneLeaf does not expose DB-level scan/range reads yet.
- go-ycsb workload F is measured as underlying DB calls, so its table mixes each generated read-modify-write operation's read and update calls.
- RocksDB benchmarks are behind `-tags rocksdb` and were not included in this run.

## Memory Optimization (2026-05-11)

Eliminated closure allocations and optimized value encoding:

| Metric | Before | After | Improvement |
|---|---:|---:|---:|
| RSS under load | ~800 MB | 97-126 MB | 6.3-8.2x |
| Heap allocated | - | 62.8 MB | - |
| Get B/op | 3504-3510 | 334-335 | 10.5x |
| Mixed B/op | 5547-5890 | 2549-2648 | 2.2x |
| Put allocs/op | 12-13 | 10-13 | -2 allocs |

Changes:
- Replaced closure-based buffer release with `PooledBuffer` and `PooledDecompressBuffer` structs
- Explicit `Release()` calls instead of defer for lower overhead
- Lock-free decompression using `atomic.Bool`
- Removed `bytes.Clone` in skiplist (keys are immutable)
- Moved value type tag to end of encoded data (eliminates one allocation)
