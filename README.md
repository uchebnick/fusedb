# FuseDB

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat)](./LICENSE)

Embedded key-value database for hot mutable state.

[Русский](./README.ru.md) | [Library Usage](./LIBRARY.md)

## Performance

Apple M4, `darwin/arm64`, 128 B values, 64K seeded keys, 5 MB cache on both
engines. Pebble runs in `NoSync` mode, which is a low-latency baseline rather
than a durable-per-write one. Range over 3 runs.

| Operation | FuseDB | Pebble (NoSync) |
|---|---:|---:|
| Read (64K keys) | `1.06-1.13µs` | `4.25-4.39µs` |
| Write | `1.03µs` | `0.83-0.88µs` |
| Open + first read | `7.4-7.8ms` | `26.3-26.6ms` |
| Inc (atomic counter) | `407-482ns` | — |

Pebble is faster on writes. FuseDB trades that for read latency and startup
time, which is the workload it targets: hot state that is read constantly and
must come back quickly after a restart.

### Write amplification

Bytes actually written to disk per byte of user data, merging after every 4000
keys. `one leaf` is the whole keyspace in a single segment; `splitting` lets
leaves divide at 1 MiB.

| Keys | User MB | One leaf | Splitting | Leaves |
|---:|---:|---:|---:|---:|
| 20K | `2.82` | `3.33x` | `1.78x` | 3 |
| 40K | `5.65` | `6.11x` | `2.01x` | 6 |
| 80K | `11.29` | `11.66x` | `2.10x` | 12 |
| 160K | `22.58` | `22.76x` | `2.07x` | 23 |

A single segment has to be rewritten in full on every merge, so its
amplification grows with the dataset. Splitting keeps merges local to one leaf,
and amplification stays flat as the data grows 8x.

Reproduce: `go test ./benchmarks/oneleafdb -run TestWriteAmplification -v`

## Install

```bash
go get github.com/uchebnick/fusedb
```

## Usage

```go
import "github.com/uchebnick/fusedb/pkg/fusedb"

db, _ := fusedb.Open(fusedb.Options{
    Dir:       "/tmp/db",
    CacheSize: 5 << 20,
    MergeSize: 5 << 20,
})
defer db.Close()

db.Put([]byte("key"), []byte("value"))
db.Inc([]byte("counter"), 1)
value, found, _ := db.Get([]byte("key"))
```

## Architecture

- Key-ordered tree of leaves, each owning one segment and its own buffer
- Lock-free skiplist buffer
- Immutable segments with LZ4 compression
- Write-ahead log with crash recovery and group commit

Leaves split when they outgrow `MaxLeafSize`, so a merge rewrites one leaf
instead of the whole database. That is what keeps write amplification flat as
the dataset grows.

Details: [ARCHITECTURE.md](./ARCHITECTURE.md)

## Status

Experimental. Not production-ready.
