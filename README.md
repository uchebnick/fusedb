# FuseDB

[![Language: Go](https://img.shields.io/badge/language-Go-00ADD8)](#)
[![Status: In development](https://img.shields.io/badge/status-in_development-blue)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

FuseDB is an embedded key-value engine for small hot mutable state. It is built
around leaf-local mutation buffers, immutable segments, and local merge instead
of global LSM-style compaction.

The target workloads are rate limits, counters, sessions, quotas, and other
metadata/state-serving paths where point reads and frequent point updates are
more important than large scans.

## Design

FuseDB routes keys into leaves. A leaf owns recent mutations in memory and a
materialized immutable segment on disk.

Core pieces:

- `internal/skiplist`: lock-free ordered mutation index for `Put`, `Delete`,
  and `Inc` operations.
- `internal/leaf`: leaf-local buffer and immutable operation snapshots for
  future merge work.
- `internal/segment`: immutable segment writer/reader with blocks, index,
  bloom filter, footer, iterators, and crash-safe finalize/abort.
- `internal/compression`: Zstd dictionary compression, persistent dictionary
  files, LRU registry, and pretraining helpers.
- `internal/disk`: filesystem abstraction used by segment and compression
  persistence.

## Read And Write Model

Writes become typed operations and enter the leaf buffer first. Existing
buffered operations may coalesce, for example repeated increments on the same
key.

Reads check the buffer before the segment:

1. `Delete` means not found.
2. `Put` returns the buffered value.
3. `Inc` is resolved against the materialized base value.
4. Missing buffer key falls through to the immutable segment.

Merge freezes the current buffer operations into an immutable snapshot, lets new
writes continue in a fresh buffer, and builds a new segment from:

```text
old segment entries + frozen buffer operations -> new immutable segment
```

## Current Capabilities

- Concurrent skiplist mutation buffer with zero-copy and safe-copy APIs.
- Buffer freeze into read-only operation snapshots for merge.
- Segment writer/reader with point lookup and ordered iterator.
- Segment-wide compression mode with persistent dictionary registry.
- Leaf buffer snapshots let merge read frozen operations while new writes
  continue in a fresh buffer.
- In-memory and OS-backed filesystem implementations for tests and runtime.

## Not A Fit

FuseDB is not intended for:

- large blobs;
- analytics-heavy scans;
- distributed storage;
- SQL or full OLTP workloads.

## Development

Useful checks:

```sh
go test ./internal/skiplist
go test -race ./internal/skiplist
go test ./internal/segment
go test ./internal/leaf
```

`go test ./...` may include unfinished package stubs while modules are still
being wired together.

## Roadmap

- Implement leaf-level read/write orchestration over buffer + segment.
- Implement buffer/segment merge, including resolver-backed `Inc`.
- Add WAL and recovery metadata for segment replacement.
- Add scheduler policies for merge, split, and IO budgeting.
- Add leaf split plus routing-tree persistence.
- Add end-to-end latency and write-amplification benchmarks.

## Docs

- [Architecture](./docs/architecture.md)
- [Compression](./docs/compression.md)
- [Skiplist module](./internal/skiplist/README.md)
