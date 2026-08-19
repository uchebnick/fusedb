# FuseDB

[![CI](https://github.com/uchebnick/fusedb/actions/workflows/ci.yml/badge.svg)](https://github.com/uchebnick/fusedb/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/uchebnick/fusedb/pkg/fusedb.svg)](https://pkg.go.dev/github.com/uchebnick/fusedb/pkg/fusedb)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)

An experimental embedded key-value engine for read-heavy, frequently updated
state. FuseDB partitions the keyspace into independently merged leaves, keeping
the amount of data rewritten by one merge bounded as the database grows.

> [!WARNING]
> FuseDB is a research project. Its API and on-disk format are not stable, and
> it is not ready for production data.

[Русский](./README.ru.md) · [Library guide](./LIBRARY.md) ·
[Architecture](./ARCHITECTURE.md) · [Benchmarks](./benchmarks/RESULTS.md) ·
[Scheduler](./docs/scheduler.md) · [Operations](./docs/operations.md) ·
[Qualification](./docs/qualification.md) · [Monitoring](./monitoring/prometheus/README.md) ·
[Disk format](./docs/format.md) · [Release engineering](./docs/release.md) ·
[Likes/tickets pilot](./docs/pilot-likes-tickets.md) ·
[Contributing](./CONTRIBUTING.md)

## Why FuseDB?

A conventional single-segment engine eventually rewrites the whole dataset on
every merge. FuseDB instead routes each key to a leaf with its own mutable
buffer and immutable segment. Leaves merge and split independently.

- Point reads without a global read lock
- Local merges with a configurable leaf-size bound
- Atomic `int64` increments that survive restart and replay exactly once
- Conditional multi-key write batches and idempotent event application in one
  WAL record for atomic membership plus materialized-counter updates
- WAL recovery, checksummed records, and atomic manifest replacement
- Bounded decoders and write limits that reject hostile lengths before
  allocation, plus same-key WAL/live ordering
- Terminal WAL failure fencing: failed or uncertain persistence poisons the
  handle until reopen instead of allowing an unsafe checkpoint
- Checksummed format negotiation with early rejection of newer or unknown
  required features and atomic manifest v2/v3 migration
- Exclusive process-crash-safe ownership lock for each database directory
- Startup reconciliation of unreferenced completed and temporary segment files
- Subprocess crash matrix that kills the engine at WAL, segment, manifest, and
  WAL-rotation commit boundaries and checks exact counter recovery
- Full `Verify` scan for manifest, WAL, block CRC, key-range, bloom, and LZ4
  dictionary integrity
- Online point-in-time backup and restore with streaming checksums and
  post-restore semantic verification
- Adaptive merge admission that protects foreground p95/p99
- Checksummed UTC time-of-week scheduler history that survives restart and backup
- Adaptive per-leaf-group LZ4 training on real merged blocks, with held-out
  evaluation, cooperative cancellation, and durable publication
- Reference-safe dictionary lifecycle GC, bounded and preemptible through the
  same scheduler as merge and backup
- Automatic CPU/RAM capacity detection and learned effective disk throughput,
  with explicit overrides for every resource budget
- Optional low-cardinality Prometheus collector, readiness contract, recording
  rules, and production-oriented alert examples
- Bounded phased qualification runner with p95/p99 gates, maintenance-debt
  evidence, full verification, close/reopen, and exact-counter validation
- Immutable compressed segments with bloom filters and block indexes
- Safe byte-slice ownership: writes do not retain caller memory and reads return
  copies

The current scope is deliberately narrow: point operations and atomic write
batches on one local database. There are no general read/write transactions,
transactional read snapshots, range scans, replication, or long-term
cross-release storage compatibility guarantee yet.

## Install

FuseDB requires Go 1.25.13+, a C toolchain, and the native LZ4 development
library. On Debian/Ubuntu use `apt install liblz4-dev`; on macOS use
`brew install lz4`. Builds with `CGO_ENABLED=0` compile, but dictionary codecs
and adaptive dictionary training return `fusedb.ErrCGODisabled`.

```bash
go get github.com/uchebnick/fusedb/pkg/fusedb@latest
```

## Quick start

```go
package main

import (
    "log"

    "github.com/uchebnick/fusedb/pkg/fusedb"
)

func main() {
    db, err := fusedb.Open(fusedb.Options{Dir: "./data"})
    if err != nil {
        log.Fatal(err)
    }

    if err := db.Put([]byte("user:42"), []byte("Ada")); err != nil {
        log.Fatal(err)
    }

    value, found, err := db.Get([]byte("user:42"))
    if err != nil {
        log.Fatal(err)
    }
    if found {
        log.Printf("user:42 = %s", value)
    }

    if err := db.Close(); err != nil {
        log.Fatal(err)
    }
}
```

`DB` is safe for concurrent use. `Put`, `Delete`, and `Inc` reject empty keys;
`Get` treats an empty key as a miss. See the [library guide](./LIBRARY.md) for
configuration and durability details.

## How it works

```text
Put / Delete / Inc
        │
        ├── append to WAL
        ▼
  route key to leaf ──────► mutable skiplist buffer
                                  │
                                  │ merge at threshold
                                  ▼
Get ─► leaf ─► buffer ─────► immutable segment
                              bloom → index → block
```

The ordered leaf set is published copy-on-write. A leaf owns a half-open key
range, an in-memory mutation buffer, and at most one immutable segment. When a
leaf grows beyond `MaxLeafSize`, its next merge emits multiple leaves and
publishes them atomically. The [architecture guide](./ARCHITECTURE.md) documents
the concurrency and recovery invariants.

## Durability modes

| Setting | A write returns after | Crash window |
|---|---|---|
| `WALSyncWrites: false` (default) | WAL append and in-memory apply | Up to one group-commit interval (200 µs by default) |
| `WALSyncWrites: true` | Coalesced WAL group sync and in-memory apply | No acknowledged WAL record is intentionally left unsynced |

`Close` and `Merge` checkpoint buffered operations into segments. Every leaf
persists its own exact replay watermark, so independently merged leaves cannot
re-apply a non-idempotent `Inc`; the manifest also keeps the minimum global
watermark used for safe WAL truncation.

## Monitoring

`DB.Health`, `DB.Metrics`, `DB.Stats`, and `DB.Format` expose immutable
in-process snapshots. The optional `pkg/fusedb/prometheus` package turns them
into a custom collector without global registration or persistent I/O during a
scrape. A runnable `/metrics` and `/readyz` server plus recording and alert
rules live in the [Prometheus operations bundle](./monitoring/prometheus/README.md).

## Benchmark snapshot

Apple M4, `darwin/arm64`, 128-byte values, 64K seeded keys, 5 MiB cache.
Pebble uses `NoSync`, so this is a latency comparison rather than equivalent
per-write durability. Ranges cover three runs.

| Operation | FuseDB | Pebble (`NoSync`) |
|---|---:|---:|
| Point read | `1.06–1.13 µs` | `4.25–4.39 µs` |
| Write | `1.03 µs` | `0.83–0.88 µs` |
| Open + first read | `7.4–7.8 ms` | `26.3–26.6 ms` |
| Atomic increment | `407–482 ns` | — |

These numbers describe one machine and workload, not a general ranking. Full
commands, datasets, and write-amplification results live in
[benchmarks/RESULTS.md](./benchmarks/RESULTS.md).

## Repository map

| Path | Responsibility |
|---|---|
| `pkg/fusedb` | Supported public API |
| `pkg/fusedb/prometheus` | Optional low-cardinality Prometheus collector |
| `internal/tree` | Key routing, leaf set, splits, and manifest updates |
| `internal/leaf` | Per-leaf buffer, reader, merge, and split output |
| `internal/skiplist` | Ordered in-memory mutation index |
| `internal/segment` | Immutable segment format and readers/writers |
| `internal/wal` | Write-ahead log, replay, group commit, and truncation |
| `internal/manifest` | Persistent leaf catalog, dictionary groups, and replay watermarks |
| `internal/dbformat` | Directory format epoch, feature negotiation, and compatibility gate |
| `internal/compression` | LZ4 codecs, adaptive training, registry, and group catalog |
| `internal/metrics` | Reusable latency, background-work, disk, CPU, and RAM telemetry |
| `internal/scheduler` | Adaptive admission and preemption of background work |
| `internal/backup` | Streaming checksummed backup archive and safe extraction |
| `internal/disk` | Filesystem abstraction and atomic file operations |
| `monitoring` | Prometheus rules, Grafana dashboard, alerts, and integration guide |
| `cmd/fusedb-qualify` | Phased target-hardware qualification and JSON evidence |
| `benchmarks` | Isolated Go module for reproducible comparisons and recorded results |

## Project status

FuseDB is suitable for storage-engine research and experimentation. Before a
production release it still needs a long-term format support policy, broader
power-loss and target-filesystem fault campaigns, repeated backup/restore
drills, and workload validation on production hardware. See the explicit
[operational readiness guide](./docs/operations.md); contributions should start
with [CONTRIBUTING.md](./CONTRIBUTING.md). Linux and macOS are the supported
runtime platforms; see the [release contract](./docs/release.md).
