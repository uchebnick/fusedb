# FuseDB benchmarks

This module contains one comparison tool: `kvbench`. It exercises the supported
public FuseDB API and equivalent point operations in Pebble, Badger, and
RocksDB. It emits a versioned JSON artifact and a Markdown rendering of the same
raw runs.

Benchmarks answer a narrow question about one workload and machine. They are
not evidence that one database is universally faster.

## What is compared

Every matrix cell:

1. creates a new private database directory;
2. inserts the same deterministic key/value corpus outside the timed region;
3. flushes, closes, and reopens the engine;
4. runs an untimed warmup;
5. measures the same operation mix for the same wall-clock duration;
6. verifies a deterministic sample of keys;
7. drains foreground write debt, closes the database, and records its size;
8. removes only that generated run directory.

The runner records throughput and an HDR histogram with p50, p95, p99, p99.9,
and maximum latency. Reports also include engine versions, Go/runtime data,
CPU model, Git commit and dirty state, complete workload configuration, errors,
open/drain/close time, and raw repetitions.

## Engines

| Adapter | API | Default availability |
|---|---|---|
| FuseDB | `pkg/fusedb` | built in |
| Pebble | `github.com/cockroachdb/pebble` | built in |
| Badger | `github.com/dgraph-io/badger/v4` | built in |
| RocksDB | official `rocksdb/c.h` C API | build tag `rocksdb` plus native library |

RocksDB uses a deliberately small direct C adapter. This avoids coupling the
benchmark to a Go wrapper whose supported C API may lag the installed native
version.

List the adapters compiled into the current binary:

```bash
cd benchmarks
go run ./cmd/kvbench -list
```

## Workloads

All operations address existing keys with a uniform deterministic
distribution. Writes are overwrites.

| Workload | Reads | Writes |
|---|---:|---:|
| `read-random` | 100% | 0% |
| `read-heavy` | 95% | 5% |
| `balanced` | 50% | 50% |
| `overwrite` | 0% | 100% |

Scans are intentionally absent because FuseDB does not expose a public range
API. General transactions are absent for the same reason. FuseDB's native
atomic increment is a useful feature benchmark but has no equivalent primitive
in the comparison engines, so it is not presented as a cross-engine result.

## Durability profiles

The profiles are explicit and never mixed in one row:

| Profile | FuseDB | Pebble | Badger | RocksDB |
|---|---|---|---|---|
| `async` | async WAL group commit | `pebble.NoSync` | `SyncWrites=false` | write option `sync=false` |
| `sync` | synchronous WAL group commit | `pebble.Sync` | `SyncWrites=true` | write option `sync=true` |

`async` acknowledges a write before it is necessarily on stable storage.
`sync` asks the engine to make each acknowledged write durable; an engine may
coalesce concurrent sync requests. These profiles still reflect each engine's
native WAL and transaction implementation, not identical internal algorithms.

## Run it

From the repository root:

```bash
make benchmark-quick
```

The quick profile uses 20,000 keys, 256-byte values, 64 MiB cache targets,
1 and 8 workers, both durability modes, a 500 ms warmup, and a two-second
measurement per cell. It runs FuseDB, Pebble, and Badger without extra native
dependencies.

For the checked-in repeatable profile:

```bash
make benchmark-standard
```

The standard profile uses 250,000 keys, three repetitions, a three-second
warmup, and a 15-second measured window. It is intentionally much slower.

### RocksDB

Install RocksDB and `pkg-config`, then enable the adapter:

```bash
# macOS
brew install rocksdb pkg-config

# Debian/Ubuntu
sudo apt-get install librocksdb-dev pkg-config

make benchmark-rocksdb
```

If Homebrew's `.pc` file is outside the default search path:

```bash
PKG_CONFIG_PATH="$(brew --prefix rocksdb)/lib/pkgconfig" make benchmark-rocksdb
```

The build fails early when native headers or libraries are unavailable. A
binary built without the tag lists RocksDB as skipped instead of pretending it
was measured.

## Useful custom runs

```bash
cd benchmarks

# Only durable write-heavy behavior.
go run ./cmd/kvbench \
  -engines fusedb,pebble,badger \
  -workloads overwrite \
  -durability sync \
  -workers 1,8 \
  -keys 100000 \
  -warmup 2s \
  -duration 10s \
  -repetitions 3 \
  -json results/write-sync.json \
  -markdown results/write-sync.md

# All available adapters, including native RocksDB.
go run -tags rocksdb ./cmd/kvbench \
  -engines all \
  -profile standard \
  -json results/standard.json \
  -markdown results/standard.md
```

Interrupting the process with Ctrl-C stops the matrix. Completed rows can still
be written when at least one cell finished.

## Reading results responsibly

- Compare rows only within one report and durability profile.
- Treat a short run as a smoke test, not capacity planning.
- Run on the deployment filesystem; APFS, ext4, network disks, and container
  overlay filesystems have different sync behavior.
- Pin CPU frequency policy and minimize unrelated work for publication runs.
- Use at least three repetitions and report every repetition or a stated
  aggregation. Do not keep only the best run.
- Directory size after drain is not write amplification. Measuring actual bytes
  written requires OS/device counters around the full run.
- A hot point-read benchmark does not evaluate backups, recovery time,
  compaction stalls, range scans, transactions, or operational maturity.

The latest checked-in run and its exact command are in
[`RESULTS.md`](./RESULTS.md). Generated scratch reports named `latest.json` and
`latest.md` are ignored by Git.
