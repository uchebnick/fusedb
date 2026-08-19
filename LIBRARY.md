# FuseDB library guide

This guide covers the supported API in `pkg/fusedb`. The package in
`pkg/oneleafdb` is an implementation detail and may change without notice.

## Installation

FuseDB requires Go 1.25.13 or newer within the 1.25 release line, a C
toolchain, and native LZ4 headers/runtime (`liblz4-dev` on Debian/Ubuntu or
`lz4` from Homebrew). A `CGO_ENABLED=0` build is accepted for static analysis
and uncompressed configurations, but creating or loading a dictionary returns
`fusedb.ErrCGODisabled`; production dictionary training therefore requires
CGO.

```bash
go get github.com/uchebnick/fusedb/pkg/fusedb@latest
```

## Opening and closing

```go
db, err := fusedb.Open(fusedb.Options{Dir: "./data"})
if err != nil {
    return err
}

// Close checkpoints buffered operations before releasing resources.
if err := db.Close(); err != nil {
    return err
}
```

`Dir` is required. Opening an existing directory loads its manifest and
segments, then replays the WAL records not covered by the manifest watermark.
`Close` is idempotent, but its error should still be checked because the final
checkpoint or file close can fail.

`Open` also takes an exclusive `<Dir>/LOCK`. A second live process receives an
error matching `fusedb.ErrDatabaseLocked`; the kernel releases ownership if the
owner process exits.

## Point operations

```go
if err := db.Put([]byte("user:1"), []byte("Alice")); err != nil {
    return err
}

value, found, err := db.Get([]byte("user:1"))
if err != nil {
    return err
}
if found {
    fmt.Printf("user:1 = %s\n", value)
}

if err := db.Delete([]byte("user:1")); err != nil {
    return err
}
```

`Put` copies the key and value before publishing them. `Get` also returns a
caller-owned copy, so reusing or modifying any of these slices cannot corrupt
the database. Deleting a missing key succeeds.

Empty keys are not representable in the current segment format. `Put`,
`Delete`, and `Inc` return `fusedb.ErrEmptyKey`; `Get` treats an empty key as a
miss.

## Counters

`Inc` atomically applies a signed delta to an `int64` counter. A missing counter
starts at zero.

```go
if err := db.Inc([]byte("page:views"), 2); err != nil {
    return err
}
if err := db.Inc([]byte("page:views"), -1); err != nil {
    return err
}

count, found, err := db.GetInt64([]byte("page:views"))
if err != nil {
    return err
}
fmt.Printf("count=%d found=%v\n", count, found)
```

`Put` values and `Inc` counters have distinct internal types. `GetInt64` on a
byte value returns an error wrapping `fusedb.ErrValueType`. Likewise, `Inc`
cannot turn an existing byte value into a counter; the rejected operation is
not appended to the WAL.

Keys are limited to `fusedb.MaxKeySize` (1 MiB) and byte values to
`fusedb.MaxValueSize` (64 MiB). Oversized mutations return
`fusedb.ErrKeyTooLarge` or `fusedb.ErrValueTooLarge` before changing durable or
in-memory state. These ceilings also bound recovery allocations.

## Atomic and idempotent write batches

`Apply` evaluates key-presence conditions and writes every mutation as one WAL
record. A failed condition returns `applied=false` without changing data.

```go
likeKey := []byte("like/post-42/user-7")
applied, err := db.Apply(
    []fusedb.Condition{fusedb.KeyAbsent(likeKey)},
    []fusedb.Mutation{
        fusedb.PutMutation(likeKey, []byte("liked")),
        fusedb.IncMutation([]byte("like-count/post-42"), 1),
    },
)
```

For retryable message delivery, prefer `ApplyOnce` or `ApplyOnceIf`. Their
idempotency key must remain stable across retries and must not be changed with
ordinary `Put`/`Delete`. Reusing it for different mutations fails with
`fusedb.ErrIdempotencyConflict`. `ApplyOnceIf` persists the first condition
decision even when it is false, preventing an old retry from applying against
newer state.

These APIs provide atomic write publication, not a general transaction or a
multi-key read snapshot. A batch may contain at most 4,095 user mutations and
may mutate each key once.

## Configuration

For a durable rebuildable pilot, start from
`fusedb.DurablePilotOptions(dir)`. It enables synchronous WAL, reserves 30%
resource headroom, samples latency, and leaves adaptive dictionary training off
until it has passed qualification on the target machine.

```go
db, err := fusedb.Open(fusedb.Options{
    Dir:                    "./data",
    CacheSize:              5 << 20,
    MergeSize:              8 << 20,
    MaxLeafSize:            64 << 20,
    WALPath:                "./data/wal.log",
    WALSyncWrites:          false,
    WALGroupCommitInterval: 200 * time.Microsecond,
    DictionaryPath:         "./dict.zdict",
    DictionaryTraining: fusedb.DictionaryTrainingOptions{
        GroupLeaves:              8,
        MinimumTrainingSamples:   32,
        MinimumEvaluationSamples: 8,
        MinimumGain:              0.03,
        MaxTotalSampleBytes:      32 << 20,
    },
    Scheduler: fusedb.SchedulerOptions{
        TargetReadP99:      2 * time.Millisecond,
        TargetWriteP99:     2 * time.Millisecond,
        QuietConfirm:       30 * time.Second,
        QuietRateCeiling:   100,
        CPUCores:           4,         // zero: use effective GOMAXPROCS
        MemoryBytes:        2 << 30,   // zero: runtime/physical limit
        DiskBytesPerSecond: 200 << 20, // zero: learn effective throughput
    },
})
```

| Option | Default | Purpose |
|---|---:|---|
| `Dir` | required | Database directory |
| `CacheSize` | 5 MiB | Approximate value-cache byte budget |
| `MergeSize` | 8 MiB | Buffered bytes that make a leaf eligible for merge |
| `MaxLeafSize` | 64 MiB | Target upper bound for data rewritten by one leaf merge |
| `WALPath` | `<Dir>/wal.log` | Write-ahead log location |
| `WALSyncWrites` | `false` | Sync every WAL append before acknowledging it |
| `WALGroupCommitInterval` | 200 µs | Async flush and synchronous group-commit coalescing interval |
| `DictionaryPath` | empty | Optional seed LZ4 dictionary; runtime training can publish one when empty |
| `DictionaryTraining` | bounded defaults | Per-group sampling, training, evaluation, and memory limits |
| `DictionaryGC` | 64 files/pass | Scheduler-driven cleanup of unreferenced dictionary versions |

Non-positive size values select their defaults. Smaller leaves reduce the
amount rewritten per merge but increase the number of leaves and segment files.

Runtime dictionary training is enabled by default and consumes only raw block
samples already produced by merges. Groups are contiguous and contain eight
leaves by default. Training/evaluation require a confirmed quiet scheduler
window, are cooperatively cancellable, use disjoint samples, and publish only
after `MinimumGain`. Set `DictionaryTraining.Disabled` to opt out. Sample memory
is bounded both per group and per database.

Dictionary lifecycle cleanup is enabled by default. A version is deleted only
after it is absent from both the checksummed active-group catalog and every
segment referenced by the current manifest. Automatic passes delete at most 64
files by default and are preemptible; `CollectDictionaryGarbage` requests a
complete scheduled pass. Set `DictionaryGC.Disabled` only for diagnostics or
forensic retention.

### Adaptive background work

Merge admission is controlled by a single adaptive scheduler rather than a
fixed day/night timer. It observes rolling request rates and sampled p95/p99,
learns recurring 15-minute time-of-week baselines, waits for a stable quiet
window, and immediately revokes optional work when latency pressure returns.

`SchedulerOptions` sets application SLOs, capacities, and admission limits.
Zero values select automatic or built-in defaults. Merge becomes mandatory at
a hard memory-debt limit and WAL checkpointing remains mandatory at its
configured watermark, so sustained traffic delays maintenance without allowing
unbounded debt.

The learned global and UTC time-of-week load model is persisted by default in
the checksummed `SCHEDULER-STATE` file. Writes occur only as a low-interference
quiet-window scheduler job; clean shutdown and backup also capture the newest
snapshot. Set `Scheduler.DisableModelPersistence` only when every restart is
expected to relearn its workload history.

`CPUCores`, `MemoryBytes`, and `DiskBytesPerSecond` describe absolute capacity
available to the scheduler. When omitted, CPU and memory are detected and disk
throughput is learned from real database IO. `MaxCPUUtilization`,
`MaxMemoryUtilization`, and `MaxDiskUtilization` can reserve a fraction of that
capacity for the application or other processes; their default is 1.0.

`WALCheckpointBytes` controls the logical WAL debt that makes a whole-tree exact
checkpoint mandatory (64 MiB by default). It is distinct from `MergeSize`,
which admits independent per-leaf merges. Lowering the checkpoint threshold
reduces replay debt but increases checkpoint frequency and write amplification.

Runtime monitoring is available without a Prometheus dependency:

```go
metrics := db.Metrics()
fmt.Printf("reads=%d writes=%d\n", metrics.ReadOps, metrics.WriteOps)
fmt.Printf("terminal=%d uncertain=%d\n",
    metrics.TerminalErrors, metrics.CommitUncertainErrors)
for _, background := range metrics.Background {
    fmt.Printf("%s: active=%d completed=%d cancelled=%d\n",
        background.Kind,
        background.Active,
        background.Completed,
        background.Cancelled,
    )
}

health := db.Health()
if !health.Ready {
    // Remove this handle from readiness and reopen it after shutdown.
}
```

Histogram bounds, exact sums, and non-cumulative bucket counts let the optional
[`pkg/fusedb/prometheus`](./pkg/fusedb/prometheus) collector export a classic
histogram without instrumenting the request path twice. `TerminalErrors` and
`CommitUncertainErrors` are monotonic per-handle health counters and should
alert immediately. The collector also exports WAL checkpoint debt, pending
merge leaves, scheduler queue/executor state, bounded background-job labels,
resource budgets, and the last successful completion of every job kind.

Registration is explicit and never modifies Prometheus' global registry. See
the [Prometheus operations bundle](./monitoring/prometheus/README.md) and
[`docs/scheduler.md`](./docs/scheduler.md).

## Durability

Every mutation is appended to the WAL before it is applied to the in-memory
tree. The default asynchronous mode acknowledges after append and in-memory
apply; a background group commit flushes the WAL every 200 µs by default. An
operating-system or power failure can lose the most recent unsynced interval.

With `WALSyncWrites: true`, each mutation waits for its WAL record to be synced.

Mutations sharing a key are serialized through one of 4096 hash shards across
type validation, WAL append, and tree application. Thus their live order is
the same order recovery observes. Unrelated shards remain concurrent.
This improves durability at the cost of write latency.

Each independent leaf merge freezes at an exact WAL sequence and persists a
per-leaf replay watermark. Recovery skips records already materialized for that
leaf, which is required for `Inc` to remain exactly-once even when other leaves
have not merged. The manifest's global `AppliedSeq` is the minimum leaf
watermark and is therefore safe for WAL truncation.

`Merge` forces a complete checkpoint: it freezes all leaf buffers at one exact
WAL sequence, writes their segments, persists the manifest, and truncates the
covered WAL prefix. Normal workloads do not need to call it because merges run
in the background and `Close` performs a final checkpoint.

A write or sync failure matching `fusedb.ErrWALPersistence` is terminal for the
current handle. FuseDB rejects later reads and writes and `Close` deliberately
skips its checkpoint, because the failed sequence may describe a complete,
partial, or absent WAL record that was never applied to the in-memory tree.
Close and reopen before reconciling or retrying the mutation. When the error
also matches `fusedb.ErrCommitUncertain`, the mutation may appear after replay
or may be absent; application retry must therefore happen only after reopen and
state reconciliation.

Open uses the validated manifest as the exact live segment root set. While
holding the exclusive directory lock it removes only canonical completed or
temporary segment filenames not referenced by that manifest. This makes a
segment rename followed by failed directory sync retryable without reusing a
still-present immutable `(segment id, version)`.

## Format compatibility

Every current database has a checksummed `FORMAT` descriptor. `Open` checks its
epoch and required feature bits before creating or rewriting database metadata.
Callers can classify incompatibility with `fusedb.ErrFormatTooNew`,
`fusedb.ErrFormatTooOld`, `fusedb.ErrUnknownRequiredFeature`,
`fusedb.ErrMissingRequiredFeature`, `fusedb.ErrFormatCorrupt`, and
`fusedb.ErrFormatMismatch`.

`db.Format()` reports the accepted epoch and feature masks. Pre-descriptor
manifest v2/v3 databases and backups are atomically migrated to manifest v4;
`FORMAT` is published last so an interrupted upgrade is retryable. General
downgrade safety and indefinite cross-release compatibility are not yet
promised. See [`docs/format.md`](./docs/format.md).

## Integrity verification

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
defer cancel()

report, err := db.Verify(ctx)
if errors.Is(err, fusedb.ErrCorruption) {
    return fmt.Errorf("database must be restored or rebuilt: %w", err)
}
if err != nil {
    return err
}
log.Printf("verified %d segments and %d keys", report.Segments, report.Keys)
```

`Verify` checks manifest allocation and replay invariants, reads and
decompresses every referenced segment block, validates block CRCs, indexes,
bloom membership and leaf key ranges, resolves exact dictionary IDs, and walks
the WAL checksums and sequence numbers. The scan is a preemptible scheduler job,
so foreground pressure or the caller deadline may cancel it; retry during a
quiet window.

Errors matching `fusedb.ErrWALPersistence` or `fusedb.ErrCommitUncertain`
require closing and reopening the handle before reconciling or retrying. See
the [operational readiness guide](./docs/operations.md).

## Point-in-time backup and restore

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
defer cancel()

report, err := db.Backup(ctx, "/backups/app.fbak")
if err != nil {
    return err
}
log.Printf("backed up WAL sequence %d in %d bytes", report.AppliedSeq, report.Bytes)

_, err = fusedb.Restore(ctx, "/backups/app.fbak", "./restored-data")
if errors.Is(err, fusedb.ErrBackupChecksum) {
    return fmt.Errorf("backup is corrupt: %w", err)
}
if err != nil {
    return err
}
```

`Backup` is an online consistent snapshot, not a transactional read-snapshot
API. It is admitted through the adaptive scheduler, forces an exact checkpoint,
and then streams the referenced manifest, segments, persistent LZ4 dictionaries,
and a clean WAL into an atomically published archive. The destination must be
outside the database directory.

`Restore` requires a new or empty directory. It verifies per-file CRCs and the
archive SHA-256 before extraction, publishes `MANIFEST` last, and performs a
full database integrity scan before succeeding. Match
`fusedb.ErrRestoreDestinationNotEmpty`, `fusedb.ErrInvalidBackup`, and
`fusedb.ErrBackupChecksum` for operational handling. Pin the FuseDB version:
backup-format compatibility is not yet promised across experimental releases.

## Statistics

```go
stats := db.Stats()
log.Printf("buffer=%d wal=%d leaves=%d pending=%d queued=%d running=%t",
    stats.BufferedBytes, stats.WALBytes, stats.Leaves,
    stats.PendingMergeLeaves, stats.SchedulerQueued, stats.SchedulerRunning)
```

`BufferedBytes` is approximate live mutation-buffer data. `WALBytes` is
checkpoint debt rather than physical file size. `Leaves` is the number of key
ranges; each leaf independently owns and merges one segment.

## Concurrency and ownership

One `DB` may be used concurrently by multiple goroutines. Point reads and
ordinary writes do not take the tree's structural lock. Structural changes
serialize internally and retry writes that raced with a split.

The ownership contract is part of the tested public behavior:

- `Put` does not retain caller-owned key or value memory.
- `Get` never exposes mutable skiplist, cache, block, or pooled memory.
- `Inc` operations are replayed exactly once across checkpoints and restarts.

## Current limitations

- Experimental API and on-disk format
- Single process and single node
- Point operations only; no range scan API
- No general read/write transactions, transactional read snapshots, TTLs, or
  arbitrary value compare-and-swap; atomic conditional write batches are supported
- No compatibility promise between unreleased storage-format versions

Runnable examples are in [`pkg/fusedb/example_test.go`](./pkg/fusedb/example_test.go).
The likes/tickets pilot adapter is in
[`examples/likes-tickets/store.go`](./examples/likes-tickets/store.go).
Durability and ownership guarantees are exercised in
[`pkg/fusedb/durability_test.go`](./pkg/fusedb/durability_test.go).
