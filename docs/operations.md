# Operational readiness

FuseDB is still a research-stage embedded engine. The durability path now has
checksummed WAL and segments, atomic manifest replacement, exact per-leaf replay
watermarks, exclusive directory ownership, integrity verification, adaptive
maintenance admission, and deterministic I/O fault tests. Those are necessary
production foundations; they are not a production support promise.

## Readiness boundary

| Area | Current state |
|---|---|
| Single-process concurrency | Supported and race-tested |
| Clean restart and WAL crash replay | Tested |
| Process kill at WAL/segment/manifest/rotation boundaries | Repeated OS-filesystem subprocess matrix in CI |
| ENOSPC/short-write/fsync/rename fault matrix | Deterministic MemFS coverage; target-filesystem qualification remains |
| Independent leaf merge with `Inc` replay exactly once | Tested |
| Atomic conditional/idempotent write batches across leaves | WAL codec, concurrency, merge/reopen, fault, and subprocess crash tested |
| Second process opening the same directory | Rejected with `ErrDatabaseLocked` |
| Manifest/WAL/segment integrity scan | `DB.Verify(context.Context)` |
| Resource-aware merge admission | Implemented; hardware defaults are automatic |
| Learned scheduler history | Checksummed, UTC-based, restart and backup persistent |
| Adaptive per-group LZ4 dictionaries | Implemented, including reference-safe lifecycle GC |
| On-disk compatibility gate | Checksummed epoch/features; manifest v2/v3 migrate to v4 |
| Corrupt-size/OOM containment | Epoch-2 hard limits checked before allocation; decoder fuzz coverage |
| Long-term cross-release support | Not promised during research stage |
| Point-in-time backup and restore | `DB.Backup` / `fusedb.Restore`, checksummed and restore-verified |
| Replication or multi-node failover | Not implemented |
| Full filesystem/power-loss certification | Not completed |
| Long-duration production workload qualification | Not completed |
| Reproducible phased qualification runner | Implemented; target-hardware soak evidence remains deployment-specific |
| Prometheus collector, readiness, and alert examples | Implemented; thresholds require workload tuning |

Do not use FuseDB as the only copy of business-critical data yet. A realistic
pilot should keep a rebuildable source of truth, pin the exact FuseDB version,
and validate the target filesystem and workload before rollout.

## Opening and ownership

`Open` acquires `<Dir>/LOCK` using a non-blocking OS advisory lock. A second
live owner receives `fusedb.ErrDatabaseLocked`. The lock file itself remains in
the directory; ownership is the kernel lock, and it is released automatically
if the process exits.

All database files, including a custom WAL path, should live on storage whose
rename and directory-sync semantics match a local POSIX filesystem. Networked
or userspace filesystems require their own fault qualification.

## Durability choice

For a rebuildable first deployment, use `fusedb.DurablePilotOptions(dir)` and
then set the target host's explicit CPU, disk, and memory capacities. The
profile enables synchronous WAL, keeps 30% foreground headroom, and disables
dictionary training until target-machine qualification is complete.

- `WALSyncWrites: false` acknowledges before the next group sync and can lose
  up to one group-commit interval on power/process failure.
- `WALSyncWrites: true` waits for WAL sync on every mutation and is the safer
  pilot setting when write latency permits.
- Always check errors from mutations, `Merge`, and `Close`.

An error matching `fusedb.ErrCommitUncertain` means rename completed but the
parent directory sync failed. Stop sending traffic to that handle, call
`Close`, and reopen the database. FuseDB preserves both sides needed for
recovery and deliberately refuses to checkpoint again from a stale in-memory
view.

An error matching `fusedb.ErrWALPersistence` is also terminal even when it does
not match `ErrCommitUncertain`. Future calls are fenced and `Close` skips its
checkpoint. Reopen first. If `ErrCommitUncertain` is present, inspect the value
after recovery before retrying because a complete WAL record may replay even
though the original call returned an error. Blindly retrying an `Inc` can apply
the business operation twice.

On open, the validated manifest is the segment liveness root. Under the
exclusive directory lock FuseDB removes canonical `.seg` and `.seg.tmp` files
that the manifest does not reference. This repairs interrupted segment commits
without deleting unrelated files or any live segment.

`make test-crash` runs each storage commit boundary three times. A child process
is killed immediately after the selected WAL write/sync, segment sync/rename,
manifest sync/rename, or WAL-rotation sync/rename. The parent then reopens the
database, requires an `Inc` to be present exactly once, runs `Verify`, and
checks the manifest's segment root set. This exercises real process death and
the OS filesystem cache; it does not emulate lost writes after power removal,
controller reordering, torn sectors, or latent media corruption.

## Integrity verification

Run a scan during a quiet window or before promoting a restored copy:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
defer cancel()

report, err := db.Verify(ctx)
if errors.Is(err, fusedb.ErrCorruption) {
    // Quarantine the copy and restore/rebuild it.
}
if err != nil {
    return err
}
log.Printf("verified leaves=%d segments=%d blocks=%d keys=%d wal=%d",
    report.Leaves, report.Segments, report.Blocks, report.Keys, report.WALRecords)
```

`Verify` rereads and checksums `FORMAT`, the scheduler model, dictionary-group catalog,
and every active dictionary, then reads and decompresses every referenced block, checks its CRC,
index separator, key order, bloom membership, leaf range and key count,
resolves the exact LZ4 dictionary, validates manifest allocator/watermark
invariants, and walks the WAL checksums and sequence numbers. It runs as
preemptible maintenance through the adaptive scheduler; foreground pressure or
the caller deadline may return `context.Canceled`/`context.DeadlineExceeded`,
in which case retry later.

## Monitoring

Use `DB.Health()` for application readiness. It returns not-ready after close
or after a terminal WAL/background persistence failure; checking it performs no
filesystem I/O. `DB.Metrics()` and `DB.Stats()` remain dependency-free for
custom adapters.

The optional `pkg/fusedb/prometheus` collector provides stable metric names,
fixed low-cardinality labels, cumulative counters, classic latency histograms,
maintenance debt, scheduler state, resource utilization, and last-success
timestamps. It requires an application-owned `prometheus.Registerer` and never
registers globally. See the [Prometheus operations bundle](../monitoring/prometheus/README.md)
for a runnable endpoint and checked-in recording/alert rules.

At minimum alert on:

- any `TerminalErrors` or `CommitUncertainErrors` increase;
- any background `Failed` increase;
- sustained `overloaded` scheduler state;
- p95/p99 above the configured targets;
- growing buffered bytes or checkpoint debt without completed maintenance;
- CPU, disk, or memory utilization at the configured ceiling;
- any `ErrCorruption`, `ErrWALPersistence`, `ErrCommitUncertain`, or unexpected
  `Close` error.

Capacities default to effective `GOMAXPROCS`, the runtime/physical memory limit,
and learned database I/O throughput. Production pilots should normally set
explicit ceilings below 1.0 to reserve host headroom and should set
`GOMEMLIMIT` consistently with the container limit.

## Backup and restore

Use the online point-in-time API; never copy live database files independently:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
defer cancel()

backup, err := db.Backup(ctx, "/backups/fusedb-2026-08-19.fbak")
if err != nil {
    return err
}
log.Printf("backup seq=%d files=%d bytes=%d",
    backup.AppliedSeq, backup.Files, backup.Bytes)

_, err = fusedb.Restore(ctx,
    "/backups/fusedb-2026-08-19.fbak",
    "/srv/fusedb-restored",
)
if err != nil {
    return err
}
```

`Backup` enters the same scheduler as merge and dictionary work. It waits while
the foreground is busy or resource headroom is unavailable, checkpoints at one
exact WAL sequence, and captures only that manifest's immutable segments,
database-owned LZ4 dictionaries, the format descriptor, and latest learned scheduler model. Later
foreground writes are excluded. The archive contains a clean empty WAL
beginning at `AppliedSeq+1`.

Archive creation and extraction are streaming and bounded-memory. Every entry
has a CRC and the complete archive has a SHA-256 footer. The completed archive
is reread before `Backup` succeeds. `Restore` verifies all checksums before
extracting into a new or empty directory, installs `MANIFEST` last, then opens
the result and runs the full semantic `Verify` scan. A failed restore is never
silently promoted; inspect or remove its destination before retrying.

The backup path must be outside `Dir`. One writer owns `<archive>.lock`; the
sidecar lock file may remain after completion. Archive and storage-format
compatibility are still experimental, so pin the FuseDB version used for both
backup and restore and run regular restore drills.

See the [format compatibility contract](./format.md) for early rejection,
legacy migration, and downgrade boundaries.

The checkpoint commit phase cannot be safely interrupted midway. Caller
cancellation stops queued work and streaming archive IO, but a cancellation or
load spike that arrives during an admitted checkpoint takes effect after that
atomic storage phase completes.

## Shutdown

Use a bounded graceful shutdown and check `Close`; it performs a checkpoint
unless the handle has already encountered a commit-uncertain background error.
`ErrSchedulerModelPersistence` means user data closed normally but optional
learned load history could not be saved; alert and allow the model to relearn.
Copying live files independently can mix manifest, segment, and WAL generations
and is not a valid backup; use `DB.Backup` instead.

## Minimum pilot gate

Before a non-critical deployment:

1. Pin the FuseDB commit/version and Go toolchain.
2. Run the full tests and race detector on the target architecture, including
   `make test-crash` and `make test-fuzz`.
3. Soak the real key/value distribution with expected and peak request curves.
4. Measure foreground p95/p99 while merge/checkpoint debt is forced.
5. Run repeated kill/reopen and storage fault campaigns on the target
   filesystem.
6. Verify restored data and application-level invariants.
7. Keep a tested rollback/rebuild path outside FuseDB.
8. Exercise `/readyz` and every critical alert in staging before routing pages.

Use [`cmd/fusedb-qualify`](./qualification.md) to produce a versioned JSON
artifact for steps 3–6. Its short default cycle is a smoke gate; production
approval still requires hour/day-scale runs on the actual filesystem and
hardware.
