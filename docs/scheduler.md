# Adaptive background scheduler

FuseDB uses one scheduler to arbitrate merge, checkpoint, backup, dictionary
training/evaluation/GC, and learned-model persistence. A fixed day/night
schedule is deliberately not part of the policy: UTC time of week is learned as
a baseline, while admission always depends on current foreground load and
latency.

## Objectives

The policy orders its objectives as follows:

1. Keep foreground p95/p99 stable.
2. Prevent merge, memory, and WAL debt from crossing hard limits.
3. Use otherwise idle CPU, disk, and memory for maintenance.
4. Train and evaluate dictionary candidates only from surplus capacity.

Merge is deferrable mandatory work. Dictionary training is optional investment
work and may be cancelled whenever foreground pressure returns.

## Metrics

`internal/metrics` records exact operation counters and sampled latency in
mergeable exponential histograms. Snapshots are cumulative and dependency-free;
the optional `pkg/fusedb/prometheus` collector translates them into counters,
gauges, and classic histogram buckets without instrumenting the request path a
second time.

The scheduler derives rolling values from consecutive snapshots:

- read, write, and total request rates;
- read/write p95 and p99;
- request-rate coefficient of variation;
- CPU cores in use, database disk bytes/s, and runtime-managed memory bytes;
- utilization of the effective CPU, disk, and memory capacity;
- background job starts, completions, failures, cancellations, and wall time.

Latency is sampled (one in 64 operations by default); operation counters are
never sampled. Filesystem reads/writes are counted by a transparent
`internal/disk.FS` wrapper. CPU and memory come from Go runtime metrics.

Resource capacities may be configured explicitly. A zero value is automatic:

- CPU capacity is the effective `GOMAXPROCS` value;
- memory capacity is the smaller applicable `GOMEMLIMIT` or physical RAM;
- disk capacity learns the highest effective throughput observed through this
  database filesystem, with headroom, instead of running a destructive disk
  benchmark during `Open`.

The default utilization ceilings are 100% of those capacities. Setting a lower
ceiling reserves headroom for the host. Automatic disk capacity is empirical:
it improves as representative IO is observed and is not a claim about the
device's theoretical specification.

## Load model

The controller has asymmetric hysteresis:

```text
normal → quiet candidate → quiet   (slow, requires a stable window)
quiet  → busy/overloaded           (immediate on a latency spike)
busy   → recovering → normal       (cooldown before new admission)
```

It maintains two learned baselines:

- a global EWMA that rises quickly and decays slowly;
- 15-minute time-of-week slots that learn recurring load shape.

Each seasonal observation is the average of a completed interval, not one
scheduler poll. A slot is trusted only after it has been seen repeatedly, so a
single quiet interval cannot teach the controller a fake schedule.

The seasonal model is a prior, never a trigger. A historically quiet period
does not admit work when current p99 is high, and an unexpected quiet period may
admit work regardless of wall-clock time.

Completed UTC seasonal buckets, the global baseline, sample counts, and a model
generation are stored in the checksummed `SCHEDULER-STATE` auxiliary file. The
file is atomically replaced in a confirmed quiet window, copied by backup, and
restored independently of the manifest. Admission state and an incomplete
15-minute interval are never persisted, so every open starts conservatively in
`normal`. A corrupt model cannot block user data: its load failure is counted in
`scheduler_model_persist` metrics and a fresh model replaces it during a later
quiet window. `SchedulerOptions.DisableModelPersistence` opts out.

The small atomic rename/directory-sync commit is deliberately non-preemptible:
the scheduler prevents it from starting under pressure, but does not pretend an
already-running filesystem sync can be cancelled safely.

## Jobs and preemption

Jobs are deduplicated by stable ID and classified as maintenance, dictionary
work, or scheduler-model persistence. They declare estimated resource cost,
dynamic urgency, whether they are mandatory, and whether they are preemptible.

Only one high-interference job runs at a time in the first implementation.
Optional jobs must fit inside configured CPU/disk/memory headroom. Mandatory
checkpoint or merge debt may bypass normal admission and immediately preempt
running dictionary training/evaluation. Explicit `DB.Merge` also enters this
queue as a mandatory checkpoint and waits for completion; it does not create a
second merge path outside the scheduler.

Explicit `DB.Backup` is high-priority but not mandatory: it waits for
maintenance admission and configured resource headroom, then checkpoints and
streams one immutable archive. Its checkpoint commit phase is non-preemptible;
caller cancellation is observed before it and throughout archive IO.

A preemptible executor must cooperate with cancellation and return
`context.Canceled` after compute actually stops. Wrapping an uninterruptible
builder in a goroutine is not preemption: the abandoned goroutine still consumes
CPU. `scheduler.Cooperative` provides cancellation boundaries for staged
trainers. Runtime LZ4 training uses the bounded frequent-chunk implementation;
the older monolithic builder remains available only for explicit offline
pretraining and is not used by scheduler jobs.

## Current merge policy

Writes submit a deduplicated merge job after buffered data reaches the merge
threshold. The scheduler defers it under foreground pressure and starts at most
one merge at a time. At four times the threshold the merge becomes mandatory to
bound memory growth. Before each admitted leaf merge, writes are excluded only
long enough to capture the current WAL sequence and freeze that leaf buffer.
The resulting manifest record stores that exact per-leaf replay watermark, so
recovery cannot apply an `Inc` already materialized by an independent merge.

WAL debt is stricter: crossing the checkpoint threshold submits a mandatory
checkpoint that freezes all buffers at an exact sequence, merges them, persists
the manifest, and truncates the covered WAL prefix.

Scheduling never changes storage correctness. WAL ordering, exact `AppliedSeq`,
per-leaf replay watermarks, split publication, manifest atomicity, and
dictionary lifetime remain executor invariants. Cancellation can delay a
commit phase but cannot partially publish one.

## Dictionary pipeline

The runtime group-dictionary workflow is a job DAG:

```text
CollectSamples → TrainCandidate → EvaluateCandidate → PublishCandidate
                                                        ↓
                                                RetireOldDictionary
```

Merge emits bounded samples from the encoded raw blocks it already materializes;
training never rescans the database. Training and evaluation use disjoint
samples. The cooperative frequency-history trainer checks cancellation at
bounded intervals, and held-out evaluation verifies codec round trips and a
minimum compression gain before publication.

Manifest v4 assigns stable contiguous leaf groups (eight leaves by default).
Dictionary IDs are reserved durably before training and never reused. A winning
immutable dictionary is saved first, then selected through the atomic
`DICTIONARY-CATALOG`; new merges observe it without blocking foreground writes.
Both group assignment and active selection survive restart and backup/restore.

Samples and rejected candidates are intentionally memory-only. A restart
relearns them from later merges. Bounded dictionary GC rereads the persisted
group catalog and exact dictionary IDs in current segment headers. It deletes
only versions absent from both reference sets, keeps already-open readers safe
through their immutable in-memory dictionary pointer, and is itself a
preemptible scheduler job. Startup cleanup also removes fully orphaned files
left by failed pre-publication attempts.
