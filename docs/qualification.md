# Production qualification

`cmd/fusedb-qualify` is a bounded-memory, phased workload runner for evaluating
one FuseDB build on its intended hardware, filesystem, resource budget, and
data shape. It produces a versioned JSON evidence artifact and exits non-zero
when correctness, durability, maintenance, or latency gates fail.

It is a qualification tool, not a universal benchmark score. A short green run
does not certify power-loss behavior, long-term format support, hardware
firmware, or an application's business invariants.

## Safety boundary

`-dir` is required. By default the runner accepts only a missing or empty data
directory and leaves it intact after the run for investigation. It never
deletes the directory. `-allow-existing` disables this guard and can modify an
existing database; do not point it at production data.

All database operations use the supported `pkg/fusedb` API. The test enables
full latency sampling, generates self-checksummed values, and maintains a
separate set of exact integer counters.

## Workload lifecycle

The default accelerated cycle is:

| Phase | Default duration | Target rate | Purpose |
|---|---:|---:|---|
| quiet | 1 minute | 250 ops/s | establish low-load admission behavior |
| steady | 3 minutes | 2,000 ops/s | create representative merge and WAL debt |
| spike | 2 minutes | 8,000 ops/s | measure p95/p99 while scheduler protects foreground work |
| recovery | 3 minutes | 250 ops/s, reads only | give deferred maintenance a stable drain window |

These phases are metadata in the report; they are never sent to the scheduler.
The scheduler sees only real request rate, latency, resource use, learned
history, and maintenance debt.

Before timing, the runner creates the requested key population and forces one
exact checkpoint. After the phases it:

1. reads every data key and validates its embedded key identity and checksum;
2. checks every integer counter against the acknowledged increment count;
3. runs the full `DB.Verify` integrity scan;
4. closes the database and checks the close result;
5. reopens the same directory;
6. repeats the complete value/counter scan and `DB.Verify`.

One corrupt/missing read, foreground error, or not-ready health transition stops
the workload immediately. Latencies use a fixed 256-bucket histogram (8%
spacing, 100 ns through tens of seconds), so memory does not grow with run
duration or QPS.

## Running

The default nine-minute cycle:

```bash
make qualification DIR=/var/tmp/fusedb-qualification
```

Capture the report without mixing logs into it:

```bash
go run ./cmd/fusedb-qualify \
  -dir /mnt/qualification/fusedb-run-001 \
  -workers 32 \
  -keys 500000 \
  -value-bytes 512 \
  -steady-qps 10000 \
  -spike-qps 40000 \
  -cpu-cores 8 \
  -memory-bytes 8589934592 \
  -disk-bytes-per-second 400000000 \
  -max-cpu-utilization 0.80 \
  -max-disk-utilization 0.80 \
  -max-memory-utilization 0.80 \
  -max-read-p99 20ms \
  -max-write-p99 50ms \
  > qualification-001.json
```

For a real pilot gate, expand phases to hours and use the measured production
curve rather than the defaults. Keep `-wal-sync-writes=true` when that is the
deployed durability mode. Run on a dedicated target host; unrelated workloads
make latency evidence non-reproducible.

## Gates and report

The JSON contains:

- exact configuration, Go/runtime/architecture and embedded VCS revision;
- per-phase target and achieved QPS;
- bounded read/write/increment p50, p95, p99, maximum and error counts;
- scheduler-state sample counts;
- background starts/completions/failures/cancellations by phase and overall;
- peak and final buffered/WAL debt, pending leaves and scheduler queue;
- CPU, disk and memory utilization against effective capacity;
- pre-close and post-reopen integrity/counter results;
- every failed gate as a stable human-readable reason.

Exit codes are:

- `0`: the run and all configured gates passed;
- `1`: configuration, database, context, verification, or persistence failure;
- `2`: the workload completed, but one or more qualification gates failed.

Zero p99 or final-debt limits disable only that individual limit. Error-rate
checking remains active at zero. `-require-maintenance`,
`-require-debt-exercise`, and `-require-debt-drain` prove that the run actually
created background pressure and observed progress instead of reporting a clean
idle database.

Do not compare JSON results from different hardware as a release regression.
Keep a baseline per machine class, filesystem, durability mode, key/value
distribution, Go version, and resource budget. Review p99 together with sample
count; a percentile from a handful of operations is not evidence.

## Limits of the accelerated cycle

The runner validates warm foreground behavior after a seed checkpoint. Measure
cold open/first-read separately. A nine-minute run also cannot fully train the
15-minute UTC time-of-week model or represent a week-long request curve. Long
runs should contain repeated peaks, unexpected quiet windows, forced debt, and
enough recovery time to observe dictionary training/evaluation/GC as well as
merge and checkpoint work.
