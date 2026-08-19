# Prometheus operations bundle

FuseDB exposes dependency-free in-process snapshots from `DB.Metrics`,
`DB.Stats`, `DB.Health`, and `DB.Format`. The optional
`pkg/fusedb/prometheus` adapter converts those snapshots into a custom
Prometheus collector. A scrape performs no filesystem, verification, merge,
dictionary-training, or network work.

## Registration

Create an application-owned registry and register each database explicitly:

```go
registry := prometheus.NewRegistry()
registration, err := fusedbprom.Register(registry, db, fusedbprom.Options{
    ConstLabels: prometheus.Labels{"database": "primary"},
})
if err != nil {
    return err
}
defer registration.Close()
```

The package never uses the global default registry. A duplicate registration
returns `prometheus.AlreadyRegisteredError`; it is not silently attached to a
different database. Const labels are copied at construction. Keep them bounded
and stable. Do not use a key, tenant, request ID, filesystem path, or error
message as a label value.

See the runnable [endpoint example](../../examples/prometheus/main.go) for
`/metrics`, `/healthz`, `/readyz`, process metrics, timeouts, signals, and
graceful shutdown.

## Metric contract

The adapter exports these stable families under the default `fusedb`
namespace:

| Family | Type | Labels | Meaning |
|---|---|---|---|
| `fusedb_ready` | gauge | const only | Handle accepts foreground traffic |
| `fusedb_closed` | gauge | const only | Handle was closed |
| `fusedb_terminal_error` | gauge | const only | Handle is fenced until reopen |
| `fusedb_format_info` | gauge | format identity | Opened format epoch and feature masks |
| `fusedb_operations_total` | counter | `operation` | Attempted reads/writes, including rejected calls |
| `fusedb_operation_duration_seconds` | histogram | `operation` | Sampled request latency with exact sum/count |
| `fusedb_terminal_errors_total` | counter | const only | Terminal handle failures |
| `fusedb_commit_uncertain_errors_total` | counter | const only | Terminal failures requiring reconciliation |
| `fusedb_buffered_bytes` | gauge | const only | In-memory merge debt |
| `fusedb_wal_checkpoint_debt_bytes` | gauge | const only | WAL bytes since exact checkpoint |
| `fusedb_pending_merge_leaves` | gauge | const only | Leaves waiting for merge |
| `fusedb_scheduler_state` | gauge | `state` | Fixed one-hot adaptive state |
| `fusedb_scheduler_jobs_queued` | gauge | const only | Jobs waiting for admission |
| `fusedb_scheduler_job_running` | gauge | const only | Serialized executor occupancy |
| `fusedb_background_jobs_total` | counter | `kind`, `outcome` | Fixed job lifecycle counters |
| `fusedb_background_jobs_active` | gauge | `kind` | Current work by fixed kind |
| `fusedb_background_job_duration_seconds_total` | counter | `kind` | Cumulative background wall time |
| `fusedb_background_job_last_success_timestamp_seconds` | gauge | `kind` | Last successful completion or zero |
| `fusedb_resource_utilization_ratio` | gauge | `resource` | Used/effective-capacity ratio |
| `fusedb_resource_*` | gauge | bounded enums | Absolute usage, capacity and auto-detection |
| `fusedb_disk_io_bytes_total` | counter | `direction` | Bytes through the database filesystem |

The scheduler states, job kinds, outcomes, operation types, resource types,
directions, scopes, and quantiles are closed enumerations. Unknown scheduler
states map to `unknown`; unknown background kinds are not exported.

Latency recording is sampled (`64` by default), while operation counters are
not. `fusedb_operation_latency_sample_every` publishes the configured ratio.
Classic histogram buckets are cumulative on export and retain the engine's
fixed boundaries. Aggregate replicas by `le` before applying
`histogram_quantile`; do not average local p99 values.

## Rules and rollout

[fusedb.rules.yml](./fusedb.rules.yml) contains recording rules for operation
rate/p95/p99 and alerts for terminal state, uncertain commits, failed
maintenance, sustained scheduler overload, SLO regression, stalled merge/WAL
debt, and resource saturation.

Import the companion [Grafana overview dashboard](../grafana/fusedb-overview.json)
after installing the recording rules. It covers readiness, terminal failures,
p95/p99, request rate, merge/WAL debt, scheduler state, resources, background
jobs, and database filesystem throughput.

The included 50 ms read p99, 100 ms write p99, 128 MiB WAL debt, and alert
durations are safe examples, not universal SLOs. Tune them to the deployed
`SchedulerOptions`, WAL checkpoint trigger, storage latency, and error budget.
Run `make test-monitoring` before installing or modifying the file. The target
uses the Prometheus 3.14.0 image pinned by digest to run the real `promtool`,
then verifies the Grafana and collector contracts through Go tests. Also repeat
`promtool check rules` with the exact Prometheus release deployed in production.

Suggested rollout:

1. Scrape a canary without alerts and verify series labels/count.
2. Compare histogram p95/p99 with the load generator's raw latency.
3. Force merge and checkpoint debt and confirm it drains outside spikes.
4. inject a staging terminal fault and confirm readiness and paging behavior;
5. install warnings first, then promote terminal and uncertain-commit alerts to
   paging after routing is verified.
