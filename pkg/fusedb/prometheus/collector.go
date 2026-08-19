// Package prometheus exports FuseDB's immutable telemetry snapshots through
// prometheus/client_golang without adding work to the foreground request path.
//
// The collector uses only fixed, low-cardinality label values. Applications
// explicitly register it with their own prometheus.Registerer; importing this
// package never mutates the global registry.
package prometheus

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/uchebnick/fusedb/pkg/fusedb"
)

const defaultNamespace = "fusedb"

var metricNamespacePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
var labelNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var reservedVariableLabels = map[string]struct{}{
	"operation": {}, "epoch": {}, "min_reader_epoch": {}, "min_writer_epoch": {},
	"required_features": {}, "optional_features": {}, "state": {}, "quantile": {},
	"kind": {}, "outcome": {}, "resource": {}, "scope": {}, "direction": {},
}

// Source is the fast, in-process snapshot contract consumed during a scrape.
// *fusedb.DB implements Source. Implementations must not perform network or
// filesystem I/O from these methods.
type Source interface {
	Metrics() fusedb.MetricsSnapshot
	Stats() fusedb.Stats
	Health() fusedb.HealthStatus
	Format() fusedb.FormatInfo
}

// Options configures metric identity. ConstLabels should identify a bounded
// deployment dimension such as shard or role; never use keys, tenants, request
// IDs, paths, or error strings.
type Options struct {
	Namespace   string
	ConstLabels prom.Labels
}

var (
	ErrNilSource     = errors.New("fusedb prometheus: nil source")
	ErrNilRegisterer = errors.New("fusedb prometheus: nil registerer")
	ErrBadNamespace  = errors.New("fusedb prometheus: invalid namespace")
	ErrBadConstLabel = errors.New("fusedb prometheus: invalid or conflicting const label")
)

// Collector converts one FuseDB snapshot into Prometheus metric families.
type Collector struct {
	source Source

	ready                     *prom.Desc
	closed                    *prom.Desc
	terminal                  *prom.Desc
	formatInfo                *prom.Desc
	snapshotTimestamp         *prom.Desc
	latencySampleEvery        *prom.Desc
	operations                *prom.Desc
	operationDuration         *prom.Desc
	terminalErrors            *prom.Desc
	commitUncertainErrors     *prom.Desc
	bufferedBytes             *prom.Desc
	walBytes                  *prom.Desc
	leaves                    *prom.Desc
	pendingMergeLeaves        *prom.Desc
	schedulerState            *prom.Desc
	schedulerQueued           *prom.Desc
	schedulerRunning          *prom.Desc
	schedulerRequestRate      *prom.Desc
	schedulerLatency          *prom.Desc
	backgroundJobs            *prom.Desc
	backgroundActive          *prom.Desc
	backgroundDuration        *prom.Desc
	backgroundLastSuccess     *prom.Desc
	resourceUtilization       *prom.Desc
	resourceCPUCores          *prom.Desc
	resourceDiskRate          *prom.Desc
	resourceMemoryBytes       *prom.Desc
	resourceCapacityAutomatic *prom.Desc
	diskIOBytes               *prom.Desc

	descriptors []*prom.Desc
}

// NewCollector creates an unregistered collector. Registration remains an
// explicit application lifecycle decision.
func NewCollector(source Source, opts Options) (*Collector, error) {
	if isNil(source) {
		return nil, ErrNilSource
	}
	namespace := opts.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	if !metricNamespacePattern.MatchString(namespace) {
		return nil, ErrBadNamespace
	}
	labels := cloneLabels(opts.ConstLabels)
	for name := range labels {
		_, conflicts := reservedVariableLabels[name]
		if !labelNamePattern.MatchString(name) || strings.HasPrefix(name, "__") || conflicts {
			return nil, fmt.Errorf("%w: %q", ErrBadConstLabel, name)
		}
	}
	desc := func(name, help string, variableLabels ...string) *prom.Desc {
		return prom.NewDesc(prom.BuildFQName(namespace, "", name), help, variableLabels, labels)
	}

	c := &Collector{source: source}
	c.ready = desc("ready", "Whether this FuseDB handle may accept foreground traffic.")
	c.closed = desc("closed", "Whether this FuseDB handle has been closed.")
	c.terminal = desc("terminal_error", "Whether a terminal storage error has fenced this FuseDB handle.")
	c.formatInfo = desc("format_info", "Persisted FuseDB format and compatibility identity.", "epoch", "min_reader_epoch", "min_writer_epoch", "required_features", "optional_features")
	c.snapshotTimestamp = desc("metrics_snapshot_timestamp_seconds", "Unix timestamp at which the engine telemetry snapshot was read.")
	c.latencySampleEvery = desc("operation_latency_sample_every", "Configured latency sampling interval; operation counters remain unsampled.")
	c.operations = desc("operations_total", "Total foreground operations attempted, including operations rejected with an error.", "operation")
	c.operationDuration = desc("operation_duration_seconds", "Sampled foreground operation latency in seconds.", "operation")
	c.terminalErrors = desc("terminal_errors_total", "Total terminal errors observed by this database handle.")
	c.commitUncertainErrors = desc("commit_uncertain_errors_total", "Total terminal errors for which durable commit outcome required reopen and reconciliation.")
	c.bufferedBytes = desc("buffered_bytes", "Bytes of mutations currently buffered outside immutable segments.")
	c.walBytes = desc("wal_checkpoint_debt_bytes", "WAL bytes accumulated since the last exact checkpoint; not physical file size.")
	c.leaves = desc("leaves", "Current number of keyspace leaves.")
	c.pendingMergeLeaves = desc("pending_merge_leaves", "Leaves currently above the merge threshold or waiting for retry.")
	c.schedulerState = desc("scheduler_state", "Current adaptive scheduler state as a one-hot gauge.", "state")
	c.schedulerQueued = desc("scheduler_jobs_queued", "Current number of background jobs waiting for admission.")
	c.schedulerRunning = desc("scheduler_job_running", "Whether one background job is currently running.")
	c.schedulerRequestRate = desc("scheduler_request_rate", "Rolling foreground request rate observed by the scheduler in operations per second.")
	c.schedulerLatency = desc("scheduler_foreground_latency_seconds", "Rolling foreground latency estimate used by scheduler admission.", "operation", "quantile")
	c.backgroundJobs = desc("background_jobs_total", "Total background job lifecycle events.", "kind", "outcome")
	c.backgroundActive = desc("background_jobs_active", "Current active background jobs.", "kind")
	c.backgroundDuration = desc("background_job_duration_seconds_total", "Cumulative wall time consumed by background jobs.", "kind")
	c.backgroundLastSuccess = desc("background_job_last_success_timestamp_seconds", "Unix timestamp of the most recent successful background job; zero if none.", "kind")
	c.resourceUtilization = desc("resource_utilization_ratio", "Current scheduler resource utilization normalized to configured effective capacity.", "resource")
	c.resourceCPUCores = desc("resource_cpu_cores", "Observed or effective-capacity CPU cores.", "scope")
	c.resourceDiskRate = desc("resource_disk_bytes_per_second", "Observed or effective-capacity database disk throughput.", "scope")
	c.resourceMemoryBytes = desc("resource_memory_bytes", "Observed or effective-capacity memory bytes.", "scope")
	c.resourceCapacityAutomatic = desc("resource_capacity_automatic", "Whether effective resource capacity was detected or learned automatically.", "resource")
	c.diskIOBytes = desc("disk_io_bytes_total", "Cumulative bytes observed through the database filesystem.", "direction")
	c.descriptors = []*prom.Desc{
		c.ready, c.closed, c.terminal, c.formatInfo, c.snapshotTimestamp,
		c.latencySampleEvery, c.operations, c.operationDuration,
		c.terminalErrors, c.commitUncertainErrors, c.bufferedBytes, c.walBytes,
		c.leaves, c.pendingMergeLeaves, c.schedulerState, c.schedulerQueued,
		c.schedulerRunning, c.schedulerRequestRate, c.schedulerLatency,
		c.backgroundJobs, c.backgroundActive, c.backgroundDuration, c.backgroundLastSuccess,
		c.resourceUtilization, c.resourceCPUCores, c.resourceDiskRate,
		c.resourceMemoryBytes, c.resourceCapacityAutomatic, c.diskIOBytes,
	}
	return c, nil
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prom.Desc) {
	for _, descriptor := range c.descriptors {
		ch <- descriptor
	}
}

var schedulerStates = [...]string{
	"normal", "quiet-candidate", "quiet", "recovering", "busy", "overloaded", "unknown",
}

var backgroundKinds = [...]string{
	"merge", "checkpoint", "dictionary_train", "dictionary_evaluate",
	"dictionary_gc", "scheduler_model_persist", "verify", "backup",
}

// Collect implements prometheus.Collector. It reads only in-memory snapshots;
// a scrape never triggers maintenance, verification, or persistent I/O.
func (c *Collector) Collect(ch chan<- prom.Metric) {
	health := c.source.Health()
	metrics := c.source.Metrics()
	stats := c.source.Stats()
	format := c.source.Format()

	emit := func(desc *prom.Desc, valueType prom.ValueType, value float64, labels ...string) {
		ch <- prom.MustNewConstMetric(desc, valueType, finite(value), labels...)
	}
	emit(c.ready, prom.GaugeValue, boolean(health.Ready))
	emit(c.closed, prom.GaugeValue, boolean(health.Closed))
	emit(c.terminal, prom.GaugeValue, boolean(health.TerminalError))
	emit(c.formatInfo, prom.GaugeValue, 1,
		strconv.FormatUint(uint64(format.Epoch), 10),
		strconv.FormatUint(uint64(format.MinReaderEpoch), 10),
		strconv.FormatUint(uint64(format.MinWriterEpoch), 10),
		strconv.FormatUint(format.RequiredFeatures, 16),
		strconv.FormatUint(format.OptionalFeatures, 16),
	)
	emit(c.snapshotTimestamp, prom.GaugeValue, timestampSeconds(metrics.At))
	emit(c.latencySampleEvery, prom.GaugeValue, float64(metrics.LatencySampleEvery))
	emit(c.operations, prom.CounterValue, float64(metrics.ReadOps), "read")
	emit(c.operations, prom.CounterValue, float64(metrics.WriteOps), "write")
	c.collectHistogram(ch, metrics.ReadLatency, "read")
	c.collectHistogram(ch, metrics.WriteLatency, "write")
	emit(c.terminalErrors, prom.CounterValue, float64(metrics.TerminalErrors))
	emit(c.commitUncertainErrors, prom.CounterValue, float64(metrics.CommitUncertainErrors))
	emit(c.bufferedBytes, prom.GaugeValue, float64(max(0, stats.BufferedBytes)))
	emit(c.walBytes, prom.GaugeValue, float64(max(0, stats.WALBytes)))
	emit(c.leaves, prom.GaugeValue, float64(max(0, stats.Leaves)))
	emit(c.pendingMergeLeaves, prom.GaugeValue, float64(max(0, stats.PendingMergeLeaves)))

	knownState := false
	for _, state := range schedulerStates[:len(schedulerStates)-1] {
		active := stats.Scheduler == state
		knownState = knownState || active
		emit(c.schedulerState, prom.GaugeValue, boolean(active), state)
	}
	emit(c.schedulerState, prom.GaugeValue, boolean(!knownState), "unknown")
	emit(c.schedulerQueued, prom.GaugeValue, float64(max(0, stats.SchedulerQueued)))
	emit(c.schedulerRunning, prom.GaugeValue, boolean(stats.SchedulerRunning))
	emit(c.schedulerRequestRate, prom.GaugeValue, stats.RequestRate)
	emit(c.schedulerLatency, prom.GaugeValue, stats.ReadP95.Seconds(), "read", "0.95")
	emit(c.schedulerLatency, prom.GaugeValue, stats.ReadP99.Seconds(), "read", "0.99")
	emit(c.schedulerLatency, prom.GaugeValue, stats.WriteP95.Seconds(), "write", "0.95")
	emit(c.schedulerLatency, prom.GaugeValue, stats.WriteP99.Seconds(), "write", "0.99")

	background := make(map[string]fusedb.BackgroundMetrics, len(metrics.Background))
	for _, item := range metrics.Background {
		background[item.Kind] = item
	}
	for _, kind := range backgroundKinds {
		item := background[kind]
		emit(c.backgroundJobs, prom.CounterValue, float64(item.Started), kind, "started")
		emit(c.backgroundJobs, prom.CounterValue, float64(item.Completed), kind, "completed")
		emit(c.backgroundJobs, prom.CounterValue, float64(item.Failed), kind, "failed")
		emit(c.backgroundJobs, prom.CounterValue, float64(item.Cancelled), kind, "cancelled")
		emit(c.backgroundActive, prom.GaugeValue, float64(item.Active), kind)
		emit(c.backgroundDuration, prom.CounterValue, max(0, item.TotalTime.Seconds()), kind)
		emit(c.backgroundLastSuccess, prom.GaugeValue, timestampSeconds(item.LastSuccess), kind)
	}

	resources := metrics.Resources
	emit(c.resourceUtilization, prom.GaugeValue, resources.CPUUtilization, "cpu")
	emit(c.resourceUtilization, prom.GaugeValue, resources.DiskUtilization, "disk")
	emit(c.resourceUtilization, prom.GaugeValue, resources.MemoryUtilization, "memory")
	emit(c.resourceCPUCores, prom.GaugeValue, resources.CPUCores, "used")
	emit(c.resourceCPUCores, prom.GaugeValue, resources.Capacity.CPUCores, "capacity")
	emit(c.resourceDiskRate, prom.GaugeValue, resources.DiskBytesPerSecond, "used")
	emit(c.resourceDiskRate, prom.GaugeValue, resources.Capacity.DiskBytesPerSecond, "capacity")
	emit(c.resourceMemoryBytes, prom.GaugeValue, float64(resources.MemoryBytes), "used")
	emit(c.resourceMemoryBytes, prom.GaugeValue, float64(resources.Capacity.MemoryBytes), "capacity")
	emit(c.resourceCapacityAutomatic, prom.GaugeValue, boolean(resources.Capacity.CPUAutomatic), "cpu")
	emit(c.resourceCapacityAutomatic, prom.GaugeValue, boolean(resources.Capacity.DiskAutomatic), "disk")
	emit(c.resourceCapacityAutomatic, prom.GaugeValue, boolean(resources.Capacity.MemoryAutomatic), "memory")
	emit(c.diskIOBytes, prom.CounterValue, float64(resources.DiskReadBytes), "read")
	emit(c.diskIOBytes, prom.CounterValue, float64(resources.DiskWriteBytes), "write")
}

func (c *Collector) collectHistogram(ch chan<- prom.Metric, histogram fusedb.LatencyHistogram, operation string) {
	length := min(len(histogram.Bounds), len(histogram.Counts))
	buckets := make(map[float64]uint64, length)
	var cumulative uint64
	lastBound := math.Inf(-1)
	for i := range length {
		count := histogram.Counts[i]
		if math.MaxUint64-cumulative < count {
			cumulative = math.MaxUint64
		} else {
			cumulative += count
		}
		bound := histogram.Bounds[i].Seconds()
		if !math.IsNaN(bound) && !math.IsInf(bound, 0) && bound > lastBound {
			buckets[bound] = cumulative
			lastBound = bound
		}
	}
	observed := cumulative
	if math.MaxUint64-observed < histogram.Overflow {
		observed = math.MaxUint64
	} else {
		observed += histogram.Overflow
	}
	count := max(histogram.Count, observed)
	sum := max(0, histogram.Sum.Seconds())
	ch <- prom.MustNewConstHistogram(c.operationDuration, count, sum, buckets, operation)
}

// Registration owns one collector's registration lifecycle.
type Registration struct {
	registerer prom.Registerer
	collector  *Collector
	once       sync.Once
	removed    bool
}

// Register creates and registers a collector with the supplied registry. It
// never falls back to prometheus.DefaultRegisterer. Duplicate registrations
// are returned to the caller for explicit resolution.
func Register(registerer prom.Registerer, source Source, opts Options) (*Registration, error) {
	if isNil(registerer) {
		return nil, ErrNilRegisterer
	}
	collector, err := NewCollector(source, opts)
	if err != nil {
		return nil, err
	}
	if err := registerer.Register(collector); err != nil {
		return nil, err
	}
	return &Registration{registerer: registerer, collector: collector}, nil
}

// Collector returns the registered collector.
func (r *Registration) Collector() *Collector {
	if r == nil {
		return nil
	}
	return r.collector
}

// Close unregisters the collector once. It reports whether the registry
// contained and removed it; subsequent calls return the same result.
func (r *Registration) Close() bool {
	if r == nil {
		return false
	}
	r.once.Do(func() {
		r.removed = r.registerer.Unregister(r.collector)
	})
	return r.removed
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func cloneLabels(labels prom.Labels) prom.Labels {
	if len(labels) == 0 {
		return nil
	}
	clone := make(prom.Labels, len(labels))
	for name, value := range labels {
		clone[name] = value
	}
	return clone
}

func boolean(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func finite(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func timestampSeconds(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.UnixNano()) / float64(time.Second)
}
