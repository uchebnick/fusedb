package prometheus

import (
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/uchebnick/fusedb/pkg/fusedb"
)

type fakeSource struct {
	metrics fusedb.MetricsSnapshot
	stats   fusedb.Stats
	health  fusedb.HealthStatus
	format  fusedb.FormatInfo
}

func (s *fakeSource) Metrics() fusedb.MetricsSnapshot { return s.metrics }
func (s *fakeSource) Stats() fusedb.Stats             { return s.stats }
func (s *fakeSource) Health() fusedb.HealthStatus     { return s.health }
func (s *fakeSource) Format() fusedb.FormatInfo       { return s.format }

func TestCollectorExportsStableSemantics(t *testing.T) {
	source := representativeSource()
	collector, err := NewCollector(source, Options{ConstLabels: prom.Labels{"database": "primary"}})
	if err != nil {
		t.Fatal(err)
	}
	registry := prom.NewPedanticRegistry()
	if err := registry.Register(collector); err != nil {
		t.Fatalf("register: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	operations := findFamily(t, families, "fusedb_operations_total")
	if got := counterValue(t, findMetric(t, operations, map[string]string{"database": "primary", "operation": "read"})); got != 11 {
		t.Fatalf("read operations = %v, want 11", got)
	}
	if got := counterValue(t, findMetric(t, operations, map[string]string{"database": "primary", "operation": "write"})); got != 7 {
		t.Fatalf("write operations = %v, want 7", got)
	}

	durations := findFamily(t, families, "fusedb_operation_duration_seconds")
	read := findMetric(t, durations, map[string]string{"database": "primary", "operation": "read"})
	if got := read.GetHistogram().GetSampleCount(); got != 3 {
		t.Fatalf("read histogram count = %d, want 3", got)
	}
	if got := read.GetHistogram().GetSampleSum(); math.Abs(got-0.004) > 1e-12 {
		t.Fatalf("read histogram sum = %v, want 0.004", got)
	}
	if buckets := read.GetHistogram().GetBucket(); len(buckets) != 2 || buckets[0].GetCumulativeCount() != 2 || buckets[1].GetCumulativeCount() != 3 {
		t.Fatalf("read histogram buckets = %+v", buckets)
	}

	states := findFamily(t, families, "fusedb_scheduler_state")
	if len(states.Metric) != len(schedulerStates) {
		t.Fatalf("scheduler states = %d, want %d", len(states.Metric), len(schedulerStates))
	}
	if got := gaugeValue(t, findMetric(t, states, map[string]string{"database": "primary", "state": "overloaded"})); got != 1 {
		t.Fatalf("overloaded = %v, want 1", got)
	}
	if got := gaugeValue(t, findMetric(t, states, map[string]string{"database": "primary", "state": "unknown"})); got != 0 {
		t.Fatalf("unknown = %v, want 0", got)
	}

	jobs := findFamily(t, families, "fusedb_background_jobs_total")
	if len(jobs.Metric) != len(backgroundKinds)*4 {
		t.Fatalf("background job series = %d, want %d", len(jobs.Metric), len(backgroundKinds)*4)
	}
	for _, metric := range jobs.Metric {
		if labels(metric)["kind"] == "unbounded-user-value" {
			t.Fatal("collector exported an unknown high-cardinality background kind")
		}
	}
	if got := counterValue(t, findMetric(t, jobs, map[string]string{"database": "primary", "kind": "merge", "outcome": "completed"})); got != 4 {
		t.Fatalf("completed merges = %v, want 4", got)
	}

	if got := gaugeValue(t, findMetric(t, findFamily(t, families, "fusedb_ready"), map[string]string{"database": "primary"})); got != 1 {
		t.Fatalf("ready = %v, want 1", got)
	}
	if got := gaugeValue(t, findMetric(t, findFamily(t, families, "fusedb_wal_checkpoint_debt_bytes"), map[string]string{"database": "primary"})); got != 8192 {
		t.Fatalf("WAL debt = %v, want 8192", got)
	}
	if got := gaugeValue(t, findMetric(t, findFamily(t, families, "fusedb_resource_capacity_automatic"), map[string]string{"database": "primary", "resource": "disk"})); got != 1 {
		t.Fatalf("automatic disk capacity = %v, want 1", got)
	}
}

func TestCollectorUnknownStateAndMalformedHistogramRemainGatherable(t *testing.T) {
	source := representativeSource()
	source.stats.Scheduler = "caller-controlled-state"
	source.metrics.ReadLatency = fusedb.LatencyHistogram{
		Bounds:   []time.Duration{2 * time.Millisecond, time.Millisecond, 4 * time.Millisecond},
		Counts:   []uint64{2, 3, 5},
		Overflow: 2,
		Count:    1,
		Sum:      -time.Second,
	}
	collector, err := NewCollector(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	registry := prom.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather malformed source: %v", err)
	}
	states := findFamily(t, families, "fusedb_scheduler_state")
	if got := gaugeValue(t, findMetric(t, states, map[string]string{"state": "unknown"})); got != 1 {
		t.Fatalf("unknown state = %v, want 1", got)
	}
	durations := findFamily(t, families, "fusedb_operation_duration_seconds")
	read := findMetric(t, durations, map[string]string{"operation": "read"}).GetHistogram()
	if read.GetSampleCount() != 12 || read.GetSampleSum() != 0 {
		t.Fatalf("sanitized histogram = count:%d sum:%v", read.GetSampleCount(), read.GetSampleSum())
	}
}

func TestRegistrationLifecycleAndValidation(t *testing.T) {
	source := representativeSource()
	if _, err := NewCollector(nil, Options{}); !errors.Is(err, ErrNilSource) {
		t.Fatalf("nil source error = %v", err)
	}
	var typedNilSource *fakeSource
	if _, err := NewCollector(typedNilSource, Options{}); !errors.Is(err, ErrNilSource) {
		t.Fatalf("typed nil source error = %v", err)
	}
	if _, err := NewCollector(source, Options{Namespace: "bad namespace"}); !errors.Is(err, ErrBadNamespace) {
		t.Fatalf("bad namespace error = %v", err)
	}
	for _, labels := range []prom.Labels{{"bad-label": "x"}, {"__reserved": "x"}, {"operation": "x"}} {
		if _, err := NewCollector(source, Options{ConstLabels: labels}); !errors.Is(err, ErrBadConstLabel) {
			t.Fatalf("bad const labels %v error = %v", labels, err)
		}
	}
	if _, err := Register(nil, source, Options{}); !errors.Is(err, ErrNilRegisterer) {
		t.Fatalf("nil registerer error = %v", err)
	}
	var typedNilRegistry *prom.Registry
	if _, err := Register(typedNilRegistry, source, Options{}); !errors.Is(err, ErrNilRegisterer) {
		t.Fatalf("typed nil registerer error = %v", err)
	}

	registry := prom.NewPedanticRegistry()
	registration, err := Register(registry, source, Options{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := Register(registry, source, Options{}); err == nil {
		t.Fatal("duplicate registration succeeded")
	} else {
		var duplicate prom.AlreadyRegisteredError
		if !errors.As(err, &duplicate) {
			t.Fatalf("duplicate error = %T %v", err, err)
		}
	}
	firstClose := registration.Close()
	secondClose := registration.Close()
	if !firstClose || !secondClose {
		t.Fatal("registration Close was not idempotently successful")
	}
	second, err := Register(registry, source, Options{})
	if err != nil {
		t.Fatalf("register after close: %v", err)
	}
	if !second.Close() {
		t.Fatal("second registration was not removed")
	}
}

func TestRegistrySupportsMultipleDatabasesWithStableConstLabel(t *testing.T) {
	registry := prom.NewPedanticRegistry()
	constLabels := prom.Labels{"database": "primary"}
	primary, err := Register(registry, representativeSource(), Options{ConstLabels: constLabels})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	// Construction copies the map; later application mutation cannot silently
	// change descriptor identity or time-series labels.
	constLabels["database"] = "mutated"
	secondary, err := Register(registry, representativeSource(), Options{
		ConstLabels: prom.Labels{"database": "secondary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer secondary.Close()

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	ready := findFamily(t, families, "fusedb_ready")
	if len(ready.Metric) != 2 {
		t.Fatalf("ready series = %d, want 2", len(ready.Metric))
	}
	findMetric(t, ready, map[string]string{"database": "primary"})
	findMetric(t, ready, map[string]string{"database": "secondary"})
	for _, metric := range ready.Metric {
		if labels(metric)["database"] == "mutated" {
			t.Fatal("collector retained caller-owned const-label map")
		}
	}
}

func TestCollectorConcurrentGather(t *testing.T) {
	collector, err := NewCollector(representativeSource(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	registry := prom.NewPedanticRegistry()
	registry.MustRegister(collector)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if _, err := registry.Gather(); err != nil {
					t.Errorf("gather: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestCollectorTracksRealDBReadiness(t *testing.T) {
	db, err := fusedb.Open(fusedb.Options{
		Dir:           filepath.Join(t.TempDir(), "db"),
		WALSyncWrites: true,
		Metrics:       fusedb.MetricsOptions{LatencySampleEvery: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	collector, err := NewCollector(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	registry := prom.NewPedanticRegistry()
	registry.MustRegister(collector)
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Get([]byte("key")); err != nil {
		t.Fatal(err)
	}
	assertGatheredGauge(t, registry, "fusedb_ready", 1)
	assertGatheredCounter(t, registry, "fusedb_operations_total", map[string]string{"operation": "read"}, 1)
	assertGatheredCounter(t, registry, "fusedb_operations_total", map[string]string{"operation": "write"}, 1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertGatheredGauge(t, registry, "fusedb_ready", 0)
	assertGatheredGauge(t, registry, "fusedb_closed", 1)
}

func representativeSource() *fakeSource {
	return &fakeSource{
		metrics: fusedb.MetricsSnapshot{
			At:                 time.Unix(1700000000, 250000000),
			LatencySampleEvery: 4,
			ReadOps:            11,
			WriteOps:           7,
			ReadLatency: fusedb.LatencyHistogram{
				Bounds: []time.Duration{time.Millisecond, 2 * time.Millisecond},
				Counts: []uint64{2, 1},
				Count:  3,
				Sum:    4 * time.Millisecond,
			},
			WriteLatency: fusedb.LatencyHistogram{
				Bounds: []time.Duration{time.Millisecond}, Counts: []uint64{1}, Count: 1, Sum: time.Millisecond,
			},
			TerminalErrors:        2,
			CommitUncertainErrors: 1,
			Background: []fusedb.BackgroundMetrics{
				{Kind: "merge", Started: 5, Completed: 4, Failed: 1, Active: 1, TotalTime: 3 * time.Second, LastSuccess: time.Unix(1699999990, 0)},
				{Kind: "unbounded-user-value", Started: 99},
			},
			Resources: fusedb.ResourceMetrics{
				CPUUtilization: 0.5, DiskUtilization: 0.7, MemoryUtilization: 0.8,
				CPUCores: 2, DiskBytesPerSecond: 10_000, MemoryBytes: 2048,
				DiskReadBytes: 100, DiskWriteBytes: 200,
				Capacity: fusedb.ResourceCapacity{
					CPUCores: 4, DiskBytesPerSecond: 20_000, MemoryBytes: 4096,
					DiskAutomatic: true,
				},
			},
		},
		stats: fusedb.Stats{
			BufferedBytes: 4096, WALBytes: 8192, Leaves: 3, PendingMergeLeaves: 2,
			Scheduler: "overloaded", SchedulerQueued: 2, SchedulerRunning: true,
			RequestRate: 123, ReadP95: time.Millisecond, ReadP99: 2 * time.Millisecond,
			WriteP95: 3 * time.Millisecond, WriteP99: 4 * time.Millisecond,
		},
		health: fusedb.HealthStatus{Ready: true},
		format: fusedb.FormatInfo{
			Epoch: 2, MinReaderEpoch: 2, MinWriterEpoch: 2, RequiredFeatures: 31, OptionalFeatures: 1,
		},
	}
}

func assertGatheredGauge(t *testing.T, registry *prom.Registry, name string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if got := gaugeValue(t, findMetric(t, findFamily(t, families, name), nil)); got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertGatheredCounter(t *testing.T, registry *prom.Registry, name string, labels map[string]string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, findMetric(t, findFamily(t, families, name), labels)); got != want {
		t.Fatalf("%s%v = %v, want %v", name, labels, got, want)
	}
}

func findFamily(t *testing.T, families []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func findMetric(t *testing.T, family *dto.MetricFamily, want map[string]string) *dto.Metric {
	t.Helper()
	for _, metric := range family.Metric {
		got := labels(metric)
		matches := true
		for name, value := range want {
			if got[name] != value {
				matches = false
				break
			}
		}
		if matches {
			return metric
		}
	}
	t.Fatalf("metric in %q with labels %v not found", family.GetName(), want)
	return nil
}

func labels(metric *dto.Metric) map[string]string {
	result := make(map[string]string, len(metric.Label))
	for _, pair := range metric.Label {
		result[pair.GetName()] = pair.GetValue()
	}
	return result
}

func gaugeValue(t *testing.T, metric *dto.Metric) float64 {
	t.Helper()
	if metric.Gauge == nil {
		t.Fatal("metric is not a gauge")
	}
	return metric.Gauge.GetValue()
}

func counterValue(t *testing.T, metric *dto.Metric) float64 {
	t.Helper()
	if metric.Counter == nil {
		t.Fatal("metric is not a counter")
	}
	return metric.Counter.GetValue()
}
