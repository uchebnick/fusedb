package metrics

import (
	"runtime"
	"testing"
	"time"
)

func TestHistogramSnapshotQuantilesAndDelta(t *testing.T) {
	var h latencyHistogram
	for i := 1; i <= 100; i++ {
		h.observe(time.Duration(i) * time.Microsecond)
	}
	first := h.snapshot()
	if first.SumNanos != uint64(5050*time.Microsecond) {
		t.Fatalf("sum = %s, want 5.05ms", time.Duration(first.SumNanos))
	}
	if got := first.Quantile(0.95); got < 90*time.Microsecond || got > 130*time.Microsecond {
		t.Fatalf("p95 = %s", got)
	}

	for range 10 {
		h.observe(time.Millisecond)
	}
	delta := h.snapshot().Sub(first)
	if delta.Count != 10 {
		t.Fatalf("delta count = %d, want 10", delta.Count)
	}
	if delta.SumNanos != uint64(10*time.Millisecond) {
		t.Fatalf("delta sum = %s, want 10ms", time.Duration(delta.SumNanos))
	}
	if got := delta.Quantile(0.99); got < time.Millisecond {
		t.Fatalf("delta p99 = %s, want at least 1ms", got)
	}
}

func TestHistogramRetainsOverflowOutsideFiniteBuckets(t *testing.T) {
	var h latencyHistogram
	h.observe(latencyBounds[len(latencyBounds)-1] + time.Second)
	snapshot := h.snapshot()
	if snapshot.Count != 1 || snapshot.Overflow != 1 {
		t.Fatalf("overflow snapshot = %+v", snapshot)
	}
	for i, count := range snapshot.Buckets {
		if count != 0 {
			t.Fatalf("finite bucket %d contains overflow observation", i)
		}
	}
}

func TestAutomaticResourceCapacityAndObservedUsage(t *testing.T) {
	capacity := detectCapacity(ResourceConfig{})
	if !capacity.CPUAutomatic || capacity.CPUCores != float64(runtime.GOMAXPROCS(0)) {
		t.Fatalf("automatic CPU capacity = %+v", capacity)
	}
	if !capacity.MemoryAutomatic || capacity.MemoryBytes == 0 {
		t.Fatalf("automatic memory capacity = %+v", capacity)
	}
	if !capacity.DiskAutomatic || capacity.DiskBytesPerSecond != 0 {
		t.Fatalf("initial automatic disk capacity = %+v", capacity)
	}

	recorder := NewRecorder(Config{})
	m := &ResourceMonitor{
		recorder: recorder,
		capacity: ResourceCapacity{
			CPUCores:        2,
			MemoryBytes:     1000,
			DiskAutomatic:   true,
			CPUAutomatic:    false,
			MemoryAutomatic: false,
		},
		previous: resourcePoint{at: time.Unix(1, 0), cpuTotal: 10, cpuIdle: 4, diskBytes: 100},
	}
	m.observe(resourcePoint{at: time.Unix(2, 0), cpuTotal: 12, cpuIdle: 4.5, diskBytes: 1100}, 500)

	resources := recorder.Snapshot().Resources
	if resources.CPUCores != 1.5 || resources.CPUUtilization != 0.75 {
		t.Fatalf("CPU sample = %+v", resources)
	}
	if resources.DiskBytesPerSecond != 1000 || resources.DiskUtilization != 0.8 {
		t.Fatalf("disk sample = %+v", resources)
	}
	if resources.MemoryBytes != 500 || resources.MemoryUtilization != 0.5 {
		t.Fatalf("memory sample = %+v", resources)
	}
	if resources.Capacity.DiskBytesPerSecond != 1250 {
		t.Fatalf("learned disk capacity = %+v", resources.Capacity)
	}
}

func TestRecorderSamplingAndBackgroundMetrics(t *testing.T) {
	r := NewRecorder(Config{LatencySampleEvery: 2})
	for range 4 {
		start := r.BeginRead()
		time.Sleep(time.Microsecond)
		r.EndRead(start)
	}
	start := r.BackgroundStarted(BackgroundDictionaryTrain)
	r.BackgroundFinished(BackgroundDictionaryTrain, start, false, true)
	start = r.BackgroundStarted(BackgroundMerge)
	r.BackgroundFinished(BackgroundMerge, start, false, false)
	r.SetResourceUtilization(1.5, -1, 0.75)
	r.RecordTerminalError(true)

	snapshot := r.Snapshot()
	if snapshot.ReadOps != 4 || snapshot.ReadLatency.Count != 2 || snapshot.LatencySampleEvery != 2 {
		t.Fatalf("read snapshot = ops:%d samples:%d", snapshot.ReadOps, snapshot.ReadLatency.Count)
	}
	if snapshot.BackgroundStarted[BackgroundDictionaryTrain] != 1 ||
		snapshot.BackgroundCancelled[BackgroundDictionaryTrain] != 1 ||
		snapshot.ActiveBackground[BackgroundDictionaryTrain] != 0 {
		t.Fatalf("background snapshot = %+v", snapshot)
	}
	if snapshot.BackgroundLastSuccessUnixNanos[BackgroundDictionaryTrain] != 0 {
		t.Fatalf("cancelled job has last-success timestamp: %+v", snapshot)
	}
	if snapshot.BackgroundLastSuccessUnixNanos[BackgroundMerge] == 0 {
		t.Fatalf("successful job has no last-success timestamp: %+v", snapshot)
	}
	if snapshot.TerminalErrors != 1 || snapshot.CommitUncertainErrors != 1 {
		t.Fatalf("terminal errors = (%d,%d)", snapshot.TerminalErrors, snapshot.CommitUncertainErrors)
	}
	if snapshot.Resources.CPUUtilization != 1 || snapshot.Resources.DiskUtilization != 0 || snapshot.Resources.MemoryUtilization != 0.75 {
		t.Fatalf("resources = %+v", snapshot.Resources)
	}
}
