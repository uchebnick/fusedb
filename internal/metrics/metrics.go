package metrics

import (
	"math"
	"sync/atomic"
	"time"
)

// BackgroundKind identifies resource-consuming maintenance work.
type BackgroundKind uint8

const (
	BackgroundMerge BackgroundKind = iota
	BackgroundCheckpoint
	BackgroundDictionaryTrain
	BackgroundDictionaryEvaluate
	BackgroundDictionaryGC
	BackgroundSchedulerModelPersist
	BackgroundVerify
	BackgroundBackup
	backgroundKindCount
)

// Config controls low-overhead foreground latency sampling.
type Config struct {
	// LatencySampleEvery records one latency for every N operations. Values <= 1
	// record every operation. Counters are never sampled.
	LatencySampleEvery uint64
}

// Snapshot is a cumulative point-in-time view. Its monotonic counters and
// histograms map directly to Prometheus counters/histograms; ActiveBackground
// maps to gauges.
type Snapshot struct {
	At time.Time
	// LatencySampleEvery is the configured foreground sampling interval. The
	// operation counters are unsampled; latency histograms contain every Nth
	// observation.
	LatencySampleEvery uint64

	ReadOps  uint64
	WriteOps uint64

	ReadLatency  HistogramSnapshot
	WriteLatency HistogramSnapshot
	// TerminalErrors counts failures that poison the current database handle.
	// CommitUncertainErrors is the subset whose durable outcome requires reopen
	// and reconciliation.
	TerminalErrors        uint64
	CommitUncertainErrors uint64

	BackgroundStarted              [backgroundKindCount]uint64
	BackgroundCompleted            [backgroundKindCount]uint64
	BackgroundFailed               [backgroundKindCount]uint64
	BackgroundCancelled            [backgroundKindCount]uint64
	ActiveBackground               [backgroundKindCount]int64
	BackgroundNanos                [backgroundKindCount]uint64
	BackgroundLastSuccessUnixNanos [backgroundKindCount]int64
	Resources                      ResourceSnapshot
}

// ResourceSnapshot contains absolute resource usage, capacity, cumulative disk
// bytes, and normalized [0,1] utilization signals.
type ResourceSnapshot struct {
	CPUUtilization    float64
	DiskUtilization   float64
	MemoryUtilization float64

	CPUCores           float64
	DiskBytesPerSecond float64
	MemoryBytes        uint64
	DiskReadBytes      uint64
	DiskWriteBytes     uint64
	Capacity           ResourceCapacity
}

// ResourceCapacity is the effective budget available to one scheduler. Zero
// configuration values are detected from the runtime or learned online.
type ResourceCapacity struct {
	CPUCores           float64
	DiskBytesPerSecond float64
	MemoryBytes        uint64
	CPUAutomatic       bool
	DiskAutomatic      bool
	MemoryAutomatic    bool
}

// Recorder owns dependency-free process metrics for one database.
type Recorder struct {
	sampleEvery uint64
	readSample  atomic.Uint64
	writeSample atomic.Uint64

	readOps  atomic.Uint64
	writeOps atomic.Uint64

	readLatency           latencyHistogram
	writeLatency          latencyHistogram
	terminalErrors        atomic.Uint64
	commitUncertainErrors atomic.Uint64

	backgroundStarted              [backgroundKindCount]atomic.Uint64
	backgroundCompleted            [backgroundKindCount]atomic.Uint64
	backgroundFailed               [backgroundKindCount]atomic.Uint64
	backgroundCancelled            [backgroundKindCount]atomic.Uint64
	activeBackground               [backgroundKindCount]atomic.Int64
	backgroundNanos                [backgroundKindCount]atomic.Uint64
	backgroundLastSuccessUnixNanos [backgroundKindCount]atomic.Int64
	diskReadBytes                  atomic.Uint64
	diskWriteBytes                 atomic.Uint64
	cpuBits                        atomic.Uint64
	diskBits                       atomic.Uint64
	memoryBits                     atomic.Uint64
	cpuCoresBits                   atomic.Uint64
	diskRateBits                   atomic.Uint64
	memoryBytes                    atomic.Uint64
	capacity                       atomic.Pointer[ResourceCapacity]
}

// NewRecorder creates an isolated telemetry recorder.
func NewRecorder(cfg Config) *Recorder {
	every := cfg.LatencySampleEvery
	if every == 0 {
		every = 64
	}
	return &Recorder{sampleEvery: every}
}

// BeginRead returns a sampling timestamp or the zero time when this operation
// was not selected. EndRead must be called exactly once.
func (r *Recorder) BeginRead() time.Time {
	if r == nil || !r.shouldSample(&r.readSample) {
		return time.Time{}
	}
	return time.Now()
}

// EndRead records one completed read and its sampled latency.
func (r *Recorder) EndRead(start time.Time) {
	if r == nil {
		return
	}
	r.readOps.Add(1)
	if !start.IsZero() {
		r.readLatency.observe(time.Since(start))
	}
}

// BeginWrite returns a sampling timestamp or the zero time when this operation
// was not selected. EndWrite must be called exactly once.
func (r *Recorder) BeginWrite() time.Time {
	if r == nil || !r.shouldSample(&r.writeSample) {
		return time.Time{}
	}
	return time.Now()
}

// EndWrite records one completed mutation and its sampled latency.
func (r *Recorder) EndWrite(start time.Time) {
	if r == nil {
		return
	}
	r.writeOps.Add(1)
	if !start.IsZero() {
		r.writeLatency.observe(time.Since(start))
	}
}

// RecordTerminalError records the first-class health event that makes a
// database handle unusable until reopen.
func (r *Recorder) RecordTerminalError(commitUncertain bool) {
	if r == nil {
		return
	}
	r.terminalErrors.Add(1)
	if commitUncertain {
		r.commitUncertainErrors.Add(1)
	}
}

// BackgroundStarted records admission of a background job.
func (r *Recorder) BackgroundStarted(kind BackgroundKind) time.Time {
	if r == nil || kind >= backgroundKindCount {
		return time.Time{}
	}
	r.backgroundStarted[kind].Add(1)
	r.activeBackground[kind].Add(1)
	return time.Now()
}

// BackgroundFinished records completion, cancellation, and consumed wall time.
func (r *Recorder) BackgroundFinished(kind BackgroundKind, start time.Time, err, cancelled bool) {
	if r == nil || kind >= backgroundKindCount {
		return
	}
	r.activeBackground[kind].Add(-1)
	if !start.IsZero() {
		r.backgroundNanos[kind].Add(uint64(time.Since(start)))
	}
	switch {
	case cancelled:
		r.backgroundCancelled[kind].Add(1)
	case err:
		r.backgroundFailed[kind].Add(1)
	default:
		r.backgroundCompleted[kind].Add(1)
		r.backgroundLastSuccessUnixNanos[kind].Store(time.Now().UnixNano())
	}
}

// SetResourceUtilization publishes normalized process or host resource signals
// for scheduler admission and monitoring exporters.
func (r *Recorder) SetResourceUtilization(cpu, disk, memory float64) {
	if r == nil {
		return
	}
	r.cpuBits.Store(math.Float64bits(clampUtilization(cpu)))
	r.diskBits.Store(math.Float64bits(clampUtilization(disk)))
	r.memoryBits.Store(math.Float64bits(clampUtilization(memory)))
}

// AddDiskReadBytes records bytes returned by the database filesystem.
func (r *Recorder) AddDiskReadBytes(n int) {
	if r != nil && n > 0 {
		r.diskReadBytes.Add(uint64(n))
	}
}

// AddDiskWriteBytes records bytes accepted by the database filesystem.
func (r *Recorder) AddDiskWriteBytes(n int) {
	if r != nil && n > 0 {
		r.diskWriteBytes.Add(uint64(n))
	}
}

// SetResourceSample publishes absolute usage and its effective capacity.
func (r *Recorder) SetResourceSample(cpuCores, diskBytesPerSecond float64, memoryBytes uint64, capacity ResourceCapacity) {
	if r == nil {
		return
	}
	r.cpuCoresBits.Store(math.Float64bits(max(0, cpuCores)))
	r.diskRateBits.Store(math.Float64bits(max(0, diskBytesPerSecond)))
	r.memoryBytes.Store(memoryBytes)
	r.SetResourceUtilization(
		ratio(cpuCores, capacity.CPUCores),
		ratio(diskBytesPerSecond, capacity.DiskBytesPerSecond),
		ratio(float64(memoryBytes), float64(capacity.MemoryBytes)),
	)
	copy := capacity
	r.capacity.Store(&copy)
}

// Snapshot reads all metrics without stopping foreground operations.
func (r *Recorder) Snapshot() Snapshot {
	out := Snapshot{At: time.Now()}
	if r == nil {
		return out
	}
	out.ReadOps = r.readOps.Load()
	out.WriteOps = r.writeOps.Load()
	out.LatencySampleEvery = r.sampleEvery
	out.TerminalErrors = r.terminalErrors.Load()
	out.CommitUncertainErrors = r.commitUncertainErrors.Load()
	out.ReadLatency = r.readLatency.snapshot()
	out.WriteLatency = r.writeLatency.snapshot()
	for i := BackgroundKind(0); i < backgroundKindCount; i++ {
		out.BackgroundStarted[i] = r.backgroundStarted[i].Load()
		out.BackgroundCompleted[i] = r.backgroundCompleted[i].Load()
		out.BackgroundFailed[i] = r.backgroundFailed[i].Load()
		out.BackgroundCancelled[i] = r.backgroundCancelled[i].Load()
		out.ActiveBackground[i] = r.activeBackground[i].Load()
		out.BackgroundNanos[i] = r.backgroundNanos[i].Load()
		out.BackgroundLastSuccessUnixNanos[i] = r.backgroundLastSuccessUnixNanos[i].Load()
	}
	out.Resources = ResourceSnapshot{
		CPUUtilization:     math.Float64frombits(r.cpuBits.Load()),
		DiskUtilization:    math.Float64frombits(r.diskBits.Load()),
		MemoryUtilization:  math.Float64frombits(r.memoryBits.Load()),
		CPUCores:           math.Float64frombits(r.cpuCoresBits.Load()),
		DiskBytesPerSecond: math.Float64frombits(r.diskRateBits.Load()),
		MemoryBytes:        r.memoryBytes.Load(),
		DiskReadBytes:      r.diskReadBytes.Load(),
		DiskWriteBytes:     r.diskWriteBytes.Load(),
	}
	if capacity := r.capacity.Load(); capacity != nil {
		out.Resources.Capacity = *capacity
	}
	return out
}

func (r *Recorder) shouldSample(counter *atomic.Uint64) bool {
	sequence := counter.Add(1)
	return r.sampleEvery <= 1 || sequence%r.sampleEvery == 0
}

func clampUtilization(value float64) float64 {
	if value < 0 || math.IsNaN(value) {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func ratio(used, capacity float64) float64 {
	if capacity <= 0 {
		return 0
	}
	return clampUtilization(used / capacity)
}
