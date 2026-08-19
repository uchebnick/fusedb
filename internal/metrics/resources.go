package metrics

import (
	"math"
	"runtime"
	"runtime/debug"
	runtimemetrics "runtime/metrics"
	"sync"
	"time"
)

const diskLearningHeadroom = 1.25

// ResourceConfig sets absolute scheduler capacity. Zero values are automatic:
// CPU follows GOMAXPROCS, memory follows the smaller runtime/physical limit,
// and disk throughput learns the highest effective rate observed by this DB.
type ResourceConfig struct {
	CPUCores           float64
	DiskBytesPerSecond float64
	MemoryBytes        uint64
	SampleInterval     time.Duration
}

// ResourceMonitor samples process resources until Close.
type ResourceMonitor struct {
	recorder *Recorder
	capacity ResourceCapacity
	interval time.Duration

	mu       sync.Mutex
	previous resourcePoint
	stop     chan struct{}
	done     chan struct{}
	close    sync.Once
}

type resourcePoint struct {
	at        time.Time
	cpuTotal  float64
	cpuIdle   float64
	diskBytes uint64
}

// StartResourceMonitor detects effective capacity and starts sampling.
func StartResourceMonitor(recorder *Recorder, cfg ResourceConfig) *ResourceMonitor {
	if recorder == nil {
		return nil
	}
	interval := cfg.SampleInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	m := &ResourceMonitor{
		recorder: recorder,
		capacity: detectCapacity(cfg),
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	total, idle, memory := readRuntimeResources()
	snapshot := recorder.Snapshot().Resources
	m.previous = resourcePoint{
		at:        time.Now(),
		cpuTotal:  total,
		cpuIdle:   idle,
		diskBytes: snapshot.DiskReadBytes + snapshot.DiskWriteBytes,
	}
	recorder.SetResourceSample(0, 0, memory, m.capacity)
	go m.loop()
	return m
}

// Capacity returns the currently effective budget. Automatic disk capacity may
// grow as the engine observes higher throughput.
func (m *ResourceMonitor) Capacity() ResourceCapacity {
	if m == nil {
		return ResourceCapacity{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.capacity
}

// Close stops resource sampling.
func (m *ResourceMonitor) Close() {
	if m == nil {
		return
	}
	m.close.Do(func() {
		close(m.stop)
	})
	<-m.done
}

func (m *ResourceMonitor) loop() {
	defer close(m.done)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			m.sample(now)
		case <-m.stop:
			return
		}
	}
}

func (m *ResourceMonitor) sample(now time.Time) {
	total, idle, memory := readRuntimeResources()
	snapshot := m.recorder.Snapshot().Resources
	m.observe(resourcePoint{
		at:        now,
		cpuTotal:  total,
		cpuIdle:   idle,
		diskBytes: snapshot.DiskReadBytes + snapshot.DiskWriteBytes,
	}, memory)
}

func (m *ResourceMonitor) observe(current resourcePoint, memory uint64) {
	m.mu.Lock()
	previous := m.previous
	m.previous = current
	duration := current.at.Sub(previous.at).Seconds()
	if duration <= 0 {
		m.mu.Unlock()
		return
	}
	activeCPU := max(0, current.cpuTotal-previous.cpuTotal-(current.cpuIdle-previous.cpuIdle))
	cpuCores := activeCPU / duration
	diskRate := float64(saturatingSub(current.diskBytes, previous.diskBytes)) / duration
	if m.capacity.DiskAutomatic && diskRate > 0 {
		learned := diskRate * diskLearningHeadroom
		if learned > m.capacity.DiskBytesPerSecond {
			m.capacity.DiskBytesPerSecond = learned
		}
	}
	capacity := m.capacity
	m.mu.Unlock()
	m.recorder.SetResourceSample(cpuCores, diskRate, memory, capacity)
}

func detectCapacity(cfg ResourceConfig) ResourceCapacity {
	capacity := ResourceCapacity{
		CPUCores:           cfg.CPUCores,
		DiskBytesPerSecond: cfg.DiskBytesPerSecond,
		MemoryBytes:        cfg.MemoryBytes,
		CPUAutomatic:       cfg.CPUCores <= 0,
		DiskAutomatic:      cfg.DiskBytesPerSecond <= 0,
		MemoryAutomatic:    cfg.MemoryBytes == 0,
	}
	if capacity.CPUAutomatic {
		capacity.CPUCores = float64(runtime.GOMAXPROCS(0))
	}
	if capacity.MemoryAutomatic {
		capacity.MemoryBytes = automaticMemoryCapacity()
	}
	return capacity
}

func automaticMemoryCapacity() uint64 {
	physical := physicalMemoryBytes()
	limit := debug.SetMemoryLimit(-1)
	if limit > 0 && limit < math.MaxInt64 && (physical == 0 || uint64(limit) < physical) {
		return uint64(limit)
	}
	if physical > 0 {
		return physical
	}
	_, _, used := readRuntimeResources()
	return max(used*4, uint64(512<<20))
}

func readRuntimeResources() (totalCPU, idleCPU float64, memory uint64) {
	samples := []runtimemetrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}
	runtimemetrics.Read(samples)
	totalCPU = samples[0].Value.Float64()
	idleCPU = samples[1].Value.Float64()
	totalMemory := samples[2].Value.Uint64()
	released := samples[3].Value.Uint64()
	return totalCPU, idleCPU, saturatingSub(totalMemory, released)
}

func saturatingSub[T ~uint64](current, previous T) T {
	if current < previous {
		return 0
	}
	return current - previous
}
