package qualification

import (
	"context"
	"sync"
	"time"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

// pacer allocates global operation slots across workers. setRate wakes workers
// that reserved slots under the previous phase, so phase transitions do not
// inherit a long quiet-rate backlog.
type pacer struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
	paused   bool
	wake     chan struct{}
}

func newPacer() *pacer {
	return &pacer{paused: true, wake: make(chan struct{})}
}

func (p *pacer) setRate(operationsPerSecond int) {
	p.mu.Lock()
	close(p.wake)
	p.wake = make(chan struct{})
	p.paused = false
	p.interval = time.Second / time.Duration(operationsPerSecond)
	if p.interval <= 0 {
		p.interval = time.Nanosecond
	}
	p.next = time.Now()
	p.mu.Unlock()
}

func (p *pacer) pause() {
	p.mu.Lock()
	close(p.wake)
	p.wake = make(chan struct{})
	p.paused = true
	p.next = time.Time{}
	p.mu.Unlock()
}

func (p *pacer) wait(ctx context.Context) bool {
	for {
		p.mu.Lock()
		wake := p.wake
		if p.paused {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return false
			case <-wake:
				continue
			}
		}
		now := time.Now()
		slot := maxTime(now, p.next)
		p.next = slot.Add(p.interval)
		p.mu.Unlock()

		wait := time.Until(slot)
		if wait <= 0 {
			return true
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-timer.C:
			return true
		}
	}
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

type runMonitor struct {
	mu          sync.Mutex
	phases      []monitoredPhase
	peakOverall DebtReport
}

type monitoredPhase struct {
	Peak             DebtReport
	SchedulerSamples map[string]uint64
}

func newRunMonitor(phaseCount int, baseline DebtReport) *runMonitor {
	monitor := &runMonitor{phases: make([]monitoredPhase, phaseCount), peakOverall: baseline}
	for index := range monitor.phases {
		monitor.phases[index].SchedulerSamples = make(map[string]uint64)
	}
	return monitor
}

func (m *runMonitor) observe(index int, stats fusedb.Stats, resources fusedb.ResourceMetrics) {
	if index < 0 || index >= len(m.phases) {
		return
	}
	current := DebtReport{
		BufferedBytes:      stats.BufferedBytes,
		WALBytes:           stats.WALBytes,
		PendingMergeLeaves: stats.PendingMergeLeaves,
		SchedulerQueued:    stats.SchedulerQueued,
		SchedulerRunning:   stats.SchedulerRunning,
		CPUUtilization:     resources.CPUUtilization,
		DiskUtilization:    resources.DiskUtilization,
		MemoryUtilization:  resources.MemoryUtilization,
	}
	m.mu.Lock()
	updatePeak(&m.phases[index].Peak, current)
	updatePeak(&m.peakOverall, current)
	m.phases[index].SchedulerSamples[stats.Scheduler]++
	m.mu.Unlock()
}

func (m *runMonitor) phase(index int) monitoredPhase {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := m.phases[index]
	snapshot.SchedulerSamples = cloneStringCounters(snapshot.SchedulerSamples)
	return snapshot
}

func (m *runMonitor) overall() DebtReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peakOverall
}

func updatePeak(peak *DebtReport, current DebtReport) {
	peak.BufferedBytes = max(peak.BufferedBytes, current.BufferedBytes)
	peak.WALBytes = max(peak.WALBytes, current.WALBytes)
	peak.PendingMergeLeaves = max(peak.PendingMergeLeaves, current.PendingMergeLeaves)
	peak.SchedulerQueued = max(peak.SchedulerQueued, current.SchedulerQueued)
	peak.SchedulerRunning = peak.SchedulerRunning || current.SchedulerRunning
	peak.CPUUtilization = max(peak.CPUUtilization, current.CPUUtilization)
	peak.DiskUtilization = max(peak.DiskUtilization, current.DiskUtilization)
	peak.MemoryUtilization = max(peak.MemoryUtilization, current.MemoryUtilization)
}

func cloneStringCounters(source map[string]uint64) map[string]uint64 {
	clone := make(map[string]uint64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
