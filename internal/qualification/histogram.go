package qualification

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	qualificationBucketCount = 256
	qualificationFirstBound  = 100.0
	qualificationGrowth      = 1.08
)

var qualificationBounds = buildQualificationBounds()

type latencyHistogram struct {
	buckets  [qualificationBucketCount]atomic.Uint64
	overflow atomic.Uint64
	count    atomic.Uint64
	sum      atomic.Uint64
	maximum  atomic.Int64
}

type histogramSnapshot struct {
	Buckets  [qualificationBucketCount]uint64
	Overflow uint64
	Count    uint64
	Sum      uint64
	Maximum  int64
}

func (h *latencyHistogram) observe(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	nanos := duration.Nanoseconds()
	index := sort.Search(len(qualificationBounds), func(i int) bool {
		return qualificationBounds[i] >= duration
	})
	if index == len(qualificationBounds) {
		h.overflow.Add(1)
	} else {
		h.buckets[index].Add(1)
	}
	h.count.Add(1)
	h.sum.Add(uint64(nanos))
	for previous := h.maximum.Load(); nanos > previous; previous = h.maximum.Load() {
		if h.maximum.CompareAndSwap(previous, nanos) {
			break
		}
	}
}

func (h *latencyHistogram) snapshot() histogramSnapshot {
	var snapshot histogramSnapshot
	for i := range h.buckets {
		snapshot.Buckets[i] = h.buckets[i].Load()
	}
	snapshot.Overflow = h.overflow.Load()
	snapshot.Count = h.count.Load()
	snapshot.Sum = h.sum.Load()
	snapshot.Maximum = h.maximum.Load()
	return snapshot
}

func (h *histogramSnapshot) add(other histogramSnapshot) {
	for i := range h.Buckets {
		h.Buckets[i] += other.Buckets[i]
	}
	h.Overflow += other.Overflow
	h.Count += other.Count
	h.Sum += other.Sum
	h.Maximum = max(h.Maximum, other.Maximum)
}

func (h histogramSnapshot) quantile(value float64) time.Duration {
	if h.Count == 0 {
		return 0
	}
	value = min(1, max(0, value))
	want := uint64(math.Ceil(float64(h.Count) * value))
	if want == 0 {
		want = 1
	}
	var seen uint64
	for i, count := range h.Buckets {
		seen += count
		if seen >= want {
			return qualificationBounds[i]
		}
	}
	return time.Duration(h.Maximum)
}

type operationStats struct {
	latency latencyHistogram
	errors  atomic.Uint64
	mu      sync.Mutex
	first   string
}

func (s *operationStats) record(duration time.Duration, err error) {
	s.latency.observe(duration)
	if err == nil {
		return
	}
	s.errors.Add(1)
	s.mu.Lock()
	if s.first == "" {
		s.first = err.Error()
	}
	s.mu.Unlock()
}

func (s *operationStats) snapshot() operationSnapshot {
	s.mu.Lock()
	first := s.first
	s.mu.Unlock()
	return operationSnapshot{
		Histogram: s.latency.snapshot(),
		Errors:    s.errors.Load(),
		First:     first,
	}
}

type operationSnapshot struct {
	Histogram histogramSnapshot
	Errors    uint64
	First     string
}

func (s *operationSnapshot) add(other operationSnapshot) {
	s.Histogram.add(other.Histogram)
	s.Errors += other.Errors
	if s.First == "" {
		s.First = other.First
	}
}

type phaseOperationStats struct {
	read      operationStats
	write     operationStats
	increment operationStats
}

func (s *phaseOperationStats) snapshot() phaseOperationSnapshot {
	return phaseOperationSnapshot{
		Read:      s.read.snapshot(),
		Write:     s.write.snapshot(),
		Increment: s.increment.snapshot(),
	}
}

type phaseOperationSnapshot struct {
	Read      operationSnapshot
	Write     operationSnapshot
	Increment operationSnapshot
}

func (s *phaseOperationSnapshot) add(other phaseOperationSnapshot) {
	s.Read.add(other.Read)
	s.Write.add(other.Write)
	s.Increment.add(other.Increment)
}

func (s phaseOperationSnapshot) report() OperationReport {
	read := latencyReport(s.Read)
	write := latencyReport(s.Write)
	increment := latencyReport(s.Increment)
	total := read.Count + write.Count + increment.Count
	errors := read.Errors + write.Errors + increment.Errors
	errorRate := 0.0
	if total > 0 {
		errorRate = float64(errors) / float64(total)
	}
	return OperationReport{
		Read:      read,
		Write:     write,
		Increment: increment,
		Total:     total,
		Errors:    errors,
		ErrorRate: errorRate,
	}
}

func latencyReport(snapshot operationSnapshot) LatencyReport {
	average := int64(0)
	if snapshot.Histogram.Count > 0 {
		average = int64(snapshot.Histogram.Sum / snapshot.Histogram.Count)
	}
	return LatencyReport{
		Count:        snapshot.Histogram.Count,
		Errors:       snapshot.Errors,
		SumNanos:     snapshot.Histogram.Sum,
		AverageNanos: average,
		P50Nanos:     snapshot.Histogram.quantile(0.50).Nanoseconds(),
		P95Nanos:     snapshot.Histogram.quantile(0.95).Nanoseconds(),
		P99Nanos:     snapshot.Histogram.quantile(0.99).Nanoseconds(),
		MaxNanos:     snapshot.Histogram.Maximum,
		FirstError:   snapshot.First,
	}
}

func buildQualificationBounds() []time.Duration {
	bounds := make([]time.Duration, qualificationBucketCount)
	value := qualificationFirstBound
	for i := range bounds {
		bounds[i] = time.Duration(value)
		value *= qualificationGrowth
	}
	return bounds
}
