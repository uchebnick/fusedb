// Package metrics provides dependency-free engine telemetry that can be
// consumed by the background scheduler and exported by adapters such as
// Prometheus collectors.
package metrics

import (
	"math"
	"sort"
	"sync/atomic"
	"time"
)

// LatencyBucketCount is the number of cumulative latency buckets exposed in a
// HistogramSnapshot. Bounds are stable for the lifetime of the process.
const LatencyBucketCount = 64

var latencyBounds = buildLatencyBounds()

// HistogramSnapshot is a cumulative, mergeable latency histogram.
// Buckets contain non-cumulative counts for the corresponding upper bounds.
type HistogramSnapshot struct {
	Buckets [LatencyBucketCount]uint64
	// Overflow counts observations above the largest finite bound. Prometheus
	// exporters retain them in _count/+Inf rather than misclassifying them into
	// the last finite bucket.
	Overflow uint64
	Count    uint64
	// SumNanos is the exact sum of sampled observations. It lets exporters emit
	// a valid histogram _sum instead of estimating it from bucket boundaries.
	SumNanos uint64
}

type latencyHistogram struct {
	buckets  [LatencyBucketCount]atomic.Uint64
	overflow atomic.Uint64
	sumNanos atomic.Uint64
}

func (h *latencyHistogram) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	i := sort.Search(len(latencyBounds), func(i int) bool {
		return latencyBounds[i] >= d
	})
	if i == len(latencyBounds) {
		h.overflow.Add(1)
	} else {
		h.buckets[i].Add(1)
	}
	h.sumNanos.Add(uint64(d))
}

func (h *latencyHistogram) snapshot() HistogramSnapshot {
	var out HistogramSnapshot
	for i := range h.buckets {
		out.Buckets[i] = h.buckets[i].Load()
		out.Count += out.Buckets[i]
	}
	out.Overflow = h.overflow.Load()
	out.Count += out.Overflow
	out.SumNanos = h.sumNanos.Load()
	return out
}

// Sub returns the non-negative difference between two cumulative snapshots.
func (h HistogramSnapshot) Sub(previous HistogramSnapshot) HistogramSnapshot {
	var out HistogramSnapshot
	for i := range h.Buckets {
		if h.Buckets[i] >= previous.Buckets[i] {
			out.Buckets[i] = h.Buckets[i] - previous.Buckets[i]
		}
	}
	if h.Count >= previous.Count {
		out.Count = h.Count - previous.Count
	}
	if h.Overflow >= previous.Overflow {
		out.Overflow = h.Overflow - previous.Overflow
	}
	if h.SumNanos >= previous.SumNanos {
		out.SumNanos = h.SumNanos - previous.SumNanos
	}
	return out
}

// Add merges another non-cumulative histogram into h.
func (h *HistogramSnapshot) Add(other HistogramSnapshot) {
	for i := range h.Buckets {
		h.Buckets[i] += other.Buckets[i]
	}
	h.Count += other.Count
	h.Overflow += other.Overflow
	h.SumNanos += other.SumNanos
}

// Quantile returns the upper bound of the bucket containing q. It returns zero
// for an empty histogram. q is clamped to [0, 1].
func (h HistogramSnapshot) Quantile(q float64) time.Duration {
	if h.Count == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	want := uint64(math.Ceil(float64(h.Count) * q))
	if want == 0 {
		want = 1
	}
	var seen uint64
	for i, count := range h.Buckets {
		seen += count
		if seen >= want {
			return latencyBounds[i]
		}
	}
	return latencyBounds[len(latencyBounds)-1]
}

// LatencyBounds returns a detached copy of the histogram upper bounds. A
// Prometheus adapter can use these bounds without depending on scheduler code.
func LatencyBounds() []time.Duration {
	return append([]time.Duration(nil), latencyBounds...)
}

func buildLatencyBounds() []time.Duration {
	// 100 ns with a 1.35 growth factor covers sub-microsecond operations through
	// multi-second stalls while keeping p95/p99 resolution useful around 1 ms.
	bounds := make([]time.Duration, LatencyBucketCount)
	value := 100.0
	for i := range bounds {
		bounds[i] = time.Duration(value)
		value *= 1.35
	}
	return bounds
}
