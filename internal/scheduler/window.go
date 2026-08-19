package scheduler

import (
	"math"
	"time"

	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
)

type intervalSample struct {
	duration  time.Duration
	readOps   uint64
	writeOps  uint64
	read      enginemetrics.HistogramSnapshot
	write     enginemetrics.HistogramSnapshot
	resources enginemetrics.ResourceSnapshot
}

type rollingWindow struct {
	maxAge  time.Duration
	samples []intervalSample
}

func newRollingWindow(maxAge time.Duration) *rollingWindow {
	return &rollingWindow{maxAge: maxAge}
}

func (w *rollingWindow) add(previous, current enginemetrics.Snapshot) {
	duration := current.At.Sub(previous.At)
	if duration <= 0 {
		return
	}
	sample := intervalSample{
		duration:  duration,
		read:      current.ReadLatency.Sub(previous.ReadLatency),
		write:     current.WriteLatency.Sub(previous.WriteLatency),
		resources: current.Resources,
	}
	if current.ReadOps >= previous.ReadOps {
		sample.readOps = current.ReadOps - previous.ReadOps
	}
	if current.WriteOps >= previous.WriteOps {
		sample.writeOps = current.WriteOps - previous.WriteOps
	}
	w.samples = append(w.samples, sample)

	var age time.Duration
	keep := len(w.samples)
	for i := len(w.samples) - 1; i >= 0; i-- {
		age += w.samples[i].duration
		if age > w.maxAge {
			keep = i + 1
			break
		}
	}
	if keep > 0 && keep < len(w.samples) {
		w.samples = append(w.samples[:0], w.samples[keep:]...)
	}
}

func (w *rollingWindow) load() Load {
	var out Load
	var read, write enginemetrics.HistogramSnapshot
	var rateSum, rateSquareSum float64
	for _, sample := range w.samples {
		out.Window += sample.duration
		seconds := sample.duration.Seconds()
		if seconds > 0 {
			rate := float64(sample.readOps+sample.writeOps) / seconds
			rateSum += rate
			rateSquareSum += rate * rate
		}
		out.ReadRate += float64(sample.readOps)
		out.WriteRate += float64(sample.writeOps)
		read.Add(sample.read)
		write.Add(sample.write)
		out.CPUUtilization = max(out.CPUUtilization, sample.resources.CPUUtilization)
		out.DiskUtilization = max(out.DiskUtilization, sample.resources.DiskUtilization)
		out.MemoryUtilization = max(out.MemoryUtilization, sample.resources.MemoryUtilization)
	}
	if seconds := out.Window.Seconds(); seconds > 0 {
		out.ReadRate /= seconds
		out.WriteRate /= seconds
	}
	out.RequestRate = out.ReadRate + out.WriteRate
	if n := float64(len(w.samples)); n > 1 {
		mean := rateSum / n
		variance := max(0, rateSquareSum/n-mean*mean)
		if mean > 0 {
			out.RateCV = math.Sqrt(variance) / mean
		}
	}
	out.ReadP95 = read.Quantile(0.95)
	out.ReadP99 = read.Quantile(0.99)
	out.WriteP95 = write.Quantile(0.95)
	out.WriteP99 = write.Quantile(0.99)
	out.ReadLatencySamples = read.Count
	out.WriteLatencySamples = write.Count
	return out
}
