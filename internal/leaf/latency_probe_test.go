package leaf

import (
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/segment"
)

type latencySample struct {
	at  time.Time
	dur time.Duration
}

type latencyStats struct {
	count int
	avg   time.Duration
	p50   time.Duration
	p95   time.Duration
	p99   time.Duration
	max   time.Duration
}

func TestLeafReadWriteLatencyDuringMergeOSFS(t *testing.T) {
	if os.Getenv("FUSEDB_LATENCY_PROBE") != "1" {
		t.Skip("set FUSEDB_LATENCY_PROBE=1 to run the OSFS latency probe")
	}

	const (
		baseKeys      = 64 * 1024
		updateKeys    = 1024
		writeInterval = 50 * time.Microsecond
		readInterval  = 50 * time.Microsecond
		warmup        = 300 * time.Millisecond
		cooldown      = 300 * time.Millisecond
	)

	dir := t.TempDir()
	reader := benchSegmentReader(t, disk.DefaultFS, dir, 1, 1, baseKeys)
	defer reader.Close()

	leaf := NewLeaf(1, 42, reader, &Merger{})
	writeSamples := make(chan latencySample, 64*1024)
	readSamples := make(chan latencySample, 64*1024)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var readErrors atomic.Int64
	var readMisses atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		nextWrite := time.Now()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}

			key := benchKey(i % updateKeys)
			start := time.Now()
			leaf.Put(key, []byte("updated-value"))
			writeSamples <- latencySample{at: start, dur: time.Since(start)}

			nextWrite = nextWrite.Add(writeInterval)
			if sleep := time.Until(nextWrite); sleep > 0 {
				time.Sleep(sleep)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nextRead := time.Now()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}

			key := benchKey((baseKeys / 2) + (i % (baseKeys / 2)))
			start := time.Now()
			_, ok, err := leaf.Get(key)
			readSamples <- latencySample{at: start, dur: time.Since(start)}
			if err != nil {
				readErrors.Add(1)
			}
			if !ok {
				readMisses.Add(1)
			}

			nextRead = nextRead.Add(readInterval)
			if sleep := time.Until(nextRead); sleep > 0 {
				time.Sleep(sleep)
			}
		}
	}()

	time.Sleep(warmup)
	mergeStart := time.Now()
	err := leaf.Merge(segment.Options{
		FS:                 disk.DefaultFS,
		Dir:                dir,
		SegmentID:          1,
		Version:            2,
		ExpectedKeys:       baseKeys,
		TargetBlockSize:    segment.DefaultTargetBlockSize,
		BloomFalsePositive: 0.01,
		Compression:        segment.CompressionNone,
	})
	mergeEnd := time.Now()
	time.Sleep(cooldown)

	close(stop)
	wg.Wait()
	close(writeSamples)
	close(readSamples)

	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if reader := leaf.reader.Load(); reader != nil {
		_ = leaf.Close()
		_ = reader.Segment().Remove()
	}

	writeBefore, writeDuring, writeAfter := splitLatencySamples(writeSamples, mergeStart, mergeEnd)
	readBefore, readDuring, readAfter := splitLatencySamples(readSamples, mergeStart, mergeEnd)
	t.Logf("merge_duration=%s", mergeEnd.Sub(mergeStart))
	t.Logf("put_latency_before: %s", formatLatencyStats(writeBefore))
	t.Logf("put_latency_during: %s", formatLatencyStats(writeDuring))
	t.Logf("put_latency_after:  %s", formatLatencyStats(writeAfter))
	t.Logf("get_latency_before: %s", formatLatencyStats(readBefore))
	t.Logf("get_latency_during: %s", formatLatencyStats(readDuring))
	t.Logf("get_latency_after:  %s", formatLatencyStats(readAfter))
	t.Logf("get_errors=%d get_misses=%d", readErrors.Load(), readMisses.Load())
}

func splitLatencySamples(samples <-chan latencySample, start, end time.Time) ([]time.Duration, []time.Duration, []time.Duration) {
	var before []time.Duration
	var during []time.Duration
	var after []time.Duration
	for sample := range samples {
		switch {
		case sample.at.Before(start):
			before = append(before, sample.dur)
		case sample.at.Before(end):
			during = append(during, sample.dur)
		default:
			after = append(after, sample.dur)
		}
	}
	return before, during, after
}

func formatLatencyStats(samples []time.Duration) latencyStats {
	if len(samples) == 0 {
		return latencyStats{}
	}

	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})

	var total time.Duration
	for _, sample := range samples {
		total += sample
	}

	return latencyStats{
		count: len(samples),
		avg:   total / time.Duration(len(samples)),
		p50:   percentile(samples, 0.50),
		p95:   percentile(samples, 0.95),
		p99:   percentile(samples, 0.99),
		max:   samples[len(samples)-1],
	}
}

func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(samples))*p)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(samples) {
		index = len(samples) - 1
	}
	return samples[index]
}

func (s latencyStats) String() string {
	return fmt.Sprintf(
		"count=%d avg=%s p50=%s p95=%s p99=%s max=%s",
		s.count,
		s.avg,
		s.p50,
		s.p95,
		s.p99,
		s.max,
	)
}
