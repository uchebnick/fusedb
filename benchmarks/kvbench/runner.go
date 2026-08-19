package kvbench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const maxTrackedLatency = int64(time.Minute)

// Run executes every requested matrix cell in an isolated temporary directory.
func Run(ctx context.Context, config Config, progress io.Writer) (Report, error) {
	if err := config.normalize(); err != nil {
		return Report{}, err
	}
	selected, skipped, err := selectFactories(config.Engines)
	if err != nil {
		return Report{}, err
	}
	if err := os.MkdirAll(config.RootDir, 0o755); err != nil {
		return Report{}, fmt.Errorf("create benchmark root: %w", err)
	}

	started := time.Now().UTC()
	report := Report{
		Version:   ReportVersion,
		StartedAt: started,
		Runtime:   runtimeInfo(),
		Config:    snapshotConfig(config),
		Skipped:   skipped,
		Results:   make([]Result, 0, matrixSize(config, len(selected))),
	}
	for _, factory := range selected {
		report.Engines = append(report.Engines, factory.Info())
	}

	keys := makeKeys(config.Keys)
	values := makeValues(config.ValueBytes, config.Seed)
	for repetition := 1; repetition <= config.Repetitions; repetition++ {
		for _, durability := range config.Durabilities {
			for _, workers := range config.WorkerCounts {
				for workloadIndex, workloadName := range config.Workloads {
					workload := workloads[workloadName]
					for engineIndex := range selected {
						factory := selected[(engineIndex+workloadIndex+repetition-1)%len(selected)]
						if err := ctx.Err(); err != nil {
							return report, err
						}
						fmt.Fprintf(progress, "run engine=%s durability=%s workload=%s workers=%d repetition=%d\n",
							factory.Info().Name, durability, workload.Name, workers, repetition)
						result, runErr := runOne(ctx, factory, config, workload, durability, workers, repetition, keys, values)
						report.Results = append(report.Results, result)
						if runErr != nil {
							report.FinishedAt = time.Now().UTC()
							return report, fmt.Errorf("%s/%s/%s/%d workers: %w",
								factory.Info().Name, durability, workload.Name, workers, runErr)
						}
					}
				}
			}
		}
	}
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func matrixSize(config Config, engines int) int {
	return engines * len(config.Workloads) * len(config.Durabilities) * len(config.WorkerCounts) * config.Repetitions
}

func runOne(
	ctx context.Context,
	factory engineFactory,
	config Config,
	workload Workload,
	durability Durability,
	workers int,
	repetition int,
	keys [][]byte,
	values [][]byte,
) (result Result, returnErr error) {
	info := factory.Info()
	result = Result{
		Engine:        info.Name,
		EngineVersion: info.Version,
		Workload:      workload,
		Durability:    durability,
		Workers:       workers,
		Repetition:    repetition,
	}
	runDir, err := os.MkdirTemp(config.RootDir, fmt.Sprintf("%s-%s-%s-", info.Name, durability, workload.Name))
	if err != nil {
		return result, fmt.Errorf("create run directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(runDir); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("remove run directory: %w", err)
		}
	}()

	asyncOptions := engineOptions{Durability: DurabilityAsync, CacheBytes: config.CacheBytes, MemtableBytes: config.MemtableBytes}
	seedStart := time.Now()
	seedStore, err := factory.Open(runDir, asyncOptions)
	if err != nil {
		return result, fmt.Errorf("open for seed: %w", err)
	}
	for index, key := range keys {
		if err := seedStore.Put(key, values[index%len(values)]); err != nil {
			_ = seedStore.Close()
			return result, fmt.Errorf("seed key %d: %w", index, err)
		}
	}
	if err := seedStore.Flush(); err != nil {
		_ = seedStore.Close()
		return result, fmt.Errorf("flush seed: %w", err)
	}
	if err := seedStore.Close(); err != nil {
		return result, fmt.Errorf("close seed: %w", err)
	}
	result.SeedElapsed = time.Since(seedStart)

	openStart := time.Now()
	benchmarkStore, err := factory.Open(runDir, engineOptions{
		Durability: durability, CacheBytes: config.CacheBytes, MemtableBytes: config.MemtableBytes,
	})
	if err != nil {
		return result, fmt.Errorf("open benchmark database: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = benchmarkStore.Close()
		}
	}()
	result.OpenElapsed = time.Since(openStart)

	if config.Warmup > 0 {
		if _, err := runPhase(ctx, benchmarkStore, workload, workers, config.Warmup, config.Seed+uint64(repetition), keys, values); err != nil {
			return result, fmt.Errorf("warmup: %w", err)
		}
	}
	runtime.GC()
	phase, err := runPhase(ctx, benchmarkStore, workload, workers, config.Duration, config.Seed+uint64(repetition), keys, values)
	result.Operations = phase.operations
	result.Reads = phase.reads
	result.Writes = phase.writes
	result.Errors = phase.errors
	result.Elapsed = phase.elapsed
	if phase.elapsed > 0 {
		result.OperationsPerSec = float64(phase.operations) / phase.elapsed.Seconds()
	}
	result.Latency = summarizeHistogram(phase.histogram)
	if err != nil {
		return result, err
	}

	verified, err := verifyKeys(benchmarkStore, keys, config.ValueBytes)
	result.VerifiedKeys = verified
	if err != nil {
		return result, err
	}
	drainStart := time.Now()
	if err := benchmarkStore.Flush(); err != nil {
		return result, fmt.Errorf("drain foreground debt: %w", err)
	}
	result.DrainElapsed = time.Since(drainStart)
	closeStart := time.Now()
	if err := benchmarkStore.Close(); err != nil {
		return result, fmt.Errorf("close: %w", err)
	}
	closed = true
	result.CloseElapsed = time.Since(closeStart)
	result.DirectoryBytes, err = directoryBytes(runDir)
	if err != nil {
		return result, err
	}
	return result, nil
}

type phaseResult struct {
	operations uint64
	reads      uint64
	writes     uint64
	errors     uint64
	elapsed    time.Duration
	histogram  *hdrhistogram.Histogram
}

type workerResult struct {
	operations uint64
	reads      uint64
	writes     uint64
	errors     uint64
	histogram  *hdrhistogram.Histogram
	err        error
}

func runPhase(
	ctx context.Context,
	db store,
	workload Workload,
	workers int,
	duration time.Duration,
	seed uint64,
	keys [][]byte,
	values [][]byte,
) (phaseResult, error) {
	results := make([]workerResult, workers)
	for index := range results {
		results[index].histogram = hdrhistogram.New(1, maxTrackedLatency, 3)
	}
	startSignal := make(chan struct{})
	ready := sync.WaitGroup{}
	ready.Add(workers)
	workersDone := sync.WaitGroup{}
	workersDone.Add(workers)
	var stop atomic.Bool
	for workerID := 0; workerID < workers; workerID++ {
		go func() {
			defer workersDone.Done()
			result := &results[workerID]
			rng := xorshift64{state: seed ^ (uint64(workerID+1) * 0x9e3779b97f4a7c15)}
			ready.Done()
			<-startSignal
			deadline := time.Now().Add(duration)
			for !stop.Load() {
				if err := ctx.Err(); err != nil {
					result.err = err
					stop.Store(true)
					return
				}
				operationStart := time.Now()
				if !operationStart.Before(deadline) {
					return
				}
				key := keys[int(rng.next()%uint64(len(keys)))]
				read := int(rng.next()%100) < workload.ReadPercent
				var err error
				if read {
					var value []byte
					var found bool
					value, found, err = db.Get(key)
					if err == nil && (!found || len(value) == 0) {
						err = fmt.Errorf("read returned found=%v length=%d", found, len(value))
					}
					result.reads++
				} else {
					value := values[int(rng.next()%uint64(len(values)))]
					err = db.Put(key, value)
					result.writes++
				}
				elapsed := time.Since(operationStart).Nanoseconds()
				if elapsed > maxTrackedLatency {
					elapsed = maxTrackedLatency
				}
				_ = result.histogram.RecordValue(elapsed)
				result.operations++
				if err != nil {
					result.errors++
					result.err = err
					stop.Store(true)
					return
				}
			}
		}()
	}
	ready.Wait()
	started := time.Now()
	close(startSignal)
	workersDone.Wait()
	elapsed := time.Since(started)

	merged := hdrhistogram.New(1, maxTrackedLatency, 3)
	var phase phaseResult
	phase.elapsed = elapsed
	phase.histogram = merged
	var firstErr error
	for index := range results {
		result := &results[index]
		phase.operations += result.operations
		phase.reads += result.reads
		phase.writes += result.writes
		phase.errors += result.errors
		merged.Merge(result.histogram)
		if firstErr == nil && result.err != nil {
			firstErr = result.err
		}
	}
	return phase, firstErr
}

func summarizeHistogram(histogram *hdrhistogram.Histogram) Latency {
	return Latency{
		Samples: uint64(histogram.TotalCount()),
		Mean:    histogram.Mean(),
		P50:     histogram.ValueAtPercentile(50),
		P95:     histogram.ValueAtPercentile(95),
		P99:     histogram.ValueAtPercentile(99),
		P999:    histogram.ValueAtPercentile(99.9),
		Max:     histogram.Max(),
	}
}

func verifyKeys(db store, keys [][]byte, valueBytes int) (int, error) {
	count := len(keys)
	if count > 1024 {
		count = 1024
	}
	for index := 0; index < count; index++ {
		keyIndex := index * len(keys) / count
		value, found, err := db.Get(keys[keyIndex])
		if err != nil {
			return index, fmt.Errorf("verify key %d: %w", keyIndex, err)
		}
		if !found || len(value) != valueBytes {
			return index, fmt.Errorf("verify key %d: found=%v length=%d, want %d", keyIndex, found, len(value), valueBytes)
		}
	}
	return count, nil
}

func makeKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for index := range keys {
		keys[index] = []byte(fmt.Sprintf("key:%020d", index))
	}
	return keys
}

func makeValues(size int, seed uint64) [][]byte {
	const variants = 1024
	values := make([][]byte, variants)
	rng := xorshift64{state: seed ^ 0xd1b54a32d192ed03}
	for index := range values {
		value := make([]byte, size)
		for offset := range value {
			value[offset] = byte(rng.next())
		}
		values[index] = value
	}
	return values
}

type xorshift64 struct {
	state uint64
}

func (r *xorshift64) next() uint64 {
	if r.state == 0 {
		r.state = 0x9e3779b97f4a7c15
	}
	x := r.state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.state = x
	return x
}

func directoryBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("measure directory: %w", err)
	}
	return total, nil
}

func closeWith(primary error, closer store) error {
	return errors.Join(primary, closer.Close())
}
