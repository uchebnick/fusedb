package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	onedb "github.com/uchebnick/fusedb/pkg/oneleafdb"

	"github.com/cockroachdb/pebble"
)

type BenchmarkConfig struct {
	Engine         string `json:"engine"`
	RecordCount    int    `json:"record_count"`
	OperationCount int    `json:"operation_count"`
	ValueSize      int    `json:"value_size"`
	CacheSizeMB    int    `json:"cache_size_mb"`
	Workers        int    `json:"workers"`
	Workload       string `json:"workload"`
}

type BenchmarkResult struct {
	Engine       string        `json:"engine"`
	Workload     string        `json:"workload"`
	TotalOps     int           `json:"total_ops"`
	Duration     time.Duration `json:"duration"`
	OpsPerSec    float64       `json:"ops_per_sec"`
	AvgLatencyNs int64         `json:"avg_latency_ns"`
	P50LatencyNs int64         `json:"p50_latency_ns"`
	P95LatencyNs int64         `json:"p95_latency_ns"`
	P99LatencyNs int64         `json:"p99_latency_ns"`
	MaxLatencyNs int64         `json:"max_latency_ns"`
	AllocsPerOp  float64       `json:"allocs_per_op"`
	BytesPerOp   float64       `json:"bytes_per_op"`
	MemoryUsedMB float64       `json:"memory_used_mb"`
}

func main() {
	configPath := flag.String("config", "/config/benchmark.json", "Path to benchmark config")
	outputPath := flag.String("output", "/results/results.json", "Path to output results")
	flag.Parse()

	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting benchmark: engine=%s, workload=%s, records=%d, ops=%d",
		config.Engine, config.Workload, config.RecordCount, config.OperationCount)

	var result *BenchmarkResult
	switch config.Engine {
	case "oneleaf":
		result, err = runOneLeafBenchmark(config)
	case "pebble":
		result, err = runPebbleBenchmark(config)
	default:
		log.Fatalf("Unknown engine: %s", config.Engine)
	}

	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	if err := saveResults(*outputPath, result); err != nil {
		log.Fatalf("Failed to save results: %v", err)
	}

	printResults(result)
}

func runOneLeafBenchmark(config *BenchmarkConfig) (*BenchmarkResult, error) {
	dir := "/data/oneleaf"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	opts := onedb.DBOptions{
		Dir:            dir,
		ThresholdBytes: 10 << 20,
		CacheBytes:     int64(config.CacheSizeMB) << 20,
	}

	db, err := onedb.OpenDB(opts)
	if err != nil {
		return nil, fmt.Errorf("open oneleaf: %w", err)
	}
	defer db.Close()

	return runBenchmark(config, &oneLeafDB{db: db})
}

func runPebbleBenchmark(config *BenchmarkConfig) (*BenchmarkResult, error) {
	dir := "/data/pebble"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	cache := pebble.NewCache(int64(config.CacheSizeMB) << 20)
	defer cache.Unref()

	db, err := pebble.Open(dir, &pebble.Options{
		Cache:                       cache,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       12,
		MaxConcurrentCompactions:    func() int { return 3 },
	})
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}
	defer db.Close()

	return runBenchmark(config, &pebbleDB{db: db})
}

func runBenchmark(config *BenchmarkConfig, db benchmarkDB) (*BenchmarkResult, error) {
	// Load phase
	log.Printf("Loading %d records...", config.RecordCount)
	value := make([]byte, config.ValueSize)
	for i := 0; i < len(value); i++ {
		value[i] = byte(i % 256)
	}

	for i := 0; i < config.RecordCount; i++ {
		key := fmt.Sprintf("key:%016d", i)
		if err := db.Put([]byte(key), value); err != nil {
			return nil, fmt.Errorf("load put: %w", err)
		}
		if i%10000 == 0 && i > 0 {
			log.Printf("Loaded %d records", i)
		}
	}

	log.Printf("Load complete. Starting workload: %s", config.Workload)

	// Measure memory before
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Run workload
	latencies := make([]int64, 0, config.OperationCount)
	var totalAllocs, totalBytes uint64

	start := time.Now()

	switch config.Workload {
	case "read-only":
		for i := 0; i < config.OperationCount; i++ {
			key := fmt.Sprintf("key:%016d", i%config.RecordCount)

			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)

			opStart := time.Now()
			_, _, err := db.Get([]byte(key))
			latency := time.Since(opStart).Nanoseconds()

			runtime.ReadMemStats(&m1)

			if err != nil {
				return nil, fmt.Errorf("get: %w", err)
			}

			latencies = append(latencies, latency)
			totalAllocs += m1.Mallocs - m0.Mallocs
			totalBytes += m1.TotalAlloc - m0.TotalAlloc
		}

	case "write-only":
		for i := 0; i < config.OperationCount; i++ {
			key := fmt.Sprintf("key:%016d", config.RecordCount+i)

			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)

			opStart := time.Now()
			err := db.Put([]byte(key), value)
			latency := time.Since(opStart).Nanoseconds()

			runtime.ReadMemStats(&m1)

			if err != nil {
				return nil, fmt.Errorf("put: %w", err)
			}

			latencies = append(latencies, latency)
			totalAllocs += m1.Mallocs - m0.Mallocs
			totalBytes += m1.TotalAlloc - m0.TotalAlloc
		}

	case "mixed-50-50":
		for i := 0; i < config.OperationCount; i++ {
			key := fmt.Sprintf("key:%016d", i%config.RecordCount)

			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)

			var err error
			var latency int64

			if i%2 == 0 {
				opStart := time.Now()
				_, _, err = db.Get([]byte(key))
				latency = time.Since(opStart).Nanoseconds()
			} else {
				opStart := time.Now()
				err = db.Put([]byte(key), value)
				latency = time.Since(opStart).Nanoseconds()
			}

			runtime.ReadMemStats(&m1)

			if err != nil {
				return nil, fmt.Errorf("mixed op: %w", err)
			}

			latencies = append(latencies, latency)
			totalAllocs += m1.Mallocs - m0.Mallocs
			totalBytes += m1.TotalAlloc - m0.TotalAlloc
		}

	default:
		return nil, fmt.Errorf("unknown workload: %s", config.Workload)
	}

	duration := time.Since(start)

	// Measure memory after
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate stats
	result := &BenchmarkResult{
		Engine:       config.Engine,
		Workload:     config.Workload,
		TotalOps:     config.OperationCount,
		Duration:     duration,
		OpsPerSec:    float64(config.OperationCount) / duration.Seconds(),
		AllocsPerOp:  float64(totalAllocs) / float64(config.OperationCount),
		BytesPerOp:   float64(totalBytes) / float64(config.OperationCount),
		MemoryUsedMB: float64(memAfter.Alloc-memBefore.Alloc) / (1024 * 1024),
	}

	// Calculate latency percentiles
	if len(latencies) > 0 {
		var sum int64
		for _, l := range latencies {
			sum += l
		}
		result.AvgLatencyNs = sum / int64(len(latencies))

		// Sort for percentiles
		sortInt64(latencies)
		result.P50LatencyNs = latencies[len(latencies)*50/100]
		result.P95LatencyNs = latencies[len(latencies)*95/100]
		result.P99LatencyNs = latencies[len(latencies)*99/100]
		result.MaxLatencyNs = latencies[len(latencies)-1]
	}

	return result, nil
}

func loadConfig(path string) (*BenchmarkConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config BenchmarkConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

func saveResults(path string, result *BenchmarkResult) error {
	os.MkdirAll("/results", 0755)

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func printResults(r *BenchmarkResult) {
	fmt.Printf("\n=== Benchmark Results ===\n")
	fmt.Printf("Engine:        %s\n", r.Engine)
	fmt.Printf("Workload:      %s\n", r.Workload)
	fmt.Printf("Total Ops:     %d\n", r.TotalOps)
	fmt.Printf("Duration:      %v\n", r.Duration)
	fmt.Printf("Throughput:    %.0f ops/sec\n", r.OpsPerSec)
	fmt.Printf("\nLatency:\n")
	fmt.Printf("  Avg:         %d ns (%.2f µs)\n", r.AvgLatencyNs, float64(r.AvgLatencyNs)/1000)
	fmt.Printf("  P50:         %d ns (%.2f µs)\n", r.P50LatencyNs, float64(r.P50LatencyNs)/1000)
	fmt.Printf("  P95:         %d ns (%.2f µs)\n", r.P95LatencyNs, float64(r.P95LatencyNs)/1000)
	fmt.Printf("  P99:         %d ns (%.2f µs)\n", r.P99LatencyNs, float64(r.P99LatencyNs)/1000)
	fmt.Printf("  Max:         %d ns (%.2f µs)\n", r.MaxLatencyNs, float64(r.MaxLatencyNs)/1000)
	fmt.Printf("\nAllocations:\n")
	fmt.Printf("  Allocs/op:   %.1f\n", r.AllocsPerOp)
	fmt.Printf("  Bytes/op:    %.0f\n", r.BytesPerOp)
	fmt.Printf("  Memory Used: %.2f MB\n", r.MemoryUsedMB)
}

func sortInt64(arr []int64) {
	// Simple insertion sort for small arrays, quicksort for large
	if len(arr) < 50 {
		for i := 1; i < len(arr); i++ {
			key := arr[i]
			j := i - 1
			for j >= 0 && arr[j] > key {
				arr[j+1] = arr[j]
				j--
			}
			arr[j+1] = key
		}
		return
	}

	// Quicksort
	quicksortInt64(arr, 0, len(arr)-1)
}

func quicksortInt64(arr []int64, low, high int) {
	if low < high {
		pi := partitionInt64(arr, low, high)
		quicksortInt64(arr, low, pi-1)
		quicksortInt64(arr, pi+1, high)
	}
}

func partitionInt64(arr []int64, low, high int) int {
	pivot := arr[high]
	i := low - 1
	for j := low; j < high; j++ {
		if arr[j] < pivot {
			i++
			arr[i], arr[j] = arr[j], arr[i]
		}
	}
	arr[i+1], arr[high] = arr[high], arr[i+1]
	return i + 1
}

type benchmarkDB interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
}

type oneLeafDB struct {
	db *onedb.DB
}

func (d *oneLeafDB) Get(key []byte) ([]byte, bool, error) {
	return d.db.Get(key)
}

func (d *oneLeafDB) Put(key, value []byte) error {
	return d.db.Put(key, value)
}

type pebbleDB struct {
	db *pebble.DB
}

func (d *pebbleDB) Get(key []byte) ([]byte, bool, error) {
	value, closer, err := d.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	result := make([]byte, len(value))
	copy(result, value)
	closer.Close()
	return result, true, nil
}

func (d *pebbleDB) Put(key, value []byte) error {
	return d.db.Set(key, value, pebble.NoSync)
}
