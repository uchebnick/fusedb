package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	onedb "github.com/uchebnick/fusedb/internal/oneleafdb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Benchmark metrics (not HTTP metrics)
	benchmarkOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_operations_total",
			Help: "Total number of benchmark operations",
		},
		[]string{"operation", "status"},
	)

	benchmarkLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "benchmark_latency_nanoseconds",
			Help:    "Benchmark operation latency in nanoseconds",
			Buckets: prometheus.ExponentialBuckets(100, 2, 20),
		},
		[]string{"operation"},
	)

	benchmarkAllocations = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "benchmark_allocations_per_op",
			Help:    "Memory allocations per operation",
			Buckets: prometheus.LinearBuckets(0, 1, 20),
		},
		[]string{"operation"},
	)

	benchmarkBytesAllocated = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "benchmark_bytes_allocated_per_op",
			Help:    "Bytes allocated per operation",
			Buckets: prometheus.ExponentialBuckets(100, 2, 20),
		},
		[]string{"operation"},
	)

	benchmarkCurrentRPS = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_current_rps",
			Help: "Current requests per second (linearly increasing)",
		},
	)

	benchmarkActualRPS = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_actual_rps",
			Help: "Actual requests per second achieved",
		},
	)

	benchmarkTargetRPS = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_target_rps",
			Help: "Target requests per second",
		},
	)

	benchmarkMemoryUsed = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_memory_used_bytes",
			Help: "Current memory usage in bytes",
		},
	)

	benchmarkGoroutines = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_goroutines",
			Help: "Number of goroutines",
		},
	)

	benchmarkDiskUsed = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_disk_used_bytes",
			Help: "Disk space used by database in bytes",
		},
	)
)

type BenchmarkRunner struct {
	db             *onedb.DB
	recordCount    int
	valueSize      int
	currentRPS     atomic.Int64
	targetRPS      int64
	rampUpDuration time.Duration
	testDuration   time.Duration
	readRatio      float64
	stopChan       chan struct{}
	wg             sync.WaitGroup
	totalOps       atomic.Int64
	startTime      time.Time
}

func main() {
	cacheSizeMB, _ := strconv.Atoi(getEnv("CACHE_SIZE_MB", "64"))
	thresholdMB, _ := strconv.Atoi(getEnv("THRESHOLD_MB", "10"))
	recordCount, _ := strconv.Atoi(getEnv("RECORD_COUNT", "100000"))
	valueSize, _ := strconv.Atoi(getEnv("VALUE_SIZE", "1024"))
	targetRPS, _ := strconv.ParseInt(getEnv("TARGET_RPS", "10000"), 10, 64)
	rampUpMinutes, _ := strconv.Atoi(getEnv("RAMP_UP_MINUTES", "5"))
	testMinutes, _ := strconv.Atoi(getEnv("TEST_MINUTES", "10"))
	readRatio, _ := strconv.ParseFloat(getEnv("READ_RATIO", "0.8"), 64)

	log.Printf("Starting OneLeaf benchmark runner")
	log.Printf("Config: cache=%dMB, records=%d, valueSize=%d, targetRPS=%d",
		cacheSizeMB, recordCount, valueSize, targetRPS)
	log.Printf("Ramp-up: %d minutes, Test: %d minutes, Read ratio: %.0f%%",
		rampUpMinutes, testMinutes, readRatio*100)

	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            "/data/oneleaf",
		ThresholdBytes: int64(thresholdMB) << 20,
		CacheBytes:     int64(cacheSizeMB) << 20,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	runner := &BenchmarkRunner{
		db:             db,
		recordCount:    recordCount,
		valueSize:      valueSize,
		targetRPS:      targetRPS,
		rampUpDuration: time.Duration(rampUpMinutes) * time.Minute,
		testDuration:   time.Duration(testMinutes) * time.Minute,
		readRatio:      readRatio,
		stopChan:       make(chan struct{}),
		startTime:      time.Now(),
	}

	// Start metrics updater
	go runner.updateMetrics()

	// Start metrics server (Prometheus)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server starting on :9090")
		if err := http.ListenAndServe(":9090", mux); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()

	// Start pprof server on separate port
	go func() {
		log.Println("Pprof server starting on :6060")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatalf("Pprof server failed: %v", err)
		}
	}()

	// Load data
	log.Printf("Loading %d records...", recordCount)
	if err := runner.loadData(); err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}
	log.Println("Data loaded successfully")

	// Run benchmark with linear ramp-up
	log.Println("Starting benchmark with linear ramp-up...")
	runner.runBenchmark()

	log.Println("Benchmark complete")
}

func (r *BenchmarkRunner) loadData() error {
	value := make([]byte, r.valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < r.recordCount; i++ {
		key := fmt.Sprintf("key:%016d", i)
		if err := r.db.Put([]byte(key), value); err != nil {
			return fmt.Errorf("put: %w", err)
		}
		if i%10000 == 0 && i > 0 {
			log.Printf("Loaded %d/%d records", i, r.recordCount)
		}
	}
	return nil
}

func (r *BenchmarkRunner) runBenchmark() {
	startTime := time.Now()
	rampUpEnd := startTime.Add(r.rampUpDuration)
	testEnd := rampUpEnd.Add(r.testDuration)

	// Start worker goroutines
	numWorkers := runtime.GOMAXPROCS(0) * 2
	for i := 0; i < numWorkers; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}

	// Control RPS with linear ramp-up
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if now.After(testEnd) {
				close(r.stopChan)
				r.wg.Wait()
				return
			}

			// Calculate current RPS based on linear ramp-up
			var currentRPS int64
			if now.Before(rampUpEnd) {
				// Linear ramp-up phase
				elapsed := now.Sub(startTime)
				progress := float64(elapsed) / float64(r.rampUpDuration)
				currentRPS = int64(float64(r.targetRPS) * progress)
			} else {
				// Steady state
				currentRPS = r.targetRPS
			}

			r.currentRPS.Store(currentRPS)
			benchmarkCurrentRPS.Set(float64(currentRPS))
			benchmarkTargetRPS.Set(float64(r.targetRPS))

		case <-r.stopChan:
			r.wg.Wait()
			return
		}
	}
}

func (r *BenchmarkRunner) worker(id int) {
	defer r.wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	value := make([]byte, r.valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var opsThisSecond int64
	var lastSecond time.Time = time.Now()

	for {
		select {
		case <-r.stopChan:
			return
		case now := <-ticker.C:
			// Reset counter each second
			opsThisSecond = 0
			lastSecond = now
		default:
			// Check if we should throttle
			currentRPS := r.currentRPS.Load()
			if currentRPS == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// Simple per-worker throttling
			numWorkers := int64(runtime.GOMAXPROCS(0) * 2)
			targetOpsPerWorker := currentRPS / numWorkers

			if opsThisSecond >= targetOpsPerWorker {
				// Wait until next second
				sleepTime := time.Until(lastSecond.Add(time.Second))
				if sleepTime > 0 {
					time.Sleep(sleepTime)
				}
				continue
			}

			// Perform operation
			isRead := rng.Float64() < r.readRatio
			if isRead {
				r.benchmarkGet(rng)
			} else {
				r.benchmarkPut(rng, value)
			}

			opsThisSecond++
		}
	}
}

func (r *BenchmarkRunner) benchmarkGet(rng *rand.Rand) {
	keyNum := rng.Intn(r.recordCount)
	key := fmt.Sprintf("key:%016d", keyNum)

	// Sample allocations on 1% of operations to avoid ReadMemStats overhead
	measureAllocs := rng.Intn(100) == 0
	var m0, m1 runtime.MemStats
	if measureAllocs {
		runtime.ReadMemStats(&m0)
	}

	start := time.Now()
	_, ok, err := r.db.Get([]byte(key))
	latency := time.Since(start).Nanoseconds()

	if measureAllocs {
		runtime.ReadMemStats(&m1)
	}

	if err != nil {
		benchmarkOpsTotal.WithLabelValues("get", "error").Inc()
		return
	}

	if !ok {
		benchmarkOpsTotal.WithLabelValues("get", "miss").Inc()
		return
	}

	benchmarkOpsTotal.WithLabelValues("get", "success").Inc()
	benchmarkLatency.WithLabelValues("get").Observe(float64(latency))
	r.totalOps.Add(1)

	if measureAllocs {
		allocs := m1.Mallocs - m0.Mallocs
		bytes := m1.TotalAlloc - m0.TotalAlloc
		benchmarkAllocations.WithLabelValues("get").Observe(float64(allocs))
		benchmarkBytesAllocated.WithLabelValues("get").Observe(float64(bytes))
	}
}

func (r *BenchmarkRunner) benchmarkPut(rng *rand.Rand, value []byte) {
	keyNum := rng.Intn(r.recordCount)
	key := fmt.Sprintf("key:%016d", keyNum)

	// Sample allocations on 1% of operations to avoid ReadMemStats overhead
	measureAllocs := rng.Intn(100) == 0
	var m0, m1 runtime.MemStats
	if measureAllocs {
		runtime.ReadMemStats(&m0)
	}

	start := time.Now()
	err := r.db.Put([]byte(key), value)
	latency := time.Since(start).Nanoseconds()

	if measureAllocs {
		runtime.ReadMemStats(&m1)
	}

	if err != nil {
		benchmarkOpsTotal.WithLabelValues("put", "error").Inc()
		return
	}

	benchmarkOpsTotal.WithLabelValues("put", "success").Inc()
	benchmarkLatency.WithLabelValues("put").Observe(float64(latency))
	r.totalOps.Add(1)

	if measureAllocs {
		allocs := m1.Mallocs - m0.Mallocs
		bytes := m1.TotalAlloc - m0.TotalAlloc
		benchmarkAllocations.WithLabelValues("put").Observe(float64(allocs))
		benchmarkBytesAllocated.WithLabelValues("put").Observe(float64(bytes))
	}
}

func (r *BenchmarkRunner) updateMetrics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastOps int64
	var lastTime time.Time = time.Now()

	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		benchmarkMemoryUsed.Set(float64(m.Alloc))
		benchmarkGoroutines.Set(float64(runtime.NumGoroutine()))

		// Calculate disk usage
		if dirSize, err := getDirSize("/data/oneleaf"); err == nil {
			benchmarkDiskUsed.Set(float64(dirSize))
		}

		// Calculate actual RPS
		now := time.Now()
		currentOps := r.totalOps.Load()
		elapsed := now.Sub(lastTime).Seconds()
		if elapsed > 0 {
			actualRPS := float64(currentOps-lastOps) / elapsed
			benchmarkActualRPS.Set(actualRPS)
		}
		lastOps = currentOps
		lastTime = now
	}
}

func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
