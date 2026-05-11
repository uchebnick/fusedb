package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type LoadGenerator struct {
	oneleafURL string
	pebbleURL  string
	duration   time.Duration
	rps        int
	workers    int

	oneleafStats *Stats
	pebbleStats  *Stats
}

type Stats struct {
	requests    uint64
	errors      uint64
	totalLatency uint64
	minLatency  uint64
	maxLatency  uint64
	mu          sync.Mutex
	latencies   []uint64
}

type Request struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Response struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
	Latency int64  `json:"latency_ns"`
}

func main() {
	lg := &LoadGenerator{
		oneleafURL:  getEnv("ONELEAF_URL", "http://oneleaf:8080"),
		pebbleURL:   getEnv("PEBBLE_URL", "http://pebble:8080"),
		duration:    parseDuration(getEnv("DURATION", "5m")),
		rps:         parseInt(getEnv("RPS", "1000")),
		workers:     parseInt(getEnv("WORKERS", "10")),
		oneleafStats: &Stats{minLatency: ^uint64(0)},
		pebbleStats:  &Stats{minLatency: ^uint64(0)},
	}

	log.Printf("Starting load generator:")
	log.Printf("  OneLeaf URL: %s", lg.oneleafURL)
	log.Printf("  Pebble URL: %s", lg.pebbleURL)
	log.Printf("  Duration: %v", lg.duration)
	log.Printf("  Target RPS: %d", lg.rps)
	log.Printf("  Workers: %d", lg.workers)

	// Wait for services to be ready
	lg.waitForServices()

	// Pre-populate databases
	log.Println("Pre-populating databases with 100K keys...")
	lg.prepopulate(100000)

	// Run load test
	log.Println("Starting load test...")
	lg.run()

	// Print results
	lg.printResults()
}

func (lg *LoadGenerator) waitForServices() {
	log.Println("Waiting for services to be ready...")

	for {
		if lg.checkHealth(lg.oneleafURL) && lg.checkHealth(lg.pebbleURL) {
			log.Println("All services ready!")
			break
		}
		time.Sleep(2 * time.Second)
	}
}

func (lg *LoadGenerator) checkHealth(baseURL string) bool {
	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (lg *LoadGenerator) prepopulate(numKeys int) {
	var wg sync.WaitGroup
	keysPerWorker := numKeys / lg.workers

	for w := 0; w < lg.workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			start := workerID * keysPerWorker
			end := start + keysPerWorker

			for i := start; i < end; i++ {
				key := fmt.Sprintf("key:%010d", i)
				value := fmt.Sprintf("value_%d_%s", i, randomString(100))

				lg.put(lg.oneleafURL, key, value)
				lg.put(lg.pebbleURL, key, value)

				if i%10000 == 0 {
					log.Printf("Prepopulated %d keys...", i)
				}
			}
		}(w)
	}

	wg.Wait()
	log.Println("Prepopulation complete!")
}

func (lg *LoadGenerator) run() {
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Calculate requests per worker
	requestsPerSecond := lg.rps / lg.workers
	interval := time.Second / time.Duration(requestsPerSecond)

	// Start workers for OneLeaf
	for w := 0; w < lg.workers; w++ {
		wg.Add(1)
		go lg.worker(&wg, stopChan, lg.oneleafURL, lg.oneleafStats, interval, "OneLeaf")
	}

	// Start workers for Pebble
	for w := 0; w < lg.workers; w++ {
		wg.Add(1)
		go lg.worker(&wg, stopChan, lg.pebbleURL, lg.pebbleStats, interval, "Pebble")
	}

	// Start stats reporter
	go lg.reportStats(stopChan)

	// Run for duration
	time.Sleep(lg.duration)
	close(stopChan)

	wg.Wait()
}

func (lg *LoadGenerator) worker(wg *sync.WaitGroup, stop chan struct{}, baseURL string, stats *Stats, interval time.Duration, name string) {
	defer wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// 80% reads, 20% writes
			if rand.Float64() < 0.8 {
				lg.doGet(baseURL, stats)
			} else {
				lg.doPut(baseURL, stats)
			}
		}
	}
}

func (lg *LoadGenerator) doGet(baseURL string, stats *Stats) {
	key := fmt.Sprintf("key:%010d", rand.Intn(100000))

	start := time.Now()
	_, err := lg.get(baseURL, key)
	latency := uint64(time.Since(start).Nanoseconds())

	lg.recordStats(stats, latency, err)
}

func (lg *LoadGenerator) doPut(baseURL string, stats *Stats) {
	key := fmt.Sprintf("key:%010d", rand.Intn(100000))
	value := fmt.Sprintf("value_%d_%s", rand.Int(), randomString(100))

	start := time.Now()
	err := lg.put(baseURL, key, value)
	latency := uint64(time.Since(start).Nanoseconds())

	lg.recordStats(stats, latency, err)
}

func (lg *LoadGenerator) get(baseURL, key string) (string, error) {
	req := Request{Key: key}
	body, _ := json.Marshal(req)

	resp, err := http.Post(baseURL+"/get", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if !result.Success {
		return "", fmt.Errorf("get failed: %s", result.Error)
	}

	return result.Value, nil
}

func (lg *LoadGenerator) put(baseURL, key, value string) error {
	req := Request{Key: key, Value: value}
	body, _ := json.Marshal(req)

	resp, err := http.Post(baseURL+"/put", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	if !result.Success {
		return fmt.Errorf("put failed: %s", result.Error)
	}

	return nil
}

func (lg *LoadGenerator) recordStats(stats *Stats, latency uint64, err error) {
	atomic.AddUint64(&stats.requests, 1)
	atomic.AddUint64(&stats.totalLatency, latency)

	if err != nil {
		atomic.AddUint64(&stats.errors, 1)
	}

	stats.mu.Lock()
	if latency < stats.minLatency {
		stats.minLatency = latency
	}
	if latency > stats.maxLatency {
		stats.maxLatency = latency
	}
	stats.latencies = append(stats.latencies, latency)
	stats.mu.Unlock()
}

func (lg *LoadGenerator) reportStats(stop chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			lg.printCurrentStats()
		}
	}
}

func (lg *LoadGenerator) printCurrentStats() {
	oneleafReqs := atomic.LoadUint64(&lg.oneleafStats.requests)
	oneleafErrs := atomic.LoadUint64(&lg.oneleafStats.errors)
	oneleafLatency := atomic.LoadUint64(&lg.oneleafStats.totalLatency)

	pebbleReqs := atomic.LoadUint64(&lg.pebbleStats.requests)
	pebbleErrs := atomic.LoadUint64(&lg.pebbleStats.errors)
	pebbleLatency := atomic.LoadUint64(&lg.pebbleStats.totalLatency)

	log.Printf("=== Current Stats ===")
	log.Printf("OneLeaf: %d reqs, %d errors, avg latency: %d ns",
		oneleafReqs, oneleafErrs, oneleafLatency/max(oneleafReqs, 1))
	log.Printf("Pebble:  %d reqs, %d errors, avg latency: %d ns",
		pebbleReqs, pebbleErrs, pebbleLatency/max(pebbleReqs, 1))
}

func (lg *LoadGenerator) printResults() {
	log.Println("\n=== Final Results ===")

	log.Println("\nOneLeaf:")
	lg.printStatsDetail(lg.oneleafStats)

	log.Println("\nPebble:")
	lg.printStatsDetail(lg.pebbleStats)
}

func (lg *LoadGenerator) printStatsDetail(stats *Stats) {
	requests := atomic.LoadUint64(&stats.requests)
	errors := atomic.LoadUint64(&stats.errors)
	totalLatency := atomic.LoadUint64(&stats.totalLatency)

	avgLatency := totalLatency / max(requests, 1)

	log.Printf("  Total requests: %d", requests)
	log.Printf("  Errors: %d (%.2f%%)", errors, float64(errors)/float64(max(requests, 1))*100)
	log.Printf("  Avg latency: %d ns (%.2f µs)", avgLatency, float64(avgLatency)/1000)
	log.Printf("  Min latency: %d ns", stats.minLatency)
	log.Printf("  Max latency: %d ns", stats.maxLatency)

	// Calculate percentiles
	stats.mu.Lock()
	latencies := make([]uint64, len(stats.latencies))
	copy(latencies, stats.latencies)
	stats.mu.Unlock()

	if len(latencies) > 0 {
		p50 := percentile(latencies, 0.50)
		p95 := percentile(latencies, 0.95)
		p99 := percentile(latencies, 0.99)

		log.Printf("  p50 latency: %d ns", p50)
		log.Printf("  p95 latency: %d ns", p95)
		log.Printf("  p99 latency: %d ns", p99)
	}
}

func percentile(data []uint64, p float64) uint64 {
	if len(data) == 0 {
		return 0
	}

	// Simple percentile calculation (not sorting for performance)
	index := int(float64(len(data)) * p)
	if index >= len(data) {
		index = len(data) - 1
	}
	return data[index]
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
