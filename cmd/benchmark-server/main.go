package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/uchebnick/fusedb/internal/oneleafdb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_nanoseconds",
			Help:    "HTTP request latency in nanoseconds",
			Buckets: prometheus.ExponentialBuckets(100, 2, 20), // 100ns to ~100ms
		},
		[]string{"method", "endpoint"},
	)

	dbOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_operations_total",
			Help: "Total number of database operations",
		},
		[]string{"operation", "status"},
	)

	dbOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_operation_duration_nanoseconds",
			Help:    "Database operation latency in nanoseconds",
			Buckets: prometheus.ExponentialBuckets(100, 2, 20),
		},
		[]string{"operation"},
	)

	activeConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_connections",
			Help: "Number of active connections",
		},
	)
)

type Server struct {
	db          *oneleafdb.DB
	activeConns int64
}

type GetRequest struct {
	Key string `json:"key"`
}

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Response struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
	Latency int64  `json:"latency_ns"`
}

func main() {
	cacheSizeMB, _ := strconv.Atoi(getEnv("CACHE_SIZE_MB", "64"))
	thresholdMB, _ := strconv.Atoi(getEnv("THRESHOLD_MB", "10"))

	db, err := oneleafdb.OpenDB(oneleafdb.DBOptions{
		Dir:            "/data/oneleaf",
		ThresholdBytes: int64(thresholdMB) << 20,
		CacheBytes:     int64(cacheSizeMB) << 20,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	server := &Server{db: db}

	// HTTP endpoints
	http.HandleFunc("/get", server.handleGet)
	http.HandleFunc("/put", server.handlePut)
	http.HandleFunc("/health", server.handleHealth)

	// Start metrics updater
	go server.updateMetrics()

	// Start metrics server on separate port
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Println("Metrics server starting on :9090")
		if err := http.ListenAndServe(":9090", metricsMux); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()

	log.Println("OneLeaf server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	atomic.AddInt64(&s.activeConns, 1)
	defer atomic.AddInt64(&s.activeConns, -1)

	var req GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.respondError(w, "invalid request", http.StatusBadRequest, start)
		return
	}

	dbStart := time.Now()
	value, ok, err := s.db.Get([]byte(req.Key))
	dbDuration := time.Since(dbStart).Nanoseconds()

	dbOperationDuration.WithLabelValues("get").Observe(float64(dbDuration))

	if err != nil {
		dbOperations.WithLabelValues("get", "error").Inc()
		s.respondError(w, err.Error(), http.StatusInternalServerError, start)
		return
	}

	dbOperations.WithLabelValues("get", "success").Inc()

	resp := Response{
		Success: ok,
		Latency: dbDuration,
	}
	if ok {
		resp.Value = string(value)
	}

	s.respondJSON(w, resp, http.StatusOK, start)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	atomic.AddInt64(&s.activeConns, 1)
	defer atomic.AddInt64(&s.activeConns, -1)

	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.respondError(w, "invalid request", http.StatusBadRequest, start)
		return
	}

	dbStart := time.Now()
	err := s.db.Put([]byte(req.Key), []byte(req.Value))
	dbDuration := time.Since(dbStart).Nanoseconds()

	dbOperationDuration.WithLabelValues("put").Observe(float64(dbDuration))

	if err != nil {
		dbOperations.WithLabelValues("put", "error").Inc()
		s.respondError(w, err.Error(), http.StatusInternalServerError, start)
		return
	}

	dbOperations.WithLabelValues("put", "success").Inc()

	resp := Response{
		Success: true,
		Latency: dbDuration,
	}

	s.respondJSON(w, resp, http.StatusOK, start)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (s *Server) respondJSON(w http.ResponseWriter, data interface{}, status int, start time.Time) {
	duration := time.Since(start).Nanoseconds()
	requestDuration.WithLabelValues("POST", "/get").Observe(float64(duration))
	requestsTotal.WithLabelValues("POST", "/get", strconv.Itoa(status)).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) respondError(w http.ResponseWriter, message string, status int, start time.Time) {
	duration := time.Since(start).Nanoseconds()
	requestDuration.WithLabelValues("POST", "/error").Observe(float64(duration))
	requestsTotal.WithLabelValues("POST", "/error", strconv.Itoa(status)).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Response{
		Success: false,
		Error:   message,
		Latency: duration,
	})
}

func (s *Server) updateMetrics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		activeConnections.Set(float64(atomic.LoadInt64(&s.activeConns)))
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
