// Command prometheus demonstrates an application-owned FuseDB registry with
// metrics, liveness, readiness, and bounded graceful shutdown endpoints.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uchebnick/fusedb/pkg/fusedb"
	fusedbprom "github.com/uchebnick/fusedb/pkg/fusedb/prometheus"
)

func main() {
	if err := run(); err != nil {
		log.Printf("prometheus example: %v", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "./fusedb-data", "FuseDB data directory")
	listen := flag.String("listen", "127.0.0.1:9090", "metrics listen address")
	flag.Parse()

	db, err := fusedb.Open(fusedb.Options{
		Dir:           *dir,
		WALSyncWrites: true,
	})
	if err != nil {
		return err
	}

	registry := prom.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registration, err := fusedbprom.Register(registry, db, fusedbprom.Options{
		ConstLabels: prom.Labels{"database": "primary"},
	})
	if err != nil {
		return errors.Join(err, db.Close())
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !db.Health().Ready {
			http.Error(w, "fusedb is not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var exitErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			exitErr = fmt.Errorf("metrics server: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := server.Shutdown(shutdownCtx); err != nil {
		exitErr = errors.Join(exitErr, fmt.Errorf("metrics shutdown: %w", err))
	}
	cancel()
	registration.Close()
	if err := db.Close(); err != nil {
		exitErr = errors.Join(exitErr, fmt.Errorf("close FuseDB: %w", err))
	}
	return exitErr
}
