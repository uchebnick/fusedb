package oneleafdb

import (
	"errors"
	"sync"
	"testing"
	"time"

	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
)

func TestConcurrentForegroundLoadWithAdaptiveMerges(t *testing.T) {
	const (
		workers      = 4
		writesPerJob = 1000
	)
	cfg := scheduler.DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ObservationWindow = 25 * time.Millisecond
	cfg.QuietConfirm = 15 * time.Millisecond
	db, err := OpenDB(DBOptions{
		Dir:             t.TempDir(),
		ThresholdBytes:  32 << 10,
		MaxLeafBytes:    128 << 10,
		DisableWAL:      true,
		SchedulerConfig: cfg,
		MetricsConfig:   enginemetrics.Config{LatencySampleEvery: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value := make([]byte, 256)
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for worker := range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			base := worker * writesPerJob
			for i := range writesPerJob {
				key := DBKey(base + i)
				if err := db.Put(key, value); err != nil {
					errors <- err
					return
				}
				if i%4 == 0 {
					if _, found, err := db.Get(key); err != nil || !found {
						if err == nil {
							err = errLoadMissingKey
						}
						errors <- err
						return
					}
				}
			}
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}

	snapshot := db.MetricsSnapshot()
	if snapshot.WriteOps != workers*writesPerJob {
		t.Fatalf("write ops = %d", snapshot.WriteOps)
	}
	if snapshot.ReadOps != workers*(writesPerJob/4) {
		t.Fatalf("read ops = %d", snapshot.ReadOps)
	}
	if snapshot.WriteLatency.Count != snapshot.WriteOps || snapshot.ReadLatency.Count != snapshot.ReadOps {
		t.Fatalf("latency samples read=%d/%d write=%d/%d",
			snapshot.ReadLatency.Count, snapshot.ReadOps,
			snapshot.WriteLatency.Count, snapshot.WriteOps)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
}

var errLoadMissingKey = errors.New("key disappeared during concurrent load")
