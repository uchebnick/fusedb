package kvbench

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConfigRejectsUnknownWorkload(t *testing.T) {
	config := QuickConfig()
	config.RootDir = t.TempDir()
	config.Workloads = []string{"scan"}
	if err := config.normalize(); err == nil || !strings.Contains(err.Error(), "unknown workload") {
		t.Fatalf("normalize error = %v, want unknown workload", err)
	}
}

func TestRunPhaseCountsAndReadsExistingValues(t *testing.T) {
	db := &memoryStore{values: map[string][]byte{"key": {1, 2, 3}}}
	phase, err := runPhase(
		context.Background(), db, workloads["balanced"], 4, 50*time.Millisecond, 1,
		[][]byte{[]byte("key")}, [][]byte{{1, 2, 3}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if phase.operations == 0 || phase.operations != phase.reads+phase.writes {
		t.Fatalf("operations=%d reads=%d writes=%d", phase.operations, phase.reads, phase.writes)
	}
	if phase.histogram.TotalCount() != int64(phase.operations) {
		t.Fatalf("histogram count=%d operations=%d", phase.histogram.TotalCount(), phase.operations)
	}
}

func TestMarkdownDisclosesDurabilityAndSkippedEngines(t *testing.T) {
	report := Report{
		StartedAt: time.Unix(0, 0),
		Runtime:   RuntimeInfo{GOOS: "test", GOARCH: "test", GoVersion: "go-test"},
		Config:    ConfigSnapshot{Keys: 1, ValueBytes: 3, Warmup: "0s", Duration: "1s", Repetitions: 1},
		Skipped:   []EngineInfo{{Name: "rocksdb", Unavailable: "native dependency missing"}},
		Results:   []Result{{Engine: "fusedb", Durability: DurabilitySync, Workload: workloads["read-random"], Workers: 1}},
	}
	markdown := report.Markdown()
	for _, expected := range []string{"native dependency missing", "requests stable WAL durability", "read-random"} {
		if !strings.Contains(markdown, expected) {
			t.Fatalf("Markdown missing %q:\n%s", expected, markdown)
		}
	}
}

func TestBuiltInAdaptersRoundTrip(t *testing.T) {
	registered := factories()
	for _, name := range []string{"fusedb", "pebble", "badger"} {
		t.Run(name, func(t *testing.T) {
			db, err := registered[name].Open(t.TempDir(), engineOptions{
				Durability: DurabilitySync, CacheBytes: 1 << 20, MemtableBytes: 1 << 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Put([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			value, found, err := db.Get([]byte("key"))
			if err != nil || !found || string(value) != "value" {
				t.Fatalf("Get = %q, %v, %v", value, found, err)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunFuseDBSmoke(t *testing.T) {
	config := QuickConfig()
	config.RootDir = t.TempDir()
	config.Engines = []string{"fusedb"}
	config.Workloads = []string{"read-heavy"}
	config.Durabilities = []Durability{DurabilityAsync}
	config.WorkerCounts = []int{2}
	config.Keys = 100
	config.ValueBytes = 32
	config.CacheBytes = 1 << 20
	config.MemtableBytes = 1 << 20
	config.Warmup = 0
	config.Duration = 20 * time.Millisecond

	report, err := Run(context.Background(), config, testWriter{t})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 1 || report.Results[0].Operations == 0 || report.Results[0].Errors != 0 {
		t.Fatalf("unexpected results: %+v", report.Results)
	}
}

type memoryStore struct {
	mu     sync.RWMutex
	values map[string][]byte
}

func (s *memoryStore) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, found := s.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (s *memoryStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *memoryStore) Flush() error { return nil }
func (s *memoryStore) Close() error { return nil }

type testWriter struct {
	t *testing.T
}

func (w testWriter) Write(data []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(data)))
	return len(data), nil
}
