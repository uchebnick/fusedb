package kvbench

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// Durability describes when a successful write is allowed to return.
type Durability string

const (
	// DurabilityAsync allows the engine to buffer an acknowledged write before
	// the next filesystem sync. It is the low-latency profile.
	DurabilityAsync Durability = "async"
	// DurabilitySync requires every acknowledged write to join a filesystem
	// sync before it returns. Engines may coalesce concurrent sync requests.
	DurabilitySync Durability = "sync"
)

// Workload is a named point-operation mix. All writes overwrite an existing
// key so every engine starts from the same logical dataset.
type Workload struct {
	Name        string `json:"name"`
	ReadPercent int    `json:"read_percent"`
}

var workloads = map[string]Workload{
	"read-random": {Name: "read-random", ReadPercent: 100},
	"read-heavy":  {Name: "read-heavy", ReadPercent: 95},
	"balanced":    {Name: "balanced", ReadPercent: 50},
	"overwrite":   {Name: "overwrite", ReadPercent: 0},
}

// Config is the complete reproducible input to a benchmark matrix.
type Config struct {
	RootDir       string
	Engines       []string
	Workloads     []string
	Durabilities  []Durability
	WorkerCounts  []int
	Keys          int
	ValueBytes    int
	CacheBytes    int64
	MemtableBytes int64
	Warmup        time.Duration
	Duration      time.Duration
	Repetitions   int
	Seed          uint64
}

// QuickConfig returns a short local smoke comparison.
func QuickConfig() Config {
	return Config{
		Engines:       []string{"fusedb", "pebble", "badger"},
		Workloads:     []string{"read-random", "read-heavy", "balanced", "overwrite"},
		Durabilities:  []Durability{DurabilityAsync, DurabilitySync},
		WorkerCounts:  []int{1, 8},
		Keys:          20_000,
		ValueBytes:    256,
		CacheBytes:    64 << 20,
		MemtableBytes: 16 << 20,
		Warmup:        500 * time.Millisecond,
		Duration:      2 * time.Second,
		Repetitions:   1,
		Seed:          1,
	}
}

// StandardConfig returns the checked-in comparison profile. It is long enough
// to reduce timer noise but remains a benchmark, not a production soak test.
func StandardConfig() Config {
	cfg := QuickConfig()
	cfg.Engines = []string{"fusedb", "pebble", "badger", "rocksdb"}
	cfg.Keys = 250_000
	cfg.Warmup = 3 * time.Second
	cfg.Duration = 15 * time.Second
	cfg.Repetitions = 3
	return cfg
}

// Workloads returns the supported workloads in stable display order.
func Workloads() []Workload {
	return []Workload{
		workloads["read-random"],
		workloads["read-heavy"],
		workloads["balanced"],
		workloads["overwrite"],
	}
}

func (c *Config) normalize() error {
	if c.RootDir == "" {
		return fmt.Errorf("benchmark root directory is required")
	}
	if len(c.Engines) == 0 {
		return fmt.Errorf("at least one engine is required")
	}
	if len(c.Workloads) == 0 {
		return fmt.Errorf("at least one workload is required")
	}
	if len(c.Durabilities) == 0 {
		return fmt.Errorf("at least one durability profile is required")
	}
	if len(c.WorkerCounts) == 0 {
		return fmt.Errorf("at least one worker count is required")
	}
	if c.Keys < 1 {
		return fmt.Errorf("keys must be positive")
	}
	if c.ValueBytes < 1 || c.ValueBytes > 1<<20 {
		return fmt.Errorf("value bytes must be in [1, 1048576]")
	}
	if c.CacheBytes < 0 || c.MemtableBytes < 1 {
		return fmt.Errorf("cache bytes must be non-negative and memtable bytes positive")
	}
	if c.Warmup < 0 || c.Duration <= 0 {
		return fmt.Errorf("warmup must be non-negative and duration positive")
	}
	if c.Repetitions < 1 || c.Repetitions > 100 {
		return fmt.Errorf("repetitions must be in [1, 100]")
	}

	c.Engines = normalizeStrings(c.Engines)
	c.Workloads = normalizeStrings(c.Workloads)
	slices.Sort(c.WorkerCounts)
	c.WorkerCounts = slices.Compact(c.WorkerCounts)
	for _, workers := range c.WorkerCounts {
		if workers < 1 || workers > 4096 {
			return fmt.Errorf("workers must be in [1, 4096], got %d", workers)
		}
	}
	for _, name := range c.Workloads {
		if _, ok := workloads[name]; !ok {
			return fmt.Errorf("unknown workload %q", name)
		}
	}
	seenDurability := make(map[Durability]struct{}, len(c.Durabilities))
	cleanDurability := c.Durabilities[:0]
	for _, durability := range c.Durabilities {
		durability = Durability(strings.ToLower(strings.TrimSpace(string(durability))))
		if durability != DurabilityAsync && durability != DurabilitySync {
			return fmt.Errorf("unknown durability %q", durability)
		}
		if _, ok := seenDurability[durability]; ok {
			continue
		}
		seenDurability[durability] = struct{}{}
		cleanDurability = append(cleanDurability, durability)
	}
	c.Durabilities = cleanDurability
	return nil
}

func normalizeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
