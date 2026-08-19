package kvbench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const ReportVersion = 1

// Report is a complete machine-readable benchmark artifact.
type Report struct {
	Version    int            `json:"version"`
	StartedAt  time.Time      `json:"started_at"`
	FinishedAt time.Time      `json:"finished_at"`
	Command    []string       `json:"command,omitempty"`
	Runtime    RuntimeInfo    `json:"runtime"`
	Config     ConfigSnapshot `json:"configuration"`
	Engines    []EngineInfo   `json:"engines"`
	Skipped    []EngineInfo   `json:"skipped,omitempty"`
	Results    []Result       `json:"results"`
}

// RuntimeInfo captures the environment needed to interpret a result.
type RuntimeInfo struct {
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	GoVersion  string `json:"go_version"`
	CPUs       int    `json:"cpus"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	CPUModel   string `json:"cpu_model,omitempty"`
	GitCommit  string `json:"git_commit,omitempty"`
	GitDirty   bool   `json:"git_dirty"`
}

// ConfigSnapshot uses readable duration strings rather than Go nanoseconds.
type ConfigSnapshot struct {
	Engines       []string     `json:"engines"`
	Workloads     []string     `json:"workloads"`
	Durabilities  []Durability `json:"durabilities"`
	WorkerCounts  []int        `json:"worker_counts"`
	Keys          int          `json:"keys"`
	ValueBytes    int          `json:"value_bytes"`
	CacheBytes    int64        `json:"cache_bytes"`
	MemtableBytes int64        `json:"memtable_bytes"`
	Warmup        string       `json:"warmup"`
	Duration      string       `json:"duration"`
	Repetitions   int          `json:"repetitions"`
	Seed          uint64       `json:"seed"`
}

// Result is one isolated engine/workload/durability/worker-count run.
type Result struct {
	Engine           string        `json:"engine"`
	EngineVersion    string        `json:"engine_version"`
	Workload         Workload      `json:"workload"`
	Durability       Durability    `json:"durability"`
	Workers          int           `json:"workers"`
	Repetition       int           `json:"repetition"`
	Operations       uint64        `json:"operations"`
	Reads            uint64        `json:"reads"`
	Writes           uint64        `json:"writes"`
	Errors           uint64        `json:"errors"`
	Elapsed          time.Duration `json:"elapsed_ns"`
	OperationsPerSec float64       `json:"operations_per_second"`
	Latency          Latency       `json:"latency"`
	SeedElapsed      time.Duration `json:"seed_elapsed_ns"`
	OpenElapsed      time.Duration `json:"open_elapsed_ns"`
	DrainElapsed     time.Duration `json:"drain_elapsed_ns"`
	CloseElapsed     time.Duration `json:"close_elapsed_ns"`
	DirectoryBytes   int64         `json:"directory_bytes"`
	VerifiedKeys     int           `json:"verified_keys"`
}

// Latency summarizes the HDR histogram recorded for successful and failed DB
// calls. Values are nanoseconds.
type Latency struct {
	Samples uint64  `json:"samples"`
	Mean    float64 `json:"mean_ns"`
	P50     int64   `json:"p50_ns"`
	P95     int64   `json:"p95_ns"`
	P99     int64   `json:"p99_ns"`
	P999    int64   `json:"p999_ns"`
	Max     int64   `json:"max_ns"`
}

func snapshotConfig(config Config) ConfigSnapshot {
	return ConfigSnapshot{
		Engines:       append([]string(nil), config.Engines...),
		Workloads:     append([]string(nil), config.Workloads...),
		Durabilities:  append([]Durability(nil), config.Durabilities...),
		WorkerCounts:  append([]int(nil), config.WorkerCounts...),
		Keys:          config.Keys,
		ValueBytes:    config.ValueBytes,
		CacheBytes:    config.CacheBytes,
		MemtableBytes: config.MemtableBytes,
		Warmup:        config.Warmup.String(),
		Duration:      config.Duration.String(),
		Repetitions:   config.Repetitions,
		Seed:          config.Seed,
	}
}

func runtimeInfo() RuntimeInfo {
	commit, dirty := gitState()
	return RuntimeInfo{
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		GoVersion:  runtime.Version(),
		CPUs:       runtime.NumCPU(),
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		CPUModel:   cpuModel(),
		GitCommit:  commit,
		GitDirty:   dirty,
	}
}

func gitState() (string, bool) {
	commit, err := exec.Command("git", "rev-parse", "--short=12", "HEAD").Output()
	if err != nil {
		return "", false
	}
	status, err := exec.Command("git", "status", "--porcelain").Output()
	return strings.TrimSpace(string(commit)), err == nil && len(bytes.TrimSpace(status)) != 0
}

func cpuModel() string {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
		if err == nil {
			return strings.TrimSpace(string(out))
		}
	}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			key, value, ok := strings.Cut(line, ":")
			if ok && (strings.TrimSpace(key) == "model name" || strings.TrimSpace(key) == "Hardware") {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

// JSON returns an indented deterministic JSON artifact.
func (r Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// Markdown renders a human-readable table without discarding raw repetitions.
func (r Report) Markdown() string {
	var out strings.Builder
	out.WriteString("# FuseDB comparative benchmark\n\n")
	out.WriteString("> Generated from the versioned JSON report. Lower latency is better; higher throughput is better.\n\n")
	fmt.Fprintf(&out, "Run: `%s` · commit `%s`", r.StartedAt.UTC().Format(time.RFC3339), r.Runtime.GitCommit)
	if r.Runtime.GitDirty {
		out.WriteString(" (dirty working tree)")
	}
	out.WriteString("\n\n")
	if len(r.Command) > 0 {
		out.WriteString("Command:\n\n```bash\n")
		out.WriteString(strings.Join(r.Command, " "))
		out.WriteString("\n```\n\n")
	}
	fmt.Fprintf(&out, "Environment: `%s/%s`, `%s`, `%s`, %d CPUs, GOMAXPROCS=%d.\n\n",
		r.Runtime.GOOS, r.Runtime.GOARCH, r.Runtime.GoVersion, r.Runtime.CPUModel, r.Runtime.CPUs, r.Runtime.GOMAXPROCS)
	fmt.Fprintf(&out, "Dataset: %d keys × %s values; cache %s; memtable/merge target %s; warmup %s; measured %s; repetitions %d.\n\n",
		r.Config.Keys, humanBytes(int64(r.Config.ValueBytes)), humanBytes(r.Config.CacheBytes),
		humanBytes(r.Config.MemtableBytes), r.Config.Warmup, r.Config.Duration, r.Config.Repetitions)

	if len(r.Skipped) > 0 {
		out.WriteString("## Skipped adapters\n\n")
		for _, engine := range r.Skipped {
			fmt.Fprintf(&out, "- `%s`: %s\n", engine.Name, engine.Unavailable)
		}
		out.WriteString("\n")
	}

	out.WriteString("## Median summary\n\n")
	out.WriteString("| Engine | Durability | Workload | Workers | Median ops/s | Median p99 | Runs |\n")
	out.WriteString("|---|---|---|---:|---:|---:|---:|\n")
	results := append([]Result(nil), r.Results...)
	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.Durability != b.Durability {
			return a.Durability < b.Durability
		}
		if a.Workload.Name != b.Workload.Name {
			return a.Workload.Name < b.Workload.Name
		}
		if a.Workers != b.Workers {
			return a.Workers < b.Workers
		}
		if a.Engine != b.Engine {
			return a.Engine < b.Engine
		}
		return a.Repetition < b.Repetition
	})
	for _, group := range aggregateResults(results) {
		fmt.Fprintf(&out, "| %s | %s | %s | %d | %s | %s | %d |\n",
			group.engine, group.durability, group.workload, group.workers,
			humanRate(group.medianRate), humanDuration(group.medianP99), group.runs)
	}
	out.WriteString("\n<details>\n<summary>Raw repetitions</summary>\n\n")
	out.WriteString("| Engine | Durability | Workload | Workers | Repeat | ops/s | p50 | p95 | p99 | p99.9 | Size | Errors |\n")
	out.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, result := range results {
		fmt.Fprintf(&out, "| %s | %s | %s | %d | %d | %s | %s | %s | %s | %s | %s | %d |\n",
			result.Engine, result.Durability, result.Workload.Name, result.Workers, result.Repetition,
			humanRate(result.OperationsPerSec), humanDuration(result.Latency.P50), humanDuration(result.Latency.P95),
			humanDuration(result.Latency.P99), humanDuration(result.Latency.P999), humanBytes(result.DirectoryBytes), result.Errors)
	}
	out.WriteString("\n</details>\n\n## Interpretation rules\n\n")
	out.WriteString("- `async` means acknowledged writes may still be outside stable storage; `sync` requests stable WAL durability for every write.\n")
	out.WriteString("- A row compares point-operation behavior only. FuseDB does not currently expose scans or general transactions, so those workloads are intentionally absent.\n")
	out.WriteString("- Directory size is measured after foreground work is drained and the database is closed. It is not a write-amplification measurement.\n")
	out.WriteString("- Do not compare these numbers with results from another machine, filesystem, build, dataset, or durability profile.\n")
	return out.String()
}

type aggregateResult struct {
	engine     string
	durability Durability
	workload   string
	workers    int
	medianRate float64
	medianP99  int64
	runs       int
}

func aggregateResults(results []Result) []aggregateResult {
	type key struct {
		engine     string
		durability Durability
		workload   string
		workers    int
	}
	type values struct {
		rates []float64
		p99   []int64
	}
	grouped := make(map[key]*values)
	order := make([]key, 0)
	for _, result := range results {
		groupKey := key{
			engine: result.Engine, durability: result.Durability,
			workload: result.Workload.Name, workers: result.Workers,
		}
		group, ok := grouped[groupKey]
		if !ok {
			group = &values{}
			grouped[groupKey] = group
			order = append(order, groupKey)
		}
		group.rates = append(group.rates, result.OperationsPerSec)
		group.p99 = append(group.p99, result.Latency.P99)
	}
	result := make([]aggregateResult, 0, len(order))
	for _, groupKey := range order {
		group := grouped[groupKey]
		sort.Float64s(group.rates)
		sort.Slice(group.p99, func(i, j int) bool { return group.p99[i] < group.p99[j] })
		result = append(result, aggregateResult{
			engine: groupKey.engine, durability: groupKey.durability,
			workload: groupKey.workload, workers: groupKey.workers,
			medianRate: medianFloat(group.rates), medianP99: medianInt64(group.p99), runs: len(group.rates),
		})
	}
	return result
}

func medianFloat(values []float64) float64 {
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

func medianInt64(values []int64) int64 {
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return values[middle-1]/2 + values[middle]/2 + (values[middle-1]%2+values[middle]%2)/2
}

func humanDuration(ns int64) string {
	return time.Duration(ns).String()
}

func humanRate(value float64) string {
	return strconv.FormatFloat(value, 'f', 0, 64)
}

func humanBytes(value int64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	divisor := int64(unit)
	exponent := 0
	for quotient := value / unit; quotient >= unit && exponent < 5; quotient /= unit {
		divisor *= unit
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB", float64(value)/float64(divisor), "KMGTPE"[exponent])
}
