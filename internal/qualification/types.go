// Package qualification runs bounded, phased production-qualification
// workloads against the supported FuseDB API.
package qualification

import (
	"time"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

const ReportVersion = 1

// PhaseClass describes the operational intent of one accelerated workload
// phase. It is metadata for reports and gates, never a scheduler hint.
type PhaseClass string

const (
	PhaseQuiet    PhaseClass = "quiet"
	PhaseSteady   PhaseClass = "steady"
	PhaseSpike    PhaseClass = "spike"
	PhaseRecovery PhaseClass = "recovery"
)

// Mix is a percentage distribution of foreground operations.
type Mix struct {
	Read      int `json:"read_percent"`
	Write     int `json:"write_percent"`
	Increment int `json:"increment_percent"`
}

// Phase is one fixed-rate portion of the workload. A nil Mix inherits
// Config.Mix.
type Phase struct {
	Name               string
	Class              PhaseClass
	Duration           time.Duration
	TargetOpsPerSecond int
	Mix                *Mix
}

// Gates are machine-evaluated qualification requirements. Zero latency and
// final-debt limits disable only those individual limits.
type Gates struct {
	MinOperations              uint64
	MinSamplesPerOperation     uint64
	MaxErrorRate               float64
	MaxReadP99                 time.Duration
	MaxWriteP99                time.Duration
	MaxIncrementP99            time.Duration
	MaxFinalBufferedBytes      int64
	MaxFinalWALBytes           int64
	RequireMaintenanceProgress bool
	RequireDebtExercise        bool
	RequireDebtDrain           bool
}

// Config defines a complete, reproducible qualification run.
type Config struct {
	Dir            string
	AllowExisting  bool
	Workers        int
	KeyCount       int
	CounterCount   int
	ValueBytes     int
	Seed           int64
	SampleInterval time.Duration
	VerifyTimeout  time.Duration
	Mix            Mix
	Phases         []Phase
	Gates          Gates
	Database       fusedb.Options
}

// Report is a machine-readable evidence artifact. All durations in nested
// performance summaries use nanoseconds.
type Report struct {
	Version       int                `json:"version"`
	StartedAt     time.Time          `json:"started_at"`
	FinishedAt    time.Time          `json:"finished_at"`
	DurationNanos int64              `json:"duration_nanos"`
	Runtime       RuntimeReport      `json:"runtime"`
	Configuration ConfigReport       `json:"configuration"`
	Seed          SeedReport         `json:"seed"`
	Phases        []PhaseReport      `json:"phases"`
	Overall       OperationReport    `json:"overall"`
	Background    []BackgroundDelta  `json:"background"`
	Peak          DebtReport         `json:"peak"`
	Final         DebtReport         `json:"final"`
	Verification  VerificationReport `json:"verification"`
	Passed        bool               `json:"passed"`
	Failures      []string           `json:"failures,omitempty"`
}

type RuntimeReport struct {
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	GoVersion  string `json:"go_version"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	NumCPU     int    `json:"num_cpu"`
	Revision   string `json:"revision,omitempty"`
	Modified   bool   `json:"modified"`
}

type ConfigReport struct {
	Dir                  string              `json:"dir"`
	Workers              int                 `json:"workers"`
	KeyCount             int                 `json:"key_count"`
	CounterCount         int                 `json:"counter_count"`
	ValueBytes           int                 `json:"value_bytes"`
	Seed                 int64               `json:"seed"`
	SampleIntervalNanos  int64               `json:"sample_interval_nanos"`
	WALSyncWrites        bool                `json:"wal_sync_writes"`
	MergeBytes           int64               `json:"merge_bytes"`
	MaxLeafBytes         int64               `json:"max_leaf_bytes"`
	WALCheckpointBytes   int64               `json:"wal_checkpoint_bytes"`
	TargetReadP99Nanos   int64               `json:"target_read_p99_nanos"`
	TargetWriteP99Nanos  int64               `json:"target_write_p99_nanos"`
	CPUCores             float64             `json:"cpu_cores"`
	DiskBytesPerSecond   float64             `json:"disk_bytes_per_second"`
	MemoryBytes          uint64              `json:"memory_bytes"`
	MaxCPUUtilization    float64             `json:"max_cpu_utilization"`
	MaxDiskUtilization   float64             `json:"max_disk_utilization"`
	MaxMemoryUtilization float64             `json:"max_memory_utilization"`
	Mix                  Mix                 `json:"mix"`
	Phases               []PhaseConfigReport `json:"phases"`
	Gates                GateReport          `json:"gates"`
}

type PhaseConfigReport struct {
	Name               string     `json:"name"`
	Class              PhaseClass `json:"class"`
	DurationNanos      int64      `json:"duration_nanos"`
	TargetOpsPerSecond int        `json:"target_ops_per_second"`
	Mix                Mix        `json:"mix"`
}

type GateReport struct {
	MinOperations              uint64  `json:"min_operations"`
	MinSamplesPerOperation     uint64  `json:"min_samples_per_operation"`
	MaxErrorRate               float64 `json:"max_error_rate"`
	MaxReadP99Nanos            int64   `json:"max_read_p99_nanos"`
	MaxWriteP99Nanos           int64   `json:"max_write_p99_nanos"`
	MaxIncrementP99Nanos       int64   `json:"max_increment_p99_nanos"`
	MaxFinalBufferedBytes      int64   `json:"max_final_buffered_bytes"`
	MaxFinalWALBytes           int64   `json:"max_final_wal_bytes"`
	RequireMaintenanceProgress bool    `json:"require_maintenance_progress"`
	RequireDebtExercise        bool    `json:"require_debt_exercise"`
	RequireDebtDrain           bool    `json:"require_debt_drain"`
}

type SeedReport struct {
	Keys          int   `json:"keys"`
	DurationNanos int64 `json:"duration_nanos"`
}

type PhaseReport struct {
	Name               string            `json:"name"`
	Class              PhaseClass        `json:"class"`
	StartedAt          time.Time         `json:"started_at"`
	DurationNanos      int64             `json:"duration_nanos"`
	TargetOpsPerSecond int               `json:"target_ops_per_second"`
	Mix                Mix               `json:"mix"`
	ActualOpsPerSecond float64           `json:"actual_ops_per_second"`
	Operations         OperationReport   `json:"operations"`
	Background         []BackgroundDelta `json:"background"`
	SchedulerSamples   map[string]uint64 `json:"scheduler_samples"`
	Peak               DebtReport        `json:"peak"`
	End                DebtReport        `json:"end"`
}

type OperationReport struct {
	Read      LatencyReport `json:"read"`
	Write     LatencyReport `json:"write"`
	Increment LatencyReport `json:"increment"`
	Total     uint64        `json:"total"`
	Errors    uint64        `json:"errors"`
	ErrorRate float64       `json:"error_rate"`
}

type LatencyReport struct {
	Count        uint64 `json:"count"`
	Errors       uint64 `json:"errors"`
	SumNanos     uint64 `json:"sum_nanos"`
	AverageNanos int64  `json:"average_nanos"`
	P50Nanos     int64  `json:"p50_nanos"`
	P95Nanos     int64  `json:"p95_nanos"`
	P99Nanos     int64  `json:"p99_nanos"`
	MaxNanos     int64  `json:"max_nanos"`
	FirstError   string `json:"first_error,omitempty"`
}

type BackgroundDelta struct {
	Kind          string `json:"kind"`
	Started       uint64 `json:"started"`
	Completed     uint64 `json:"completed"`
	Failed        uint64 `json:"failed"`
	Cancelled     uint64 `json:"cancelled"`
	DurationNanos int64  `json:"duration_nanos"`
}

type DebtReport struct {
	BufferedBytes      int64   `json:"buffered_bytes"`
	WALBytes           int64   `json:"wal_bytes"`
	PendingMergeLeaves int     `json:"pending_merge_leaves"`
	SchedulerQueued    int     `json:"scheduler_queued"`
	SchedulerRunning   bool    `json:"scheduler_running"`
	CPUUtilization     float64 `json:"cpu_utilization"`
	DiskUtilization    float64 `json:"disk_utilization"`
	MemoryUtilization  float64 `json:"memory_utilization"`
}

type VerificationReport struct {
	PreCloseValues     bool   `json:"pre_close_values"`
	PreCloseVerify     bool   `json:"pre_close_verify"`
	PreCloseCounters   bool   `json:"pre_close_counters"`
	Reopen             bool   `json:"reopen"`
	PostReopenVerify   bool   `json:"post_reopen_verify"`
	PostReopenValues   bool   `json:"post_reopen_values"`
	PostReopenCounters bool   `json:"post_reopen_counters"`
	ExpectedIncrements uint64 `json:"expected_increments"`
	ObservedIncrements uint64 `json:"observed_increments"`
}
