// Command fusedb-qualify runs an accelerated production qualification cycle
// and writes one machine-readable JSON report to stdout.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/uchebnick/fusedb/internal/qualification"
)

func main() {
	os.Exit(run())
}

func run() int {
	dir := flag.String("dir", "", "new or empty FuseDB data directory (required)")
	allowExisting := flag.Bool("allow-existing", false, "allow modifying an existing database directory")
	workers := flag.Int("workers", 16, "foreground workers")
	keys := flag.Int("keys", 50_000, "prepopulated data keys")
	counters := flag.Int("counters", 256, "independent exact counters")
	valueBytes := flag.Int("value-bytes", 256, "value size including integrity header")
	seed := flag.Int64("seed", 1, "deterministic workload seed")
	readPercent := flag.Int("read-percent", 80, "steady/spike read percentage")
	writePercent := flag.Int("write-percent", 15, "steady/spike write percentage")
	incPercent := flag.Int("increment-percent", 5, "steady/spike increment percentage")

	quietDuration := flag.Duration("quiet-duration", time.Minute, "quiet phase duration")
	steadyDuration := flag.Duration("steady-duration", 3*time.Minute, "steady phase duration")
	spikeDuration := flag.Duration("spike-duration", 2*time.Minute, "spike phase duration")
	recoveryDuration := flag.Duration("recovery-duration", 3*time.Minute, "read-only recovery phase duration")
	quietRate := flag.Int("quiet-qps", 250, "quiet phase target operations/second")
	steadyRate := flag.Int("steady-qps", 2_000, "steady phase target operations/second")
	spikeRate := flag.Int("spike-qps", 8_000, "spike phase target operations/second")
	recoveryRate := flag.Int("recovery-qps", 250, "recovery phase target operations/second")

	syncWrites := flag.Bool("wal-sync-writes", true, "wait for WAL sync on every mutation")
	mergeBytes := flag.Int64("merge-bytes", 2<<20, "per-leaf merge trigger bytes")
	maxLeafBytes := flag.Int64("max-leaf-bytes", 32<<20, "maximum leaf bytes before split")
	checkpointBytes := flag.Int64("wal-checkpoint-bytes", 32<<20, "mandatory exact-checkpoint WAL debt")
	pollInterval := flag.Duration("scheduler-poll", 250*time.Millisecond, "scheduler observation interval")
	observationWindow := flag.Duration("scheduler-window", 30*time.Second, "scheduler rolling observation window")
	quietConfirm := flag.Duration("scheduler-quiet-confirm", 10*time.Second, "stable quiet time required for admission")
	recoveryPeriod := flag.Duration("scheduler-recovery", 15*time.Second, "cooldown after foreground pressure")
	targetReadP99 := flag.Duration("scheduler-read-p99", 50*time.Millisecond, "scheduler read p99 target")
	targetWriteP99 := flag.Duration("scheduler-write-p99", 100*time.Millisecond, "scheduler write p99 target")
	cpuCores := flag.Float64("cpu-cores", 0, "scheduler CPU capacity; zero detects GOMAXPROCS")
	diskRate := flag.Float64("disk-bytes-per-second", 0, "scheduler disk capacity; zero learns from database IO")
	memoryBytes := flag.Uint64("memory-bytes", 0, "scheduler memory capacity; zero detects runtime/host limit")
	maxCPU := flag.Float64("max-cpu-utilization", 0.85, "background CPU capacity fraction")
	maxDisk := flag.Float64("max-disk-utilization", 0.85, "background disk capacity fraction")
	maxMemory := flag.Float64("max-memory-utilization", 0.85, "background memory capacity fraction")
	disableDictionaryTraining := flag.Bool("disable-dictionary-training", false, "disable adaptive LZ4 dictionary work")

	sampleInterval := flag.Duration("sample-interval", time.Second, "scheduler/debt report sample interval")
	verifyTimeout := flag.Duration("verify-timeout", 30*time.Minute, "timeout for each full integrity scan")
	minOperations := flag.Uint64("min-operations", 10_000, "minimum completed foreground operations")
	minSamples := flag.Uint64("min-operation-samples", 100, "minimum samples for each active operation in every phase")
	maxErrorRate := flag.Float64("max-error-rate", 0, "maximum foreground error fraction")
	maxReadP99 := flag.Duration("max-read-p99", 50*time.Millisecond, "qualification read p99 limit; zero disables")
	maxWriteP99 := flag.Duration("max-write-p99", 100*time.Millisecond, "qualification write p99 limit; zero disables")
	maxIncP99 := flag.Duration("max-increment-p99", 100*time.Millisecond, "qualification increment p99 limit; zero disables")
	maxFinalBuffer := flag.Int64("max-final-buffered-bytes", 8<<20, "final merge debt limit; zero disables")
	maxFinalWAL := flag.Int64("max-final-wal-bytes", 32<<20, "final WAL debt limit; zero disables")
	requireMaintenance := flag.Bool("require-maintenance", true, "require a completed merge or checkpoint")
	requireDebt := flag.Bool("require-debt-exercise", true, "require workload-created merge/WAL debt")
	requireDrain := flag.Bool("require-debt-drain", true, "require final debt below observed peak")
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "fusedb-qualify: -dir is required")
		return 1
	}

	cfg := qualification.DefaultConfig(*dir)
	cfg.AllowExisting = *allowExisting
	cfg.Workers = *workers
	cfg.KeyCount = *keys
	cfg.CounterCount = *counters
	cfg.ValueBytes = *valueBytes
	cfg.Seed = *seed
	cfg.SampleInterval = *sampleInterval
	cfg.VerifyTimeout = *verifyTimeout
	cfg.Mix = qualification.Mix{Read: *readPercent, Write: *writePercent, Increment: *incPercent}
	cfg.Phases = []qualification.Phase{
		{Name: "quiet", Class: qualification.PhaseQuiet, Duration: *quietDuration, TargetOpsPerSecond: *quietRate, Mix: &qualification.Mix{Read: 95, Write: 4, Increment: 1}},
		{Name: "steady", Class: qualification.PhaseSteady, Duration: *steadyDuration, TargetOpsPerSecond: *steadyRate},
		{Name: "spike", Class: qualification.PhaseSpike, Duration: *spikeDuration, TargetOpsPerSecond: *spikeRate},
		{Name: "recovery", Class: qualification.PhaseRecovery, Duration: *recoveryDuration, TargetOpsPerSecond: *recoveryRate, Mix: &qualification.Mix{Read: 100}},
	}
	cfg.Gates = qualification.Gates{
		MinOperations: *minOperations, MinSamplesPerOperation: *minSamples, MaxErrorRate: *maxErrorRate,
		MaxReadP99: *maxReadP99, MaxWriteP99: *maxWriteP99, MaxIncrementP99: *maxIncP99,
		MaxFinalBufferedBytes: *maxFinalBuffer, MaxFinalWALBytes: *maxFinalWAL,
		RequireMaintenanceProgress: *requireMaintenance,
		RequireDebtExercise:        *requireDebt, RequireDebtDrain: *requireDrain,
	}
	cfg.Database.WALSyncWrites = *syncWrites
	cfg.Database.MergeSize = *mergeBytes
	cfg.Database.MaxLeafSize = *maxLeafBytes
	cfg.Database.WALCheckpointBytes = *checkpointBytes
	cfg.Database.DictionaryTraining.Disabled = *disableDictionaryTraining
	cfg.Database.Scheduler.PollInterval = *pollInterval
	cfg.Database.Scheduler.ObservationWindow = *observationWindow
	cfg.Database.Scheduler.QuietConfirm = *quietConfirm
	cfg.Database.Scheduler.RecoveryPeriod = *recoveryPeriod
	cfg.Database.Scheduler.TargetReadP99 = *targetReadP99
	cfg.Database.Scheduler.TargetWriteP99 = *targetWriteP99
	cfg.Database.Scheduler.CPUCores = *cpuCores
	cfg.Database.Scheduler.DiskBytesPerSecond = *diskRate
	cfg.Database.Scheduler.MemoryBytes = *memoryBytes
	cfg.Database.Scheduler.MaxCPUUtilization = *maxCPU
	cfg.Database.Scheduler.MaxDiskUtilization = *maxDisk
	cfg.Database.Scheduler.MaxMemoryUtilization = *maxMemory
	cfg.Database.Metrics.LatencySampleEvery = 1

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	report, err := qualification.Run(ctx, cfg)
	if report.Version != 0 {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if encodeErr := encoder.Encode(report); encodeErr != nil {
			fmt.Fprintf(os.Stderr, "fusedb-qualify: encode report: %v\n", encodeErr)
			return 1
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "fusedb-qualify: %v\n", err)
		return 1
	}
	if !report.Passed {
		fmt.Fprintln(os.Stderr, "fusedb-qualify: qualification gates failed")
		return 2
	}
	return 0
}
