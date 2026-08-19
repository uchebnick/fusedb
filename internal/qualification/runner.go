package qualification

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/pkg/fusedb"
)

var ErrDirectoryNotEmpty = errors.New("qualification: data directory is not empty")

// DefaultConfig returns an accelerated quiet/steady/spike/recovery workload.
// Production qualification should extend the durations and tune rates/SLOs to
// the target hardware and real application contract.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:            dir,
		Workers:        16,
		KeyCount:       50_000,
		CounterCount:   256,
		ValueBytes:     256,
		Seed:           1,
		SampleInterval: time.Second,
		VerifyTimeout:  30 * time.Minute,
		Mix:            Mix{Read: 80, Write: 15, Increment: 5},
		Phases: []Phase{
			{Name: "quiet", Class: PhaseQuiet, Duration: time.Minute, TargetOpsPerSecond: 250, Mix: &Mix{Read: 95, Write: 4, Increment: 1}},
			{Name: "steady", Class: PhaseSteady, Duration: 3 * time.Minute, TargetOpsPerSecond: 2_000},
			{Name: "spike", Class: PhaseSpike, Duration: 2 * time.Minute, TargetOpsPerSecond: 8_000},
			{Name: "recovery", Class: PhaseRecovery, Duration: 3 * time.Minute, TargetOpsPerSecond: 250, Mix: &Mix{Read: 100}},
		},
		Gates: Gates{
			MinOperations:              10_000,
			MinSamplesPerOperation:     100,
			MaxErrorRate:               0,
			MaxReadP99:                 50 * time.Millisecond,
			MaxWriteP99:                100 * time.Millisecond,
			MaxIncrementP99:            100 * time.Millisecond,
			MaxFinalBufferedBytes:      8 << 20,
			MaxFinalWALBytes:           32 << 20,
			RequireMaintenanceProgress: true,
			RequireDebtExercise:        true,
			RequireDebtDrain:           true,
		},
		Database: fusedb.Options{
			MergeSize:          2 << 20,
			MaxLeafSize:        32 << 20,
			WALSyncWrites:      true,
			WALCheckpointBytes: 32 << 20,
			Metrics:            fusedb.MetricsOptions{LatencySampleEvery: 1},
			Scheduler: fusedb.SchedulerOptions{
				PollInterval:         250 * time.Millisecond,
				ObservationWindow:    30 * time.Second,
				QuietConfirm:         10 * time.Second,
				RecoveryPeriod:       15 * time.Second,
				TargetReadP99:        50 * time.Millisecond,
				TargetWriteP99:       100 * time.Millisecond,
				MaxCPUUtilization:    0.85,
				MaxDiskUtilization:   0.85,
				MaxMemoryUtilization: 0.85,
			},
		},
	}
}

// Run executes a complete qualification cycle and leaves Dir intact for
// forensic inspection. A failed gate is represented in Report and does not by
// itself return an execution error.
func Run(ctx context.Context, cfg Config) (report Report, runErr error) {
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return Report{}, err
	}
	if !cfg.AllowExisting {
		if err := requireEmptyOrMissingDirectory(cfg.Dir); err != nil {
			return Report{}, err
		}
	}

	report = Report{
		Version:       ReportVersion,
		StartedAt:     time.Now().UTC(),
		Runtime:       runtimeReport(),
		Configuration: configReport(cfg),
		Passed:        false,
	}
	defer func() {
		report.FinishedAt = time.Now().UTC()
		report.DurationNanos = report.FinishedAt.Sub(report.StartedAt).Nanoseconds()
	}()

	options := cfg.Database
	options.Dir = cfg.Dir
	if options.Metrics.LatencySampleEvery == 0 {
		options.Metrics.LatencySampleEvery = 1
	}
	db, err := fusedb.Open(options)
	if err != nil {
		return report, fmt.Errorf("qualification: open: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	dataKeys, counterKeys := buildKeys(cfg.KeyCount, cfg.CounterCount)
	seedStarted := time.Now()
	for index, key := range dataKeys {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := db.Put(key, buildValue(index, 0, cfg.ValueBytes)); err != nil {
			return report, fmt.Errorf("qualification: seed key %d: %w", index, err)
		}
	}
	if err := db.Merge(); err != nil {
		return report, fmt.Errorf("qualification: seed checkpoint: %w", err)
	}
	report.Seed = SeedReport{Keys: len(dataKeys), DurationNanos: time.Since(seedStarted).Nanoseconds()}

	baselineMetrics := db.Metrics()
	baselineDebt := debtSnapshot(db)
	phaseStats := make([]phaseOperationStats, len(cfg.Phases))
	phaseRuns := make([]phaseRuntime, len(cfg.Phases))
	expectedCounters := make([]atomic.Int64, len(counterKeys))
	var generation atomic.Uint64
	var phaseIndex atomic.Int64
	phaseIndex.Store(-1)

	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	failure := &firstFailure{}
	pacing := newPacer()
	pacing.pause()

	var workerGroup sync.WaitGroup
	for workerID := range cfg.Workers {
		workerGroup.Add(1)
		go func() {
			defer workerGroup.Done()
			random := rand.New(rand.NewSource(cfg.Seed + int64(workerID+1)*1_000_003))
			for pacing.wait(workCtx) {
				index := int(phaseIndex.Load())
				if index < 0 || index >= len(cfg.Phases) {
					continue
				}
				mix := phaseMix(cfg, cfg.Phases[index])
				roll := random.Intn(100)
				var operationErr error
				switch {
				case roll < mix.Read:
					keyIndex := random.Intn(len(dataKeys))
					started := time.Now()
					value, found, err := db.Get(dataKeys[keyIndex])
					if err == nil && !found {
						err = errors.New("qualification: seeded key not found")
					}
					if err == nil {
						err = validateValue(value, keyIndex)
					}
					operationErr = err
					phaseStats[index].read.record(time.Since(started), err)
				case roll < mix.Read+mix.Write:
					keyIndex := random.Intn(len(dataKeys))
					value := buildValue(keyIndex, generation.Add(1), cfg.ValueBytes)
					started := time.Now()
					operationErr = db.Put(dataKeys[keyIndex], value)
					phaseStats[index].write.record(time.Since(started), operationErr)
				default:
					keyIndex := random.Intn(len(counterKeys))
					started := time.Now()
					operationErr = db.Inc(counterKeys[keyIndex], 1)
					phaseStats[index].increment.record(time.Since(started), operationErr)
					if operationErr == nil {
						expectedCounters[keyIndex].Add(1)
					}
				}
				if operationErr != nil {
					failure.set(operationErr)
					cancelWork()
					return
				}
			}
		}()
	}

	monitor := newRunMonitor(len(cfg.Phases), baselineDebt)
	var monitorGroup sync.WaitGroup
	monitorGroup.Add(1)
	go func() {
		defer monitorGroup.Done()
		ticker := time.NewTicker(cfg.SampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				index := int(phaseIndex.Load())
				monitor.observe(index, db.Stats(), db.Metrics().Resources)
				if health := db.Health(); !health.Ready {
					failure.set(errors.New("qualification: database became not ready"))
					cancelWork()
					return
				}
			}
		}
	}()

	for index, phase := range cfg.Phases {
		if err := workCtx.Err(); err != nil {
			break
		}
		phaseIndex.Store(int64(index))
		phaseRuns[index].StartedAt = time.Now().UTC()
		phaseRuns[index].BeforeMetrics = db.Metrics()
		pacing.setRate(phase.TargetOpsPerSecond)
		timer := time.NewTimer(phase.Duration)
		select {
		case <-timer.C:
		case <-workCtx.Done():
			timer.Stop()
		}
		pacing.pause()
		phaseRuns[index].Duration = time.Since(phaseRuns[index].StartedAt)
		phaseRuns[index].AfterMetrics = db.Metrics()
		phaseRuns[index].End = debtSnapshot(db)
		monitor.observe(index, db.Stats(), db.Metrics().Resources)
	}

	phaseIndex.Store(-1)
	pacing.pause()
	cancelWork()
	workerGroup.Wait()
	monitorGroup.Wait()
	finalMetrics := db.Metrics()
	finalDebt := debtSnapshot(db)

	var overall phaseOperationSnapshot
	for index, phase := range cfg.Phases {
		operations := phaseStats[index].snapshot()
		overall.add(operations)
		monitored := monitor.phase(index)
		duration := phaseRuns[index].Duration
		actualRate := 0.0
		if duration > 0 {
			actualRate = float64(operations.report().Total) / duration.Seconds()
		}
		report.Phases = append(report.Phases, PhaseReport{
			Name:               phase.Name,
			Class:              phase.Class,
			StartedAt:          phaseRuns[index].StartedAt,
			DurationNanos:      duration.Nanoseconds(),
			TargetOpsPerSecond: phase.TargetOpsPerSecond,
			Mix:                phaseMix(cfg, phase),
			ActualOpsPerSecond: actualRate,
			Operations:         operations.report(),
			Background:         backgroundDelta(phaseRuns[index].BeforeMetrics, phaseRuns[index].AfterMetrics),
			SchedulerSamples:   monitored.SchedulerSamples,
			Peak:               monitored.Peak,
			End:                phaseRuns[index].End,
		})
	}
	report.Overall = overall.report()
	report.Background = backgroundDelta(baselineMetrics, finalMetrics)
	report.Peak = monitor.overall()
	report.Final = finalDebt

	if err := failure.get(); err != nil {
		report.Failures = append(report.Failures, err.Error())
		runErr = err
	} else if err := ctx.Err(); err != nil {
		report.Failures = append(report.Failures, err.Error())
		runErr = err
	} else {
		if err := validateDataValues(db, dataKeys); err != nil {
			report.Failures = append(report.Failures, err.Error())
			runErr = errors.Join(runErr, err)
		} else {
			report.Verification.PreCloseValues = true
		}
		verifyCtx, cancel := context.WithTimeout(context.Background(), cfg.VerifyTimeout)
		_, err = db.Verify(verifyCtx)
		cancel()
		if err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("pre-close verify: %v", err))
			runErr = errors.Join(runErr, err)
		} else {
			report.Verification.PreCloseVerify = true
		}
		preCloseOK, expected, observed, err := validateCounters(db, counterKeys, expectedCounters)
		report.Verification.ExpectedIncrements = expected
		report.Verification.ObservedIncrements = observed
		if err != nil {
			report.Failures = append(report.Failures, err.Error())
			runErr = errors.Join(runErr, err)
		}
		report.Verification.PreCloseCounters = preCloseOK
	}

	if err := db.Close(); err != nil {
		report.Failures = append(report.Failures, fmt.Sprintf("close: %v", err))
		runErr = errors.Join(runErr, err)
	}
	closed = true

	if runErr == nil {
		reopened, err := fusedb.Open(options)
		if err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("reopen: %v", err))
			runErr = err
		} else {
			report.Verification.Reopen = true
			if health := reopened.Health(); !health.Ready {
				err = errors.New("qualification: reopened database is not ready")
				report.Failures = append(report.Failures, err.Error())
				runErr = errors.Join(runErr, err)
			}
			if valueErr := validateDataValues(reopened, dataKeys); valueErr != nil {
				report.Failures = append(report.Failures, valueErr.Error())
				runErr = errors.Join(runErr, valueErr)
			} else {
				report.Verification.PostReopenValues = true
			}
			countersOK, expected, observed, counterErr := validateCounters(reopened, counterKeys, expectedCounters)
			report.Verification.ExpectedIncrements = expected
			report.Verification.ObservedIncrements = observed
			report.Verification.PostReopenCounters = countersOK
			if counterErr != nil {
				report.Failures = append(report.Failures, counterErr.Error())
				runErr = errors.Join(runErr, counterErr)
			}
			verifyCtx, cancel := context.WithTimeout(context.Background(), cfg.VerifyTimeout)
			_, verifyErr := reopened.Verify(verifyCtx)
			cancel()
			if verifyErr != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("post-reopen verify: %v", verifyErr))
				runErr = errors.Join(runErr, verifyErr)
			} else {
				report.Verification.PostReopenVerify = true
			}
			if closeErr := reopened.Close(); closeErr != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("close reopened database: %v", closeErr))
				runErr = errors.Join(runErr, closeErr)
			}
		}
	}

	applyGates(cfg.Gates, &report)
	report.Passed = runErr == nil && len(report.Failures) == 0
	return report, runErr
}

type phaseRuntime struct {
	StartedAt     time.Time
	Duration      time.Duration
	BeforeMetrics fusedb.MetricsSnapshot
	AfterMetrics  fusedb.MetricsSnapshot
	End           DebtReport
}

func normalizeConfig(cfg Config) Config {
	if cfg.Workers == 0 {
		cfg.Workers = 8
	}
	if cfg.KeyCount == 0 {
		cfg.KeyCount = 10_000
	}
	if cfg.CounterCount == 0 {
		cfg.CounterCount = 128
	}
	if cfg.ValueBytes == 0 {
		cfg.ValueBytes = 256
	}
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}
	if cfg.SampleInterval == 0 {
		cfg.SampleInterval = time.Second
	}
	if cfg.VerifyTimeout == 0 {
		cfg.VerifyTimeout = 30 * time.Minute
	}
	if cfg.Mix == (Mix{}) {
		cfg.Mix = Mix{Read: 80, Write: 15, Increment: 5}
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.Dir == "" {
		return errors.New("qualification: Dir is required")
	}
	if cfg.Workers <= 0 || cfg.Workers > 4096 {
		return errors.New("qualification: Workers must be in [1,4096]")
	}
	if cfg.KeyCount <= 0 || cfg.KeyCount > 2_000_000 {
		return errors.New("qualification: KeyCount must be in [1,2000000]")
	}
	if cfg.CounterCount <= 0 || cfg.CounterCount > 1_000_000 {
		return errors.New("qualification: CounterCount must be in [1,1000000]")
	}
	if cfg.ValueBytes < qualificationValueHeader || cfg.ValueBytes > fusedb.MaxValueSize {
		return fmt.Errorf("qualification: ValueBytes must be in [%d,%d]", qualificationValueHeader, fusedb.MaxValueSize)
	}
	if uint64(cfg.Workers)*uint64(cfg.ValueBytes) > 512<<20 {
		return errors.New("qualification: concurrent value working set exceeds 512 MiB")
	}
	if cfg.SampleInterval <= 0 || cfg.VerifyTimeout <= 0 {
		return errors.New("qualification: sample and verify intervals must be positive")
	}
	if err := validateMix(cfg.Mix); err != nil {
		return err
	}
	if len(cfg.Phases) == 0 {
		return errors.New("qualification: at least one phase is required")
	}
	names := make(map[string]struct{}, len(cfg.Phases))
	for _, phase := range cfg.Phases {
		if phase.Name == "" {
			return errors.New("qualification: phase name is required")
		}
		if _, exists := names[phase.Name]; exists {
			return fmt.Errorf("qualification: duplicate phase %q", phase.Name)
		}
		names[phase.Name] = struct{}{}
		switch phase.Class {
		case PhaseQuiet, PhaseSteady, PhaseSpike, PhaseRecovery:
		default:
			return fmt.Errorf("qualification: phase %q has invalid class %q", phase.Name, phase.Class)
		}
		if phase.Duration <= 0 || phase.TargetOpsPerSecond <= 0 || phase.TargetOpsPerSecond > 10_000_000 {
			return fmt.Errorf("qualification: phase %q has invalid duration or rate", phase.Name)
		}
		if phase.Mix != nil {
			if err := validateMix(*phase.Mix); err != nil {
				return fmt.Errorf("qualification: phase %q: %w", phase.Name, err)
			}
		}
	}
	if cfg.Gates.MaxErrorRate < 0 || cfg.Gates.MaxErrorRate > 1 {
		return errors.New("qualification: MaxErrorRate must be in [0,1]")
	}
	if cfg.Gates.MaxReadP99 < 0 || cfg.Gates.MaxWriteP99 < 0 || cfg.Gates.MaxIncrementP99 < 0 ||
		cfg.Gates.MaxFinalBufferedBytes < 0 || cfg.Gates.MaxFinalWALBytes < 0 {
		return errors.New("qualification: gate limits cannot be negative")
	}
	return nil
}

func validateMix(mix Mix) error {
	if mix.Read < 0 || mix.Write < 0 || mix.Increment < 0 || mix.Read+mix.Write+mix.Increment != 100 {
		return errors.New("operation percentages must be non-negative and sum to 100")
	}
	return nil
}

func phaseMix(cfg Config, phase Phase) Mix {
	if phase.Mix != nil {
		return *phase.Mix
	}
	return cfg.Mix
}

func requireEmptyOrMissingDirectory(name string) error {
	info, err := disk.DefaultFS.Stat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("qualification: inspect data directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("qualification: data path is not a directory: %s", name)
	}
	names, err := disk.DefaultFS.List(name)
	if err != nil {
		return fmt.Errorf("qualification: list data directory: %w", err)
	}
	if len(names) != 0 {
		return fmt.Errorf("%w: %s", ErrDirectoryNotEmpty, name)
	}
	return nil
}

func buildKeys(keyCount, counterCount int) ([][]byte, [][]byte) {
	data := make([][]byte, keyCount)
	for index := range data {
		data[index] = []byte(fmt.Sprintf("data:%010d", index))
	}
	counters := make([][]byte, counterCount)
	for index := range counters {
		counters[index] = []byte(fmt.Sprintf("counter:%010d", index))
	}
	return data, counters
}

func validateCounters(db *fusedb.DB, keys [][]byte, expected []atomic.Int64) (bool, uint64, uint64, error) {
	var expectedTotal uint64
	var observedTotal uint64
	for index, key := range keys {
		want := expected[index].Load()
		expectedTotal += uint64(want)
		got, found, err := db.GetInt64(key)
		if err != nil {
			return false, expectedTotal, observedTotal, fmt.Errorf("qualification: read counter %d: %w", index, err)
		}
		if !found {
			if want == 0 {
				continue
			}
			return false, expectedTotal, observedTotal, fmt.Errorf("qualification: counter %d missing, want %d", index, want)
		}
		if got > 0 {
			observedTotal += uint64(got)
		}
		if got != want {
			return false, expectedTotal, observedTotal, fmt.Errorf("qualification: counter %d = %d, want %d", index, got, want)
		}
	}
	return true, expectedTotal, observedTotal, nil
}

func validateDataValues(db *fusedb.DB, keys [][]byte) error {
	for index, key := range keys {
		value, found, err := db.Get(key)
		if err != nil {
			return fmt.Errorf("qualification: validate data key %d: %w", index, err)
		}
		if !found {
			return fmt.Errorf("qualification: data key %d is missing", index)
		}
		if err := validateValue(value, index); err != nil {
			return fmt.Errorf("qualification: validate data key %d: %w", index, err)
		}
	}
	return nil
}

func debtSnapshot(db *fusedb.DB) DebtReport {
	stats := db.Stats()
	resources := db.Metrics().Resources
	return DebtReport{
		BufferedBytes:      stats.BufferedBytes,
		WALBytes:           stats.WALBytes,
		PendingMergeLeaves: stats.PendingMergeLeaves,
		SchedulerQueued:    stats.SchedulerQueued,
		SchedulerRunning:   stats.SchedulerRunning,
		CPUUtilization:     resources.CPUUtilization,
		DiskUtilization:    resources.DiskUtilization,
		MemoryUtilization:  resources.MemoryUtilization,
	}
}

func backgroundDelta(before, after fusedb.MetricsSnapshot) []BackgroundDelta {
	previous := make(map[string]fusedb.BackgroundMetrics, len(before.Background))
	for _, item := range before.Background {
		previous[item.Kind] = item
	}
	result := make([]BackgroundDelta, 0, len(after.Background))
	for _, item := range after.Background {
		old := previous[item.Kind]
		result = append(result, BackgroundDelta{
			Kind:          item.Kind,
			Started:       nonNegativeDelta(item.Started, old.Started),
			Completed:     nonNegativeDelta(item.Completed, old.Completed),
			Failed:        nonNegativeDelta(item.Failed, old.Failed),
			Cancelled:     nonNegativeDelta(item.Cancelled, old.Cancelled),
			DurationNanos: max(0, (item.TotalTime - old.TotalTime).Nanoseconds()),
		})
	}
	return result
}

func nonNegativeDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

func applyGates(gates Gates, report *Report) {
	add := func(format string, args ...any) {
		report.Failures = append(report.Failures, fmt.Sprintf(format, args...))
	}
	if report.Overall.Total < gates.MinOperations {
		add("operations %d below required %d", report.Overall.Total, gates.MinOperations)
	}
	if report.Overall.ErrorRate > gates.MaxErrorRate {
		add("error rate %.6f exceeds %.6f", report.Overall.ErrorRate, gates.MaxErrorRate)
	}
	for _, phase := range report.Phases {
		checkLatencyGate := func(operation string, measured LatencyReport, limit time.Duration) {
			if limit == 0 {
				return
			}
			if measured.Count < gates.MinSamplesPerOperation {
				add("phase %s %s samples %d below required %d", phase.Name, operation, measured.Count, gates.MinSamplesPerOperation)
				return
			}
			if measured.P99Nanos > limit.Nanoseconds() {
				add("phase %s %s p99 %s exceeds %s", phase.Name, operation, time.Duration(measured.P99Nanos), limit)
			}
		}
		if phase.Mix.Read > 0 {
			checkLatencyGate("read", phase.Operations.Read, gates.MaxReadP99)
		}
		if phase.Mix.Write > 0 {
			checkLatencyGate("write", phase.Operations.Write, gates.MaxWriteP99)
		}
		if phase.Mix.Increment > 0 {
			checkLatencyGate("increment", phase.Operations.Increment, gates.MaxIncrementP99)
		}
	}
	if gates.MaxFinalBufferedBytes > 0 && report.Final.BufferedBytes > gates.MaxFinalBufferedBytes {
		add("final buffered debt %d exceeds %d", report.Final.BufferedBytes, gates.MaxFinalBufferedBytes)
	}
	if gates.MaxFinalWALBytes > 0 && report.Final.WALBytes > gates.MaxFinalWALBytes {
		add("final WAL debt %d exceeds %d", report.Final.WALBytes, gates.MaxFinalWALBytes)
	}
	maintenanceCompleted := uint64(0)
	for _, item := range report.Background {
		if item.Kind == "merge" || item.Kind == "checkpoint" {
			maintenanceCompleted += item.Completed
		}
		if item.Failed != 0 {
			add("background %s failures = %d", item.Kind, item.Failed)
		}
	}
	if gates.RequireMaintenanceProgress && maintenanceCompleted == 0 {
		add("no merge or checkpoint completed during workload")
	}
	peakDebt := report.Peak.BufferedBytes + report.Peak.WALBytes
	finalDebt := report.Final.BufferedBytes + report.Final.WALBytes
	if gates.RequireDebtExercise && peakDebt == 0 {
		add("workload did not exercise merge or WAL debt")
	}
	if gates.RequireDebtDrain && (peakDebt == 0 || finalDebt >= peakDebt) {
		add("maintenance debt did not drain after peak: peak=%d final=%d", peakDebt, finalDebt)
	}
	if !report.Verification.PreCloseValues || !report.Verification.PreCloseVerify || !report.Verification.PreCloseCounters ||
		!report.Verification.Reopen || !report.Verification.PostReopenValues ||
		!report.Verification.PostReopenVerify || !report.Verification.PostReopenCounters {
		add("durability verification cycle did not complete successfully")
	}
}

func runtimeReport() RuntimeReport {
	report := RuntimeReport{
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		GOMAXPROCS: runtime.GOMAXPROCS(0), NumCPU: runtime.NumCPU(),
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				report.Revision = setting.Value
			case "vcs.modified":
				report.Modified = setting.Value == "true"
			}
		}
	}
	return report
}

func configReport(cfg Config) ConfigReport {
	phases := make([]PhaseConfigReport, 0, len(cfg.Phases))
	for _, phase := range cfg.Phases {
		phases = append(phases, PhaseConfigReport{
			Name: phase.Name, Class: phase.Class, DurationNanos: phase.Duration.Nanoseconds(),
			TargetOpsPerSecond: phase.TargetOpsPerSecond, Mix: phaseMix(cfg, phase),
		})
	}
	return ConfigReport{
		Dir: cfg.Dir, Workers: cfg.Workers, KeyCount: cfg.KeyCount,
		CounterCount: cfg.CounterCount, ValueBytes: cfg.ValueBytes, Seed: cfg.Seed,
		SampleIntervalNanos: cfg.SampleInterval.Nanoseconds(),
		WALSyncWrites:       cfg.Database.WALSyncWrites, MergeBytes: cfg.Database.MergeSize,
		MaxLeafBytes: cfg.Database.MaxLeafSize, WALCheckpointBytes: cfg.Database.WALCheckpointBytes,
		TargetReadP99Nanos:  cfg.Database.Scheduler.TargetReadP99.Nanoseconds(),
		TargetWriteP99Nanos: cfg.Database.Scheduler.TargetWriteP99.Nanoseconds(),
		CPUCores:            cfg.Database.Scheduler.CPUCores, DiskBytesPerSecond: cfg.Database.Scheduler.DiskBytesPerSecond,
		MemoryBytes:          cfg.Database.Scheduler.MemoryBytes,
		MaxCPUUtilization:    cfg.Database.Scheduler.MaxCPUUtilization,
		MaxDiskUtilization:   cfg.Database.Scheduler.MaxDiskUtilization,
		MaxMemoryUtilization: cfg.Database.Scheduler.MaxMemoryUtilization,
		Mix:                  cfg.Mix, Phases: phases,
		Gates: GateReport{
			MinOperations:              cfg.Gates.MinOperations,
			MinSamplesPerOperation:     cfg.Gates.MinSamplesPerOperation,
			MaxErrorRate:               cfg.Gates.MaxErrorRate,
			MaxReadP99Nanos:            cfg.Gates.MaxReadP99.Nanoseconds(),
			MaxWriteP99Nanos:           cfg.Gates.MaxWriteP99.Nanoseconds(),
			MaxIncrementP99Nanos:       cfg.Gates.MaxIncrementP99.Nanoseconds(),
			MaxFinalBufferedBytes:      cfg.Gates.MaxFinalBufferedBytes,
			MaxFinalWALBytes:           cfg.Gates.MaxFinalWALBytes,
			RequireMaintenanceProgress: cfg.Gates.RequireMaintenanceProgress,
			RequireDebtExercise:        cfg.Gates.RequireDebtExercise,
			RequireDebtDrain:           cfg.Gates.RequireDebtDrain,
		},
	}
}

type firstFailure struct {
	once sync.Once
	err  error
}

func (f *firstFailure) set(err error) {
	if err != nil {
		f.once.Do(func() { f.err = err })
	}
}

func (f *firstFailure) get() error { return f.err }
