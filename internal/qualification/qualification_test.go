package qualification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
)

func TestLatencyHistogramIsBoundedAndMergeable(t *testing.T) {
	var histogram latencyHistogram
	for index := 1; index <= 100; index++ {
		histogram.observe(time.Duration(index) * time.Microsecond)
	}
	histogram.observe(time.Minute)
	snapshot := histogram.snapshot()
	if snapshot.Count != 101 || snapshot.Overflow != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if got := snapshot.quantile(0.95); got < 90*time.Microsecond || got > 110*time.Microsecond {
		t.Fatalf("p95 = %s", got)
	}
	if got := snapshot.quantile(1); got != time.Minute {
		t.Fatalf("p100 = %s, want 1m", got)
	}

	var merged histogramSnapshot
	merged.add(snapshot)
	merged.add(snapshot)
	if merged.Count != 202 || merged.Overflow != 2 || merged.Maximum != int64(time.Minute) {
		t.Fatalf("merged = %+v", merged)
	}
}

func TestQualificationValueIntegrity(t *testing.T) {
	value := buildValue(17, 42, 128)
	if err := validateValue(value, 17); err != nil {
		t.Fatal(err)
	}
	if err := validateValue(value, 18); !errors.Is(err, errValueKey) {
		t.Fatalf("wrong-key error = %v", err)
	}
	value[len(value)-1] ^= 0xff
	if err := validateValue(value, 17); !errors.Is(err, errValueChecksum) {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestPacerRateChangeWakesOldReservations(t *testing.T) {
	pacer := newPacer()
	pacer.setRate(1)
	if !pacer.wait(context.Background()) {
		t.Fatal("initial pacing slot was not granted")
	}
	done := make(chan bool, 1)
	go func() { done <- pacer.wait(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	pacer.setRate(1_000)
	select {
	case granted := <-done:
		if !granted {
			t.Fatal("pacer stopped while context remained live")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("rate change did not wake old one-second reservation")
	}
	pacer.pause()
}

func TestGatesUseOnlyOperationsActiveInPhase(t *testing.T) {
	report := Report{
		Overall: OperationReport{Total: 100},
		Phases: []PhaseReport{{
			Name: "recovery", Mix: Mix{Read: 100},
			Operations: OperationReport{Read: LatencyReport{Count: 100, P99Nanos: int64(time.Millisecond)}},
		}},
		Background: []BackgroundDelta{{Kind: "merge", Completed: 1}},
		Peak:       DebtReport{BufferedBytes: 100},
		Final:      DebtReport{BufferedBytes: 10},
		Verification: VerificationReport{
			PreCloseValues: true, PreCloseVerify: true, PreCloseCounters: true, Reopen: true,
			PostReopenValues: true, PostReopenVerify: true, PostReopenCounters: true,
		},
	}
	gates := Gates{
		MinOperations: 100, MinSamplesPerOperation: 50, MaxErrorRate: 0,
		MaxReadP99: 2 * time.Millisecond, MaxWriteP99: time.Nanosecond,
		MaxIncrementP99: time.Nanosecond, RequireMaintenanceProgress: true,
		RequireDebtExercise: true, RequireDebtDrain: true,
	}
	applyGates(gates, &report)
	if len(report.Failures) != 0 {
		t.Fatalf("valid report failures = %v", report.Failures)
	}
	report.Phases[0].Operations.Read.P99Nanos = int64(3 * time.Millisecond)
	applyGates(gates, &report)
	if len(report.Failures) != 1 {
		t.Fatalf("high-p99 failures = %v", report.Failures)
	}
}

func TestQualificationRunVerifiesMaintenanceAndReopen(t *testing.T) {
	cfg := Config{
		Dir:            t.TempDir(),
		Workers:        4,
		KeyCount:       256,
		CounterCount:   16,
		ValueBytes:     128,
		Seed:           99,
		SampleInterval: 10 * time.Millisecond,
		VerifyTimeout:  10 * time.Second,
		Mix:            Mix{Read: 70, Write: 25, Increment: 5},
		Phases: []Phase{
			{Name: "quiet", Class: PhaseQuiet, Duration: 250 * time.Millisecond, TargetOpsPerSecond: 200, Mix: &Mix{Read: 90, Write: 10}},
			{Name: "steady", Class: PhaseSteady, Duration: 400 * time.Millisecond, TargetOpsPerSecond: 800},
			{Name: "spike", Class: PhaseSpike, Duration: 400 * time.Millisecond, TargetOpsPerSecond: 2_000},
			{Name: "recovery", Class: PhaseRecovery, Duration: 750 * time.Millisecond, TargetOpsPerSecond: 300, Mix: &Mix{Read: 100}},
		},
		Gates: Gates{
			MinOperations: 500, MinSamplesPerOperation: 5, MaxErrorRate: 0,
			MaxReadP99: time.Second, MaxWriteP99: time.Second, MaxIncrementP99: time.Second,
			MaxFinalBufferedBytes: 64 << 10, MaxFinalWALBytes: 64 << 10,
			RequireMaintenanceProgress: true, RequireDebtExercise: true, RequireDebtDrain: true,
		},
	}
	cfg.Database.MergeSize = 2 << 10
	cfg.Database.MaxLeafSize = 32 << 10
	cfg.Database.WALCheckpointBytes = 16 << 10
	cfg.Database.WALGroupCommitInterval = time.Millisecond
	cfg.Database.DictionaryTraining.Disabled = true
	cfg.Database.Metrics.LatencySampleEvery = 1
	cfg.Database.Scheduler.PollInterval = 10 * time.Millisecond
	cfg.Database.Scheduler.ObservationWindow = 50 * time.Millisecond
	cfg.Database.Scheduler.QuietConfirm = 20 * time.Millisecond
	cfg.Database.Scheduler.RecoveryPeriod = 20 * time.Millisecond
	cfg.Database.Scheduler.TargetReadP99 = 500 * time.Millisecond
	cfg.Database.Scheduler.TargetWriteP99 = 500 * time.Millisecond
	cfg.Database.Scheduler.MaxCPUUtilization = 1
	cfg.Database.Scheduler.MaxDiskUtilization = 1
	cfg.Database.Scheduler.MaxMemoryUtilization = 1

	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("run: %v; failures=%v", err, report.Failures)
	}
	if !report.Passed {
		t.Fatalf("qualification failed: %v; peak=%+v final=%+v background=%+v", report.Failures, report.Peak, report.Final, report.Background)
	}
	if len(report.Phases) != len(cfg.Phases) || report.Overall.Total < cfg.Gates.MinOperations {
		t.Fatalf("incomplete report: %+v", report)
	}
	if report.Verification.ExpectedIncrements == 0 || report.Verification.ExpectedIncrements != report.Verification.ObservedIncrements {
		t.Fatalf("counter verification = %+v", report.Verification)
	}
	if !report.Verification.PreCloseVerify || !report.Verification.PostReopenVerify {
		t.Fatalf("verify cycle = %+v", report.Verification)
	}

	if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrDirectoryNotEmpty) {
		t.Fatalf("reuse without opt-in error = %v", err)
	}
}

func TestQualificationRejectsNonEmptyDirectoryBeforeOpen(t *testing.T) {
	dir := t.TempDir()
	file, err := disk.DefaultFS.Create(dir + "/do-not-touch")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(dir)
	if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrDirectoryNotEmpty) {
		t.Fatalf("non-empty directory error = %v", err)
	}
}
