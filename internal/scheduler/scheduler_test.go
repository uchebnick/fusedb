package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
)

func TestAdaptivePolicyConfirmsQuietAndReactsImmediatelyToSpike(t *testing.T) {
	cfg := DefaultConfig()
	cfg.QuietConfirm = 10 * time.Second
	cfg.RecoveryPeriod = 5 * time.Second
	p := newAdaptivePolicy(cfg)
	now := time.Unix(100, 0)

	quiet := Load{RequestRate: 10, RateCV: 0.1, ReadP99: time.Millisecond}
	if got := p.observe(now, quiet, false); got != StateQuietCandidate {
		t.Fatalf("first quiet state = %s", got)
	}
	if got := p.observe(now.Add(11*time.Second), quiet, false); got != StateQuiet {
		t.Fatalf("confirmed quiet state = %s", got)
	}

	spike := Load{RequestRate: 10, ReadP99: 10 * time.Millisecond}
	if got := p.observe(now.Add(12*time.Second), spike, true); got != StateOverloaded {
		t.Fatalf("spike state = %s", got)
	}
	if got := p.observe(now.Add(13*time.Second), quiet, false); got != StateRecovering {
		t.Fatalf("post-spike state = %s", got)
	}
}

func TestExecuteWaitsForResultAndPropagatesCancellation(t *testing.T) {
	telemetry := enginemetrics.NewRecorder(enginemetrics.Config{})
	cfg := DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	s, err := New(cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Execute(ctx, Job{
		ID:          "execute-success",
		Class:       ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundVerify,
		Mandatory:   func() bool { return true },
		Run:         func(context.Context) error { return nil },
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	cancelCtx, cancelRun := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- s.Execute(cancelCtx, Job{
			ID:          "execute-cancel",
			Class:       ClassMaintenance,
			MetricsKind: enginemetrics.BackgroundVerify,
			Mandatory:   func() bool { return true },
			Preemptible: true,
			Run: func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("execute job did not start")
	}
	cancelRun()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("execute cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("execute did not return after cancellation")
	}
}

func TestSeasonalModelCountsCompletedIntervalsNotPolls(t *testing.T) {
	p := newAdaptivePolicy(DefaultConfig())
	monday := time.Date(2026, time.August, 10, 2, 0, 0, 0, time.UTC)

	for i := range 100 {
		p.updateSeasonal(monday.Add(time.Duration(i)*time.Second), 10)
	}
	if point := p.seasonal[seasonalIndex(monday)]; point.samples != 0 {
		t.Fatalf("samples inside one interval = %d", point.samples)
	}

	p.updateSeasonal(monday.Add(15*time.Minute), 20)
	point := p.seasonal[seasonalIndex(monday)]
	if point.samples != 1 || point.rate != 10 {
		t.Fatalf("first completed interval = %+v", point)
	}
	if _, ready := p.seasonalExpected(monday); ready {
		t.Fatal("one weekly observation unexpectedly made the seasonal model ready")
	}

	nextMonday := monday.Add(7 * 24 * time.Hour)
	p.updateSeasonal(nextMonday, 30)
	p.updateSeasonal(nextMonday.Add(15*time.Minute), 40)
	if _, ready := p.seasonalExpected(nextMonday); !ready {
		t.Fatal("two completed weekly observations did not make the seasonal model ready")
	}
}

func TestAdaptivePolicyModelSnapshotRestoreAndUTCIndex(t *testing.T) {
	changed := make(chan uint64, 1)
	cfg := DefaultConfig()
	cfg.ModelChanged = func(generation uint64) { changed <- generation }
	p := newAdaptivePolicy(cfg)
	start := time.Date(2026, time.August, 10, 2, 0, 0, 0, time.FixedZone("west", -7*60*60))
	p.observe(start, Load{RequestRate: 40}, false)
	p.observe(start.Add(15*time.Minute), Load{RequestRate: 60}, false)
	select {
	case generation := <-changed:
		if generation != 1 {
			t.Fatalf("changed generation = %d, want 1", generation)
		}
	default:
		t.Fatal("completed seasonal interval did not mark model changed")
	}
	snapshot := p.snapshotModel()
	restored := newAdaptivePolicy(DefaultConfig())
	if err := restored.restoreModel(snapshot); err != nil {
		t.Fatal(err)
	}
	if restored.snapshotModel() != snapshot {
		t.Fatal("restored model differs from snapshot")
	}
	if seasonalIndex(start) != seasonalIndex(start.UTC()) {
		t.Fatal("seasonal index depends on local timezone")
	}
}

func TestSchedulerPreemptsTrainingWhenLatencySpikes(t *testing.T) {
	telemetry := enginemetrics.NewRecorder(enginemetrics.Config{LatencySampleEvery: 1})
	cfg := DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ObservationWindow = 20 * time.Millisecond
	cfg.QuietConfirm = 15 * time.Millisecond
	cfg.RecoveryPeriod = 10 * time.Millisecond
	cfg.TargetReadP99 = 500 * time.Microsecond
	cfg.TargetWriteP99 = 500 * time.Microsecond
	s, err := New(cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	started := make(chan struct{})
	finished := make(chan error, 1)
	if !s.Submit(Job{
		ID:          "train:group-1",
		Class:       ClassDictionaryTrain,
		MetricsKind: enginemetrics.BackgroundDictionaryTrain,
		Preemptible: true,
		Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
		OnDone: func(err error) { finished <- err },
	}) {
		t.Fatal("training job was not submitted")
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("training did not start in quiet window")
	}

	readStart := telemetry.BeginRead()
	time.Sleep(3 * time.Millisecond)
	telemetry.EndRead(readStart)

	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("training error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("training was not preempted after latency spike")
	}

	snapshot := telemetry.Snapshot()
	if snapshot.BackgroundCancelled[enginemetrics.BackgroundDictionaryTrain] != 1 {
		t.Fatalf("cancelled training = %d", snapshot.BackgroundCancelled[enginemetrics.BackgroundDictionaryTrain])
	}
}

func TestMandatoryMaintenancePreemptsTraining(t *testing.T) {
	telemetry := enginemetrics.NewRecorder(enginemetrics.Config{})
	cfg := DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.QuietConfirm = 10 * time.Millisecond
	s, err := New(cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	trainingStarted := make(chan struct{})
	trainingDone := make(chan error, 1)
	if !s.Submit(Job{
		ID:          "train",
		Class:       ClassDictionaryTrain,
		MetricsKind: enginemetrics.BackgroundDictionaryTrain,
		Preemptible: true,
		Run: func(ctx context.Context) error {
			close(trainingStarted)
			<-ctx.Done()
			return ctx.Err()
		},
		OnDone: func(err error) { trainingDone <- err },
	}) {
		t.Fatal("training was not submitted")
	}
	select {
	case <-trainingStarted:
	case <-time.After(time.Second):
		t.Fatal("training did not start")
	}

	maintenanceDone := make(chan struct{})
	if !s.Submit(Job{
		ID:          "hard-checkpoint",
		Class:       ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundCheckpoint,
		Mandatory:   func() bool { return true },
		Run: func(context.Context) error {
			close(maintenanceDone)
			return nil
		},
	}) {
		t.Fatal("mandatory maintenance was not submitted")
	}

	select {
	case err := <-trainingDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("training error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("mandatory maintenance did not preempt training")
	}
	select {
	case <-maintenanceDone:
	case <-time.After(time.Second):
		t.Fatal("mandatory maintenance did not run after preemption")
	}
}

func TestCloseCancelsQueuedJob(t *testing.T) {
	telemetry := enginemetrics.NewRecorder(enginemetrics.Config{})
	cfg := DefaultConfig()
	cfg.PollInterval = time.Hour
	s, err := New(cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	if !s.Submit(Job{
		ID:          "queued",
		Class:       ClassDictionaryTrain,
		MetricsKind: enginemetrics.BackgroundDictionaryTrain,
		Run:         func(context.Context) error { return nil },
		OnDone:      func(err error) { done <- err },
	}) {
		t.Fatal("job was not submitted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued job error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued job was abandoned without completion callback")
	}
}

func TestSchedulerDeduplicatesAndRunsMandatoryMaintenanceUnderLoad(t *testing.T) {
	telemetry := enginemetrics.NewRecorder(enginemetrics.Config{LatencySampleEvery: 1})
	cfg := DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.TargetReadP99 = time.Microsecond
	s, err := New(cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	start := telemetry.BeginRead()
	time.Sleep(time.Millisecond)
	telemetry.EndRead(start)
	time.Sleep(2 * cfg.PollInterval)

	var runs atomic.Int64
	done := make(chan struct{})
	job := Job{
		ID:          "merge",
		Class:       ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundMerge,
		Mandatory:   func() bool { return true },
		Run: func(context.Context) error {
			runs.Add(1)
			close(done)
			return nil
		},
	}
	if !s.Submit(job) {
		t.Fatal("mandatory job was not submitted")
	}
	if s.Submit(job) {
		t.Fatal("duplicate job was submitted")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mandatory maintenance did not run")
	}
	if runs.Load() != 1 {
		t.Fatalf("runs = %d", runs.Load())
	}
}

func TestSchedulerWaitsForResourceHeadroom(t *testing.T) {
	telemetry := enginemetrics.NewRecorder(enginemetrics.Config{})
	telemetry.SetResourceUtilization(0.95, 0.10, 0.10)
	cfg := DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ObservationWindow = 15 * time.Millisecond
	s, err := New(cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	time.Sleep(2 * cfg.PollInterval)

	started := make(chan struct{})
	if !s.Submit(Job{
		ID:          "optional-merge",
		Class:       ClassMaintenance,
		MetricsKind: enginemetrics.BackgroundMerge,
		Cost:        Cost{CPUFraction: 0.10},
		Run: func(context.Context) error {
			close(started)
			return nil
		},
	}) {
		t.Fatal("optional merge was not submitted")
	}

	select {
	case <-started:
		t.Fatal("optional merge started without CPU headroom")
	case <-time.After(20 * time.Millisecond):
	}

	telemetry.SetResourceUtilization(0.10, 0.10, 0.10)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("optional merge did not start after CPU recovered")
	}
}

func TestCooperativeStopsAtCancellationBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	steps := 0
	err := Cooperative(ctx, func() (bool, error) {
		steps++
		if steps == 3 {
			cancel()
		}
		return false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cooperative error = %v", err)
	}
	if steps != 3 {
		t.Fatalf("steps = %d, want 3", steps)
	}
}
