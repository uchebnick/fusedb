// Package scheduler arbitrates background storage work against foreground
// latency. It owns admission, prioritization, and cancellation policy; jobs own
// their storage-specific correctness and commit protocols.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
)

// State is the scheduler's current view of foreground load.
type State uint8

const (
	StateNormal State = iota
	StateQuietCandidate
	StateQuiet
	StateRecovering
	StateBusy
	StateOverloaded
)

func (s State) String() string {
	switch s {
	case StateNormal:
		return "normal"
	case StateQuietCandidate:
		return "quiet-candidate"
	case StateQuiet:
		return "quiet"
	case StateRecovering:
		return "recovering"
	case StateBusy:
		return "busy"
	case StateOverloaded:
		return "overloaded"
	default:
		return "unknown"
	}
}

// Class defines how aggressively a job may consume foreground headroom.
type Class uint8

const (
	ClassMaintenance Class = iota
	ClassDictionaryTrain
	ClassDictionaryEvaluate
	ClassSchedulerModelPersist
)

// Cost is a scheduler estimate, not an accounting guarantee. Executors should
// update future estimates from observed work.
type Cost struct {
	CPUFraction    float64
	DiskFraction   float64
	MemoryFraction float64
	ReadBytes      int64
	WriteBytes     int64
	MemoryBytes    int64
	Interference   float64
}

// Job is one deduplicated unit of background work.
//
// A preemptible Run function must observe ctx and return promptly. Returning
// context.Canceled confirms that compute actually stopped; merely abandoning a
// result while work continues is not considered preemption.
type Job struct {
	ID          string
	Class       Class
	MetricsKind enginemetrics.BackgroundKind
	Priority    float64
	Cost        Cost
	Preemptible bool
	// Context cancels queued or running work at the caller's request. A nil
	// context means the job is owned entirely by the scheduler lifecycle.
	Context context.Context

	// Urgency is evaluated at admission time so queued maintenance debt can grow.
	Urgency func() float64
	// Mandatory bypasses foreground admission when correctness or a hard resource
	// limit requires progress.
	Mandatory func() bool
	// Needed is checked before execution and after completion. A still-needed job
	// is requeued without allowing duplicate IDs.
	Needed func() bool
	Run    func(context.Context) error
	OnDone func(error)
}

// ErrUnavailable reports that a job could not be admitted because the
// scheduler is closed or another job already uses its identifier.
var ErrUnavailable = errors.New("scheduler: job unavailable")

// Config controls observation and adaptive load classification.
type Config struct {
	PollInterval      time.Duration
	ObservationWindow time.Duration
	QuietConfirm      time.Duration
	RecoveryPeriod    time.Duration

	TargetReadP99  time.Duration
	TargetWriteP99 time.Duration

	QuietRateCeiling     float64
	QuietRateRatio       float64
	BusyRateRatio        float64
	QuietHeadroom        float64
	QuietMaxRateCV       float64
	OverloadRatio        float64
	MaxCPUUtilization    float64
	MaxDiskUtilization   float64
	MaxMemoryUtilization float64

	// InitialModel restores learned workload history. ModelChanged is invoked
	// after a completed UTC 15-minute observation changes durable model state;
	// callbacks must return quickly and must not perform filesystem IO.
	InitialModel *ModelSnapshot
	ModelChanged func(generation uint64)
}

// DefaultConfig is deliberately conservative: training needs a confirmed quiet
// period, while latency pressure revokes its budget immediately.
func DefaultConfig() Config {
	return Config{
		PollInterval:         250 * time.Millisecond,
		ObservationWindow:    10 * time.Second,
		QuietConfirm:         30 * time.Second,
		RecoveryPeriod:       5 * time.Second,
		TargetReadP99:        5 * time.Millisecond,
		TargetWriteP99:       5 * time.Millisecond,
		QuietRateCeiling:     100,
		QuietRateRatio:       0.35,
		BusyRateRatio:        1.25,
		QuietHeadroom:        0.60,
		QuietMaxRateCV:       0.30,
		OverloadRatio:        1.25,
		MaxCPUUtilization:    1.0,
		MaxDiskUtilization:   1.0,
		MaxMemoryUtilization: 1.0,
	}
}

// Load is a rolling foreground observation.
type Load struct {
	Window time.Duration

	ReadRate    float64
	WriteRate   float64
	RequestRate float64
	// ExpectedRequestRate is the learned time-of-week baseline used for spike
	// detection. It is not a fixed day/night schedule.
	ExpectedRequestRate float64
	RateCV              float64

	ReadP95  time.Duration
	ReadP99  time.Duration
	WriteP95 time.Duration
	WriteP99 time.Duration

	ReadLatencySamples  uint64
	WriteLatencySamples uint64

	CPUUtilization    float64
	DiskUtilization   float64
	MemoryUtilization float64
}

// RuntimeStats is a lock-bounded snapshot of scheduler lifecycle state. It is
// intended for monitoring and never exposes job IDs, which may contain
// caller-controlled or otherwise high-cardinality values.
type RuntimeStats struct {
	Queued  int
	Running bool
	Closed  bool
}

// Telemetry is intentionally compatible with a future Prometheus adapter: the
// scheduler only consumes cumulative snapshots and records job lifecycle data.
type Telemetry interface {
	Snapshot() enginemetrics.Snapshot
	BackgroundStarted(enginemetrics.BackgroundKind) time.Time
	BackgroundFinished(enginemetrics.BackgroundKind, time.Time, bool, bool)
}

// Scheduler serializes high-interference background work and preempts optional
// jobs as soon as foreground pressure returns.
type Scheduler struct {
	cfg       Config
	telemetry Telemetry
	policy    *adaptivePolicy
	policyMu  sync.RWMutex

	mu        sync.Mutex
	queue     map[string]Job
	runningID string
	closed    bool

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	state atomic.Uint32
	load  atomic.Pointer[Load]
}

type runningJob struct {
	job    Job
	cancel context.CancelFunc
	done   chan error
	start  time.Time
}

// New starts a scheduler. telemetry must not be nil.
func New(cfg Config, telemetry Telemetry) (*Scheduler, error) {
	if telemetry == nil {
		return nil, errors.New("scheduler: nil telemetry")
	}
	cfg = normalizeConfig(cfg)
	policy := newAdaptivePolicy(cfg)
	if cfg.InitialModel != nil {
		if err := policy.restoreModel(*cfg.InitialModel); err != nil {
			return nil, fmt.Errorf("scheduler: restore model: %w", err)
		}
	}
	s := &Scheduler{
		cfg:       cfg,
		telemetry: telemetry,
		policy:    policy,
		queue:     make(map[string]Job),
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	s.state.Store(uint32(StateNormal))
	initialLoad := &Load{}
	s.load.Store(initialLoad)
	go s.loop()
	return s, nil
}

// Submit queues job unless the same ID is already queued or running.
func (s *Scheduler) Submit(job Job) bool {
	if s == nil || job.ID == "" || job.Run == nil {
		return false
	}
	s.mu.Lock()
	if s.closed || s.runningID == job.ID {
		s.mu.Unlock()
		return false
	}
	if _, exists := s.queue[job.ID]; exists {
		s.mu.Unlock()
		return false
	}
	s.queue[job.ID] = job
	s.mu.Unlock()
	s.notify()
	return true
}

// Execute submits one job and waits for its terminal result. Cancellation of
// ctx also cancels the queued or running executor; callers should use a unique
// job ID when concurrent executions are possible.
func (s *Scheduler) Execute(ctx context.Context, job Job) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	done := make(chan error, 1)
	originalDone := job.OnDone
	job.Context = ctx
	job.OnDone = func(err error) {
		if originalDone != nil {
			originalDone(err)
		}
		done <- err
	}
	if !s.Submit(job) {
		return ErrUnavailable
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		s.notify()
		return ctx.Err()
	}
}

// State returns the current adaptive load state.
func (s *Scheduler) State() State {
	if s == nil {
		return StateNormal
	}
	return State(s.state.Load())
}

// Load returns the latest immutable rolling observation.
func (s *Scheduler) Load() Load {
	if s == nil {
		return Load{}
	}
	load := s.load.Load()
	if load == nil {
		return Load{}
	}
	return *load
}

// RuntimeStats returns queue and executor state without invoking job callbacks
// or evaluating admission policy.
func (s *Scheduler) RuntimeStats() RuntimeStats {
	if s == nil {
		return RuntimeStats{Closed: true}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return RuntimeStats{
		Queued:  len(s.queue),
		Running: s.runningID != "",
		Closed:  s.closed,
	}
}

// ModelSnapshot returns a detached learned workload model. Admission state and
// the incomplete current interval are intentionally excluded.
func (s *Scheduler) ModelSnapshot() ModelSnapshot {
	if s == nil {
		return ModelSnapshot{}
	}
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()
	return s.policy.snapshotModel()
}

// Close stops admission, cancels the running job, and waits for its executor to
// return. Correctness-critical jobs may defer cancellation until their atomic
// commit phase completes.
func (s *Scheduler) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.done
		return nil
	}
	s.closed = true
	close(s.stop)
	s.mu.Unlock()
	<-s.done
	return nil
}

func (s *Scheduler) loop() {
	defer close(s.done)
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	previous := s.telemetry.Snapshot()
	window := newRollingWindow(s.cfg.ObservationWindow)
	var running *runningJob

	for {
		admit := false
		var completed <-chan error
		if running != nil {
			completed = running.done
		}
		select {
		case <-s.stop:
			if running != nil {
				running.cancel()
				err := <-running.done
				s.finish(running, err)
			}
			s.cancelQueued()
			return
		case err := <-completed:
			s.finish(running, err)
			running = nil
			admit = true
		case <-ticker.C:
			current := s.telemetry.Snapshot()
			window.add(previous, current)
			previous = current
			load := window.load()
			s.policyMu.Lock()
			state := s.policy.observe(time.Now(), load, running != nil)
			load.ExpectedRequestRate = s.policy.expectedRate
			s.policyMu.Unlock()
			s.load.Store(&load)
			s.state.Store(uint32(state))
			if running != nil && running.job.Preemptible && !classAllowed(running.job.Class, state) {
				running.cancel()
			}
			admit = true
		case <-s.wake:
			// A hard maintenance limit outranks optional compute even if foreground
			// load remains quiet. Optional work waits for the next fresh observation
			// instead of being admitted from a stale state.
			mandatory := s.hasMandatoryQueued()
			if mandatory && running != nil && running.job.Preemptible {
				running.cancel()
			}
			admit = mandatory
		}

		if admit && running == nil {
			running = s.startNext()
		}
	}
}

func (s *Scheduler) hasMandatoryQueued() bool {
	s.mu.Lock()
	queued := make([]Job, 0, len(s.queue))
	for _, job := range s.queue {
		queued = append(queued, job)
	}
	s.mu.Unlock()
	for _, job := range queued {
		if jobNeeded(job) && job.Mandatory != nil && job.Mandatory() {
			return true
		}
	}
	return false
}

func (s *Scheduler) cancelQueued() {
	s.mu.Lock()
	queued := make([]Job, 0, len(s.queue))
	for id, job := range s.queue {
		queued = append(queued, job)
		delete(s.queue, id)
	}
	s.mu.Unlock()

	for _, job := range queued {
		if job.OnDone != nil {
			job.OnDone(context.Canceled)
		}
	}
}

func (s *Scheduler) startNext() *runningJob {
	state := s.State()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	queued := make([]Job, 0, len(s.queue))
	for _, job := range s.queue {
		queued = append(queued, job)
	}
	s.mu.Unlock()

	var selected Job
	selectedScore := math.Inf(-1)
	var unnecessary []Job
	for _, job := range queued {
		if !jobNeeded(job) {
			unnecessary = append(unnecessary, job)
			continue
		}
		mandatory := job.Mandatory != nil && job.Mandatory()
		if !mandatory && !classAllowed(job.Class, state) {
			continue
		}
		if !mandatory && !resourcesAllow(job.Cost, s.Load(), s.cfg) {
			continue
		}
		score := job.Priority
		if job.Urgency != nil {
			score += job.Urgency()
		}
		if mandatory {
			score += 1e9
		}
		if score > selectedScore {
			selected = job
			selectedScore = score
		}
	}

	s.mu.Lock()
	removed := unnecessary[:0]
	for _, job := range unnecessary {
		if _, exists := s.queue[job.ID]; exists {
			delete(s.queue, job.ID)
			removed = append(removed, job)
		}
	}
	if selected.ID == "" {
		s.mu.Unlock()
		s.requeueIfNeeded(removed)
		return nil
	}
	if s.closed || s.runningID != "" {
		s.mu.Unlock()
		s.requeueIfNeeded(removed)
		return nil
	}
	if _, exists := s.queue[selected.ID]; !exists {
		s.mu.Unlock()
		s.requeueIfNeeded(removed)
		return nil
	}
	delete(s.queue, selected.ID)
	s.runningID = selected.ID

	parent := jobContext(selected)
	ctx, cancel := context.WithCancel(parent)
	running := &runningJob{
		job:    selected,
		cancel: cancel,
		done:   make(chan error, 1),
		start:  s.telemetry.BackgroundStarted(selected.MetricsKind),
	}
	go func() {
		running.done <- selected.Run(ctx)
	}()
	s.mu.Unlock()
	s.requeueIfNeeded(removed)
	return running
}

func (s *Scheduler) requeueIfNeeded(jobs []Job) {
	for _, job := range jobs {
		if job.Needed != nil && jobNeeded(job) {
			s.Submit(job)
		}
	}
}

func (s *Scheduler) finish(running *runningJob, err error) {
	if running == nil {
		return
	}
	cancelled := errors.Is(err, context.Canceled)
	s.telemetry.BackgroundFinished(running.job.MetricsKind, running.start, err != nil && !cancelled, cancelled)

	s.mu.Lock()
	s.runningID = ""
	closed := s.closed
	s.mu.Unlock()

	if running.job.OnDone != nil {
		running.job.OnDone(err)
	}
	if !closed && running.job.Needed != nil && jobNeeded(running.job) {
		s.Submit(running.job)
	}
}

func jobContext(job Job) context.Context {
	if job.Context != nil {
		return job.Context
	}
	return context.Background()
}

func jobNeeded(job Job) bool {
	if job.Context != nil && job.Context.Err() != nil {
		return false
	}
	return job.Needed == nil || job.Needed()
}

func (s *Scheduler) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
