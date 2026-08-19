package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
	enginemetrics "github.com/uchebnick/fusedb/internal/metrics"
	"github.com/uchebnick/fusedb/internal/scheduler"
)

// DictionaryTrainingConfig bounds adaptive runtime dictionary work. Zero
// values select conservative defaults; Disabled opts out completely.
type DictionaryTrainingConfig struct {
	Disabled                 bool
	GroupLeaves              int
	DictionarySize           int
	ChunkBytes               int
	MaxChunks                int
	MinimumTrainingSamples   int
	MinimumEvaluationSamples int
	MaxSamplesPerGroup       int
	MaxSampleBytesPerGroup   int64
	MaxTotalSampleBytes      int64
	MinimumGain              float64
}

type normalizedDictionaryTraining struct {
	DictionaryTrainingConfig
}

func normalizeDictionaryTraining(cfg DictionaryTrainingConfig) normalizedDictionaryTraining {
	if cfg.GroupLeaves <= 0 {
		cfg.GroupLeaves = 8
	}
	if cfg.DictionarySize <= 0 {
		cfg.DictionarySize = compression.DefaultDictionarySize
	}
	if cfg.ChunkBytes <= 0 {
		cfg.ChunkBytes = 64
	}
	if cfg.MaxChunks <= 0 {
		cfg.MaxChunks = 8 << 10
	}
	if cfg.MinimumTrainingSamples <= 0 {
		cfg.MinimumTrainingSamples = 32
	}
	if cfg.MinimumEvaluationSamples <= 0 {
		cfg.MinimumEvaluationSamples = 8
	}
	if cfg.MaxSamplesPerGroup <= 0 {
		cfg.MaxSamplesPerGroup = 128
	}
	if cfg.MaxSampleBytesPerGroup <= 0 {
		cfg.MaxSampleBytesPerGroup = 4 << 20
	}
	if cfg.MaxTotalSampleBytes <= 0 {
		cfg.MaxTotalSampleBytes = 32 << 20
	}
	if cfg.MinimumGain == 0 {
		cfg.MinimumGain = 0.03
	}
	return normalizedDictionaryTraining{DictionaryTrainingConfig: cfg}
}

func (c normalizedDictionaryTraining) validate() error {
	if c.MinimumGain < 0 || c.MinimumGain >= 1 {
		return compression.ErrInvalidPromotionGain
	}
	if c.DictionarySize < compression.MinDictionarySize || c.ChunkBytes < compression.MinDictionarySize || c.ChunkBytes > c.DictionarySize {
		return errors.New("fusedb: invalid adaptive dictionary or chunk size")
	}
	if c.MinimumTrainingSamples+c.MinimumEvaluationSamples > c.MaxSamplesPerGroup {
		return errors.New("fusedb: dictionary sample minimums exceed per-group limit")
	}
	if c.MaxSampleBytesPerGroup > c.MaxTotalSampleBytes {
		return errors.New("fusedb: dictionary per-group sample bytes exceed total limit")
	}
	return nil
}

type dictionaryTrainingGroup struct {
	training   [][]byte
	evaluation [][]byte
	bytes      int64
	seen       uint64
	trainRun   bool
	evalRun    bool
	candidate  *compression.Dictionary
}

type dictionaryTrainer struct {
	mu               sync.Mutex
	cfg              normalizedDictionaryTraining
	catalog          *compression.DictionaryGroupCatalog
	registry         *compression.Registry
	scheduler        *scheduler.Scheduler
	fatal            func(error)
	lifecycleChanged func()
	groups           map[uint64]*dictionaryTrainingGroup
	totalBytes       int64
	closed           bool
}

func newDictionaryTrainer(
	cfg DictionaryTrainingConfig,
	catalog *compression.DictionaryGroupCatalog,
	registry *compression.Registry,
	fatal func(error),
	lifecycleChanged func(),
) *dictionaryTrainer {
	return &dictionaryTrainer{
		cfg:              normalizeDictionaryTraining(cfg),
		catalog:          catalog,
		registry:         registry,
		fatal:            fatal,
		lifecycleChanged: lifecycleChanged,
		groups:           make(map[uint64]*dictionaryTrainingGroup),
	}
}

func (t *dictionaryTrainer) attach(background *scheduler.Scheduler) {
	if t == nil || t.cfg.Disabled {
		return
	}
	t.mu.Lock()
	t.scheduler = background
	ready := make([]uint64, 0)
	for id, group := range t.groups {
		if t.readyToTrain(group) {
			ready = append(ready, id)
		}
	}
	t.mu.Unlock()
	for _, id := range ready {
		t.scheduleTrain(id)
	}
}

func (t *dictionaryTrainer) observe(groupID uint64, raw []byte) {
	if t == nil || t.cfg.Disabled || groupID == 0 || len(raw) == 0 {
		return
	}
	if int64(len(raw)) > t.cfg.MaxSampleBytesPerGroup {
		return
	}
	copy := append([]byte(nil), raw...)
	t.mu.Lock()
	if t.closed || t.totalBytes+int64(len(copy)) > t.cfg.MaxTotalSampleBytes {
		t.mu.Unlock()
		return
	}
	group := t.groups[groupID]
	if group == nil {
		group = &dictionaryTrainingGroup{}
		t.groups[groupID] = group
	}
	if len(group.training)+len(group.evaluation) >= t.cfg.MaxSamplesPerGroup ||
		group.bytes+int64(len(copy)) > t.cfg.MaxSampleBytesPerGroup {
		t.mu.Unlock()
		return
	}
	group.seen++
	if group.seen%5 == 0 {
		group.evaluation = append(group.evaluation, copy)
	} else {
		group.training = append(group.training, copy)
	}
	group.bytes += int64(len(copy))
	t.totalBytes += int64(len(copy))
	ready := t.readyToTrain(group) && t.scheduler != nil
	t.mu.Unlock()
	if ready {
		t.scheduleTrain(groupID)
	}
}

func (t *dictionaryTrainer) readyToTrain(group *dictionaryTrainingGroup) bool {
	return group != nil && !group.trainRun && !group.evalRun && group.candidate == nil &&
		len(group.training) >= t.cfg.MinimumTrainingSamples &&
		len(group.evaluation) >= t.cfg.MinimumEvaluationSamples
}

func (t *dictionaryTrainer) scheduleTrain(groupID uint64) {
	t.mu.Lock()
	group := t.groups[groupID]
	background := t.scheduler
	if t.closed || background == nil || !t.readyToTrain(group) {
		t.mu.Unlock()
		return
	}
	group.trainRun = true
	t.mu.Unlock()

	job := scheduler.Job{
		ID:          fmt.Sprintf("dictionary-train:%d", groupID),
		Class:       scheduler.ClassDictionaryTrain,
		MetricsKind: enginemetrics.BackgroundDictionaryTrain,
		Priority:    10,
		Cost: scheduler.Cost{
			CPUFraction:    0.70,
			DiskFraction:   0.05,
			MemoryFraction: 0.15,
			Interference:   0.60,
		},
		Preemptible: true,
		Run:         func(ctx context.Context) error { return t.train(ctx, groupID) },
		OnDone: func(err error) {
			t.mu.Lock()
			if group := t.groups[groupID]; group != nil {
				group.trainRun = false
			}
			closed := t.closed
			t.mu.Unlock()
			if !closed && errors.Is(err, context.Canceled) {
				t.scheduleTrain(groupID)
			}
		},
	}
	if !background.Submit(job) {
		t.mu.Lock()
		group.trainRun = false
		t.mu.Unlock()
	}
}

func (t *dictionaryTrainer) train(ctx context.Context, groupID uint64) error {
	t.mu.Lock()
	group := t.groups[groupID]
	if group == nil {
		t.mu.Unlock()
		return compression.ErrNoSamples
	}
	samples := append([][]byte(nil), group.training...)
	t.mu.Unlock()
	id, err := t.catalog.ReserveDictionaryID()
	if err != nil {
		t.handleFatal(err)
		return err
	}
	candidate, err := compression.TrainDictionaryCooperative(ctx, compression.AdaptiveTrainOptions{
		ID:         id,
		Size:       t.cfg.DictionarySize,
		ChunkBytes: t.cfg.ChunkBytes,
		MaxChunks:  t.cfg.MaxChunks,
		Samples:    samples,
	})
	if err != nil {
		return err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = candidate.Close()
		return context.Canceled
	}
	group = t.groups[groupID]
	group.candidate = candidate
	group.evalRun = true
	t.mu.Unlock()
	t.scheduleEvaluation(groupID)
	return nil
}

func (t *dictionaryTrainer) scheduleEvaluation(groupID uint64) {
	t.mu.Lock()
	background := t.scheduler
	group := t.groups[groupID]
	if t.closed || background == nil || group == nil || group.candidate == nil || !group.evalRun {
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	job := scheduler.Job{
		ID:          fmt.Sprintf("dictionary-evaluate:%d", groupID),
		Class:       scheduler.ClassDictionaryEvaluate,
		MetricsKind: enginemetrics.BackgroundDictionaryEvaluate,
		Priority:    9,
		Cost:        scheduler.Cost{CPUFraction: 0.45, MemoryFraction: 0.05, Interference: 0.35},
		Preemptible: true,
		Run:         func(ctx context.Context) error { return t.evaluate(ctx, groupID) },
		OnDone: func(err error) {
			t.mu.Lock()
			if group := t.groups[groupID]; group != nil {
				group.evalRun = false
			}
			closed := t.closed
			t.mu.Unlock()
			if !closed && errors.Is(err, context.Canceled) {
				t.mu.Lock()
				if group := t.groups[groupID]; group != nil {
					group.evalRun = true
				}
				t.mu.Unlock()
				t.scheduleEvaluation(groupID)
			} else if err != nil {
				t.finishCandidate(groupID, false)
			}
		},
	}
	if !background.Submit(job) {
		t.mu.Lock()
		group.evalRun = false
		t.mu.Unlock()
	}
}

func (t *dictionaryTrainer) evaluate(ctx context.Context, groupID uint64) error {
	t.mu.Lock()
	group := t.groups[groupID]
	if group == nil || group.candidate == nil {
		t.mu.Unlock()
		return compression.ErrNoSamples
	}
	candidate := group.candidate
	samples := append([][]byte(nil), group.evaluation...)
	t.mu.Unlock()
	var current *compression.Dictionary
	if currentID, ok := t.catalog.ActiveDictionaryID(groupID); ok {
		loaded, err := t.registry.Load(currentID)
		if err != nil {
			return err
		}
		current = loaded
	}
	evaluation, err := compression.EvaluateDictionaryCandidate(ctx, candidate, current, samples)
	if err != nil {
		return err
	}
	promote, err := evaluation.ShouldPromote(t.cfg.MinimumGain)
	if err != nil {
		return err
	}
	if promote {
		if err := t.registry.Save(candidate); err != nil {
			t.handleFatal(err)
			return err
		}
		if _, err := t.catalog.Publish(groupID, candidate.ID()); err != nil {
			t.handleFatal(err)
			t.finishCandidate(groupID, true)
			return err
		}
		if t.lifecycleChanged != nil {
			t.lifecycleChanged()
		}
	}
	t.finishCandidate(groupID, promote)
	return nil
}

func (t *dictionaryTrainer) finishCandidate(groupID uint64, published bool) {
	t.mu.Lock()
	group := t.groups[groupID]
	if group == nil {
		t.mu.Unlock()
		return
	}
	if !published && group.candidate != nil {
		_ = group.candidate.Close()
	}
	t.totalBytes -= group.bytes
	group.training = nil
	group.evaluation = nil
	group.bytes = 0
	group.candidate = nil
	t.mu.Unlock()
}

func (t *dictionaryTrainer) handleFatal(err error) {
	if t != nil && t.fatal != nil && errors.Is(err, disk.ErrCommitUncertain) {
		t.fatal(err)
	}
}

func (t *dictionaryTrainer) close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.closed = true
	for _, group := range t.groups {
		if group.candidate != nil {
			_ = group.candidate.Close()
			group.candidate = nil
		}
	}
	t.mu.Unlock()
}
