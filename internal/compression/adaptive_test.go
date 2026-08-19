package compression

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func adaptiveSamples(count int, suffix string) [][]byte {
	samples := make([][]byte, 0, count)
	for i := range count {
		samples = append(samples, []byte(fmt.Sprintf(
			"tenant=acme|region=eu|kind=session|status=active|user=%08d|%s",
			i, suffix,
		)))
	}
	return samples
}

func TestCooperativeTrainingAndHeldOutEvaluation(t *testing.T) {
	candidate, err := TrainDictionaryCooperative(context.Background(), AdaptiveTrainOptions{
		ID:        101,
		Size:      1024,
		MaxChunks: 256,
		Samples:   adaptiveSamples(500, "training-shape"),
	})
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	defer candidate.Close()
	if got := len(candidate.Raw()); got == 0 || got > 1024 {
		t.Fatalf("dictionary bytes = %d, want 1..1024", got)
	}

	evaluation, err := EvaluateDictionaryCandidate(
		context.Background(),
		candidate,
		nil,
		adaptiveSamples(100, "held-out-shape"),
	)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if evaluation.Samples != 100 || evaluation.CandidateBytes >= evaluation.RawBytes {
		t.Fatalf("unexpected evaluation: %+v", evaluation)
	}
	promote, err := evaluation.ShouldPromote(0.05)
	if err != nil || !promote {
		t.Fatalf("promote = %v, err=%v, evaluation=%+v", promote, err, evaluation)
	}
}

type cancellingContext struct {
	checks int
	limit  int
}

func (c *cancellingContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancellingContext) Done() <-chan struct{}       { return nil }
func (c *cancellingContext) Value(any) any               { return nil }
func (c *cancellingContext) Err() error {
	c.checks++
	if c.checks >= c.limit {
		return context.Canceled
	}
	return nil
}

func TestCooperativeTrainingStopsAtBoundedCancellationPoint(t *testing.T) {
	ctx := &cancellingContext{limit: 3}
	_, err := TrainDictionaryCooperative(ctx, AdaptiveTrainOptions{
		ID:         102,
		Size:       4096,
		ChunkBytes: 32,
		MaxChunks:  4096,
		Samples:    adaptiveSamples(10_000, "cancel-me"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("train error = %v, want context.Canceled", err)
	}
	if ctx.checks > 4 {
		t.Fatalf("cancellation checks = %d, expected prompt stop", ctx.checks)
	}
}

func TestEvaluationCancellationAndPromotionValidation(t *testing.T) {
	dict, err := NewDictionary(103, bytes.Repeat([]byte("dictionary-pattern"), 32))
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	defer dict.Close()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := EvaluateDictionaryCandidate(cancelled, dict, nil, adaptiveSamples(1, "x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("evaluate error = %v, want context.Canceled", err)
	}
	if _, err := (DictionaryEvaluation{}).ShouldPromote(-0.1); !errors.Is(err, ErrInvalidPromotionGain) {
		t.Fatalf("negative promotion threshold error = %v", err)
	}
}

func TestDictionaryRawDoesNotExposeMutableCodecMemory(t *testing.T) {
	dict, err := NewDictionary(104, bytes.Repeat([]byte("immutable"), 32))
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	defer dict.Close()
	first := dict.Raw()
	first[0] ^= 0xff
	second := dict.Raw()
	if bytes.Equal(first, second) {
		t.Fatal("Raw exposed mutable dictionary memory")
	}
}
