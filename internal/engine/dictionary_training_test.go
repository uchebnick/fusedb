package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
)

func TestCanceledDictionaryTrainingDoesNotReserveID(t *testing.T) {
	fs := disk.NewMemFS()
	catalog, err := compression.OpenDictionaryGroupCatalog(fs, "training-cancel")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := compression.NewPersistentRegistry(fs, "training-cancel/dictionaries")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	trainer := newDictionaryTrainer(DictionaryTrainingConfig{
		DictionarySize: 64,
		ChunkBytes:     8,
		MaxChunks:      64,
	}, catalog, registry, nil, nil)
	trainer.groups[1] = &dictionaryTrainingGroup{
		training: [][]byte{[]byte("representative training sample bytes")},
	}

	before := catalog.Snapshot().NextDictionaryID
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := trainer.train(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("train error = %v, want context.Canceled", err)
	}
	if after := catalog.Snapshot().NextDictionaryID; after != before {
		t.Fatalf("canceled training advanced dictionary ID from %d to %d", before, after)
	}
}

func TestDictionaryTrainingFailureReleasesFullSampleGeneration(t *testing.T) {
	trainer := newDictionaryTrainer(DictionaryTrainingConfig{}, nil, nil, nil, nil)
	trainer.groups[7] = &dictionaryTrainingGroup{
		training:   [][]byte{[]byte("training")},
		evaluation: [][]byte{[]byte("evaluation")},
		bytes:      18,
		trainRun:   true,
	}
	trainer.totalBytes = 18

	trainer.trainDone(7, errors.New("transient training failure"))

	group := trainer.groups[7]
	if group.trainRun || len(group.training) != 0 || len(group.evaluation) != 0 || group.bytes != 0 {
		t.Fatalf("failed generation retained: %+v", group)
	}
	if trainer.totalBytes != 0 {
		t.Fatalf("total sample bytes = %d, want 0", trainer.totalBytes)
	}
}
