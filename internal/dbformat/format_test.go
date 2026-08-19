package dbformat

import (
	"errors"
	"fmt"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

func TestDescriptorRoundTripAndCompatibility(t *testing.T) {
	want := Current()
	data, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("descriptor = %+v, want %+v", got, want)
	}
	if err := CheckCompatible(got); err != nil {
		t.Fatal(err)
	}
}

func TestCompatibilityRejectsFutureAndUnknownRequiredFeatures(t *testing.T) {
	future := Current()
	future.Epoch++
	future.MinReaderEpoch = future.Epoch
	future.MinWriterEpoch = future.Epoch
	if err := CheckCompatible(future); !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("future error = %v", err)
	}
	unknown := Current()
	unknown.RequiredFeatures |= 1 << 63
	if err := CheckCompatible(unknown); !errors.Is(err, ErrUnknownRequiredFeature) {
		t.Fatalf("unknown feature error = %v", err)
	}
	optional := Current()
	optional.OptionalFeatures |= 1 << 63
	if err := CheckCompatible(optional); err != nil {
		t.Fatalf("unknown optional feature rejected: %v", err)
	}
	missing := Current()
	missing.RequiredFeatures &^= FeatureManifestV4
	if err := CheckCompatible(missing); !errors.Is(err, ErrMissingRequiredFeature) {
		t.Fatalf("missing feature error = %v", err)
	}
}

func TestEpochOneDescriptorRemainsReadableForMigration(t *testing.T) {
	legacy := Descriptor{
		Epoch:            1,
		MinReaderEpoch:   1,
		MinWriterEpoch:   1,
		RequiredFeatures: epoch1RequiredFeatures,
		OptionalFeatures: OptionalSchedulerModel,
	}
	if err := CheckCompatible(legacy); err != nil {
		t.Fatalf("epoch 1 descriptor rejected: %v", err)
	}
}

func TestEpochTwoDescriptorRemainsReadableForMigration(t *testing.T) {
	legacy := Descriptor{
		Epoch:            2,
		MinReaderEpoch:   2,
		MinWriterEpoch:   2,
		RequiredFeatures: epoch2RequiredFeatures,
		OptionalFeatures: OptionalSchedulerModel,
	}
	if err := CheckCompatible(legacy); err != nil {
		t.Fatalf("epoch 2 descriptor rejected: %v", err)
	}
}

func TestDecodeRejectsChecksumCorruption(t *testing.T) {
	data, err := Encode(Current())
	if err != nil {
		t.Fatal(err)
	}
	data[20] ^= 0xff
	if _, err := Decode(data); !errors.Is(err, ErrCorruptDescriptor) {
		t.Fatalf("decode error = %v, want ErrCorruptDescriptor", err)
	}
}

type formatSyncFaultFS struct {
	disk.FS
	fail bool
}

func (f *formatSyncFaultFS) SyncDir(dir string) error {
	if f.fail {
		return fmt.Errorf("injected format directory sync failure")
	}
	return f.FS.SyncDir(dir)
}

func TestSaveCurrentCommitUncertainLeavesCompleteVisibleDescriptor(t *testing.T) {
	base := disk.NewMemFS()
	fs := &formatSyncFaultFS{FS: base, fail: true}
	if err := SaveCurrent(fs, "db"); !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("save error = %v, want ErrCommitUncertain", err)
	}
	loaded, err := Load(base, "db")
	if err != nil {
		t.Fatalf("load visible descriptor: %v", err)
	}
	if loaded != Current() {
		t.Fatalf("visible descriptor = %+v", loaded)
	}
}

func FuzzDecodeNeverPanics(f *testing.F) {
	valid, err := Encode(Current())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(magic))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
