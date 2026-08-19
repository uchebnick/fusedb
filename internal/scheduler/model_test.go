package scheduler

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"testing"
)

func TestModelSnapshotRoundTrip(t *testing.T) {
	want := ModelSnapshot{
		Generation:     17,
		BaselineRate:   123.5,
		BaselineReady:  true,
		BaselinePoints: 4,
	}
	want.Seasonal[7] = ModelPoint{Rate: 42.25, Samples: 3}
	want.Seasonal[SeasonalBucketCount-1] = ModelPoint{Rate: 900, Samples: 12}
	data, err := EncodeModelSnapshot(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeModelSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded model differs:\n got=%+v\nwant=%+v", got, want)
	}
}

func TestDecodeModelSnapshotRejectsCorruptionAndInvalidFloats(t *testing.T) {
	valid, err := EncodeModelSnapshot(ModelSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), valid...)
	corrupt[40] ^= 0xff
	if _, err := DecodeModelSnapshot(corrupt); !errors.Is(err, ErrModelCorrupt) {
		t.Fatalf("checksum error = %v", err)
	}

	nan := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint64(nan[16:24], math.Float64bits(math.NaN()))
	rechecksumModel(nan)
	if _, err := DecodeModelSnapshot(nan); !errors.Is(err, ErrModelCorrupt) {
		t.Fatalf("NaN error = %v", err)
	}
}

func FuzzDecodeModelSnapshotNeverPanics(f *testing.F) {
	valid, err := EncodeModelSnapshot(ModelSnapshot{})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(modelMagic))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeModelSnapshot(data)
	})
}

func rechecksumModel(data []byte) {
	// The production decoder is also responsible for validating semantic
	// values after the checksum, so tests need a structurally valid image.
	checksumOffset := len(data) - modelChecksumSize
	binary.LittleEndian.PutUint32(data[checksumOffset:], crc32Checksum(data[:checksumOffset]))
}

func crc32Checksum(data []byte) uint32 {
	// Kept local to avoid exposing implementation details solely for tests.
	return crc32.ChecksumIEEE(data)
}
