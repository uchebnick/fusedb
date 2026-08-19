// Package dbformat owns database-directory compatibility negotiation.
package dbformat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
)

const (
	DefaultFileName = "FORMAT"
	magic           = "FDBF"
	codecVersion    = 1
	headerSize      = 4 + 4 + 4 + 4 + 4 + 8 + 8
	checksumSize    = 4

	CurrentEpoch          = 3
	MinimumSupportedEpoch = 1
)

type Feature uint64

const (
	FeatureManifestV4 Feature = 1 << iota
	FeaturePerLeafWALWatermarks
	FeatureDictionaryGroups
	FeatureExternalDictionaryCatalog
	// FeatureBoundedRecordSizes means writers reject and readers fail closed on
	// keys, values, blocks, and metadata above the documented safety ceilings.
	FeatureBoundedRecordSizes
	// FeatureAtomicBatches means WAL kind 4 may contain a bounded group of
	// mutations sharing one sequence/commit boundary.
	FeatureAtomicBatches
)

const (
	OptionalSchedulerModel Feature = 1 << iota
)

const epoch1RequiredFeatures = FeatureManifestV4 |
	FeaturePerLeafWALWatermarks |
	FeatureDictionaryGroups |
	FeatureExternalDictionaryCatalog

const epoch2RequiredFeatures = epoch1RequiredFeatures | FeatureBoundedRecordSizes

const knownRequiredFeatures = epoch2RequiredFeatures | FeatureAtomicBatches

var (
	ErrCorruptDescriptor      = errors.New("dbformat: corrupt format descriptor")
	ErrUnsupportedCodec       = errors.New("dbformat: unsupported descriptor codec")
	ErrFormatTooNew           = errors.New("dbformat: database format is newer than this binary")
	ErrFormatTooOld           = errors.New("dbformat: database format is older than this binary supports")
	ErrReaderTooOld           = errors.New("dbformat: reader epoch is too old")
	ErrWriterTooOld           = errors.New("dbformat: writer epoch is too old")
	ErrUnknownRequiredFeature = errors.New("dbformat: unknown required feature")
	ErrMissingRequiredFeature = errors.New("dbformat: required feature is missing")
)

// Descriptor declares the compatibility epoch and features used by one
// database directory. Unknown optional features are safe to ignore; unknown
// required features reject writable open.
type Descriptor struct {
	Epoch            uint32
	MinReaderEpoch   uint32
	MinWriterEpoch   uint32
	RequiredFeatures Feature
	OptionalFeatures Feature
}

// Current returns the descriptor emitted by this binary.
func Current() Descriptor {
	return Descriptor{
		Epoch:            CurrentEpoch,
		MinReaderEpoch:   CurrentEpoch,
		MinWriterEpoch:   CurrentEpoch,
		RequiredFeatures: knownRequiredFeatures,
		OptionalFeatures: OptionalSchedulerModel,
	}
}

func FileName(dir string) string {
	return filepath.Join(dir, DefaultFileName)
}

func Encode(descriptor Descriptor) ([]byte, error) {
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	data := make([]byte, headerSize+checksumSize)
	copy(data[:4], magic)
	binary.LittleEndian.PutUint32(data[4:8], codecVersion)
	binary.LittleEndian.PutUint32(data[8:12], descriptor.Epoch)
	binary.LittleEndian.PutUint32(data[12:16], descriptor.MinReaderEpoch)
	binary.LittleEndian.PutUint32(data[16:20], descriptor.MinWriterEpoch)
	binary.LittleEndian.PutUint64(data[20:28], uint64(descriptor.RequiredFeatures))
	binary.LittleEndian.PutUint64(data[28:36], uint64(descriptor.OptionalFeatures))
	binary.LittleEndian.PutUint32(data[36:40], crc32.ChecksumIEEE(data[:36]))
	return data, nil
}

func Decode(data []byte) (Descriptor, error) {
	if len(data) != headerSize+checksumSize || len(data) < 8 || string(data[:4]) != magic {
		return Descriptor{}, ErrCorruptDescriptor
	}
	if version := binary.LittleEndian.Uint32(data[4:8]); version != codecVersion {
		return Descriptor{}, fmt.Errorf("%w: %d", ErrUnsupportedCodec, version)
	}
	if crc32.ChecksumIEEE(data[:36]) != binary.LittleEndian.Uint32(data[36:40]) {
		return Descriptor{}, ErrCorruptDescriptor
	}
	descriptor := Descriptor{
		Epoch:            binary.LittleEndian.Uint32(data[8:12]),
		MinReaderEpoch:   binary.LittleEndian.Uint32(data[12:16]),
		MinWriterEpoch:   binary.LittleEndian.Uint32(data[16:20]),
		RequiredFeatures: Feature(binary.LittleEndian.Uint64(data[20:28])),
		OptionalFeatures: Feature(binary.LittleEndian.Uint64(data[28:36])),
	}
	if err := descriptor.Validate(); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

func (descriptor Descriptor) Validate() error {
	if descriptor.Epoch == 0 || descriptor.MinReaderEpoch == 0 || descriptor.MinWriterEpoch == 0 ||
		descriptor.MinReaderEpoch > descriptor.Epoch || descriptor.MinWriterEpoch > descriptor.Epoch {
		return ErrCorruptDescriptor
	}
	return nil
}

// CheckCompatible rejects a descriptor this binary cannot safely read and
// write. It does not mutate storage.
func CheckCompatible(descriptor Descriptor) error {
	if err := descriptor.Validate(); err != nil {
		return err
	}
	if descriptor.Epoch > CurrentEpoch {
		return fmt.Errorf("%w: database=%d binary=%d", ErrFormatTooNew, descriptor.Epoch, CurrentEpoch)
	}
	if descriptor.Epoch < MinimumSupportedEpoch {
		return fmt.Errorf("%w: database=%d minimum=%d", ErrFormatTooOld, descriptor.Epoch, MinimumSupportedEpoch)
	}
	if descriptor.MinReaderEpoch > CurrentEpoch {
		return fmt.Errorf("%w: need=%d binary=%d", ErrReaderTooOld, descriptor.MinReaderEpoch, CurrentEpoch)
	}
	if descriptor.MinWriterEpoch > CurrentEpoch {
		return fmt.Errorf("%w: need=%d binary=%d", ErrWriterTooOld, descriptor.MinWriterEpoch, CurrentEpoch)
	}
	if unknown := descriptor.RequiredFeatures &^ knownRequiredFeatures; unknown != 0 {
		return fmt.Errorf("%w: 0x%x", ErrUnknownRequiredFeature, uint64(unknown))
	}
	required := knownRequiredFeatures
	switch descriptor.Epoch {
	case 1:
		required = epoch1RequiredFeatures
	case 2:
		required = epoch2RequiredFeatures
	}
	if missing := required &^ descriptor.RequiredFeatures; missing != 0 {
		return fmt.Errorf("%w: 0x%x", ErrMissingRequiredFeature, uint64(missing))
	}
	return nil
}

func Load(fs disk.FS, dir string) (Descriptor, error) {
	data, err := disk.ReadFileLimited(fs, FileName(dir), limits.MaxFormatBytes)
	if err != nil {
		return Descriptor{}, err
	}
	return Decode(data)
}

func SaveCurrent(fs disk.FS, dir string) error {
	data, err := Encode(Current())
	if err != nil {
		return err
	}
	return disk.WriteFileAtomically(fs, FileName(dir), data)
}
