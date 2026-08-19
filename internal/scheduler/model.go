package scheduler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	SeasonalBucketCount = 7 * 24 * 4
	modelMagic          = "FDSM"
	modelVersion        = 1
	modelHeaderSize     = 4 + 4 + 8 + 8 + 4 + 4 + 4
	modelPointSize      = 8 + 4
	modelChecksumSize   = 4
	modelFlagReady      = 1 << 0
)

var (
	ErrModelCorrupt = errors.New("scheduler: corrupt persisted model")
	ErrModelVersion = errors.New("scheduler: unsupported persisted model version")
)

// ModelPoint is one learned UTC 15-minute time-of-week observation.
type ModelPoint struct {
	Rate    float64
	Samples uint32
}

// ModelSnapshot is detached learned scheduler state. Transient admission state
// and an incomplete 15-minute window are deliberately not persisted.
type ModelSnapshot struct {
	Generation     uint64
	BaselineRate   float64
	BaselineReady  bool
	BaselinePoints uint32
	Seasonal       [SeasonalBucketCount]ModelPoint
}

// EncodeModelSnapshot returns one stable checksummed model image.
func EncodeModelSnapshot(snapshot ModelSnapshot) ([]byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	size := modelHeaderSize + SeasonalBucketCount*modelPointSize + modelChecksumSize
	data := make([]byte, size)
	copy(data[:4], modelMagic)
	binary.LittleEndian.PutUint32(data[4:8], modelVersion)
	binary.LittleEndian.PutUint64(data[8:16], snapshot.Generation)
	binary.LittleEndian.PutUint64(data[16:24], math.Float64bits(snapshot.BaselineRate))
	binary.LittleEndian.PutUint32(data[24:28], snapshot.BaselinePoints)
	flags := uint32(0)
	if snapshot.BaselineReady {
		flags |= modelFlagReady
	}
	binary.LittleEndian.PutUint32(data[28:32], flags)
	binary.LittleEndian.PutUint32(data[32:36], SeasonalBucketCount)
	offset := modelHeaderSize
	for _, point := range snapshot.Seasonal {
		binary.LittleEndian.PutUint64(data[offset:offset+8], math.Float64bits(point.Rate))
		binary.LittleEndian.PutUint32(data[offset+8:offset+12], point.Samples)
		offset += modelPointSize
	}
	binary.LittleEndian.PutUint32(data[offset:], crc32.ChecksumIEEE(data[:offset]))
	return data, nil
}

// DecodeModelSnapshot verifies and decodes one complete model image.
func DecodeModelSnapshot(data []byte) (ModelSnapshot, error) {
	wantSize := modelHeaderSize + SeasonalBucketCount*modelPointSize + modelChecksumSize
	if len(data) != wantSize || len(data) < modelHeaderSize || string(data[:4]) != modelMagic {
		return ModelSnapshot{}, ErrModelCorrupt
	}
	if version := binary.LittleEndian.Uint32(data[4:8]); version != modelVersion {
		return ModelSnapshot{}, fmt.Errorf("%w: %d", ErrModelVersion, version)
	}
	if flags := binary.LittleEndian.Uint32(data[28:32]); flags & ^uint32(modelFlagReady) != 0 {
		return ModelSnapshot{}, ErrModelCorrupt
	}
	if count := binary.LittleEndian.Uint32(data[32:36]); count != SeasonalBucketCount {
		return ModelSnapshot{}, ErrModelCorrupt
	}
	checksumOffset := len(data) - modelChecksumSize
	if crc32.ChecksumIEEE(data[:checksumOffset]) != binary.LittleEndian.Uint32(data[checksumOffset:]) {
		return ModelSnapshot{}, ErrModelCorrupt
	}

	snapshot := ModelSnapshot{
		Generation:     binary.LittleEndian.Uint64(data[8:16]),
		BaselineRate:   math.Float64frombits(binary.LittleEndian.Uint64(data[16:24])),
		BaselinePoints: binary.LittleEndian.Uint32(data[24:28]),
		BaselineReady:  binary.LittleEndian.Uint32(data[28:32])&modelFlagReady != 0,
	}
	offset := modelHeaderSize
	for i := range snapshot.Seasonal {
		snapshot.Seasonal[i] = ModelPoint{
			Rate:    math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8])),
			Samples: binary.LittleEndian.Uint32(data[offset+8 : offset+12]),
		}
		offset += modelPointSize
	}
	if err := snapshot.Validate(); err != nil {
		return ModelSnapshot{}, err
	}
	return snapshot, nil
}

// Validate rejects values that could poison adaptive comparisons after load.
func (snapshot ModelSnapshot) Validate() error {
	if !validModelRate(snapshot.BaselineRate) {
		return ErrModelCorrupt
	}
	if snapshot.BaselineReady {
		if snapshot.BaselinePoints != 4 {
			return ErrModelCorrupt
		}
	} else if snapshot.BaselinePoints >= 4 {
		return ErrModelCorrupt
	}
	for _, point := range snapshot.Seasonal {
		if !validModelRate(point.Rate) || (point.Samples == 0 && point.Rate != 0) {
			return ErrModelCorrupt
		}
	}
	return nil
}

func validModelRate(rate float64) bool {
	return rate >= 0 && !math.IsNaN(rate) && !math.IsInf(rate, 0)
}
