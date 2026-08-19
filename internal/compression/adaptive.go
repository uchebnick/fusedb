package compression

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	defaultAdaptiveChunkBytes = 64
	defaultAdaptiveMaxChunks  = 8 << 10
	adaptiveCancelEvery       = 256
)

var ErrInvalidPromotionGain = errors.New("compression: invalid promotion gain")

// AdaptiveTrainOptions bounds a cooperatively cancellable dictionary build.
// Samples should be encoded raw segment blocks, matching the real compression
// unit. MaxChunks bounds both CPU bookkeeping and temporary memory.
type AdaptiveTrainOptions struct {
	ID         uint32
	Size       int
	Level      int
	ChunkBytes int
	MaxChunks  int
	Samples    [][]byte
}

type adaptiveChunk struct {
	hash  uint64
	data  []byte
	count uint32
}

// TrainDictionaryCooperative constructs a raw LZ4 history from frequent byte
// chunks and checks ctx at bounded intervals. Unlike dictbuilder.BuildRawDict,
// it never enters one monolithic uninterruptible training call.
func TrainDictionaryCooperative(ctx context.Context, opts AdaptiveTrainOptions) (*Dictionary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.ID == 0 {
		return nil, ErrZeroDictionaryID
	}
	if opts.Size == 0 {
		opts.Size = DefaultDictionarySize
	}
	if opts.Size < MinDictionarySize {
		return nil, fmt.Errorf("compression: dictionary size %d < %d", opts.Size, MinDictionarySize)
	}
	if opts.ChunkBytes <= 0 {
		opts.ChunkBytes = defaultAdaptiveChunkBytes
	}
	if opts.ChunkBytes < MinDictionarySize || opts.ChunkBytes > opts.Size {
		return nil, fmt.Errorf("compression: adaptive chunk size %d outside [%d,%d]", opts.ChunkBytes, MinDictionarySize, opts.Size)
	}
	if opts.MaxChunks <= 0 {
		opts.MaxChunks = defaultAdaptiveMaxChunks
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	chunks := make(map[uint64]*adaptiveChunk, min(opts.MaxChunks, 1024))
	windows := 0
	usableSamples := 0
	for _, sample := range opts.Samples {
		if len(sample) < MinDictionarySize {
			continue
		}
		usableSamples++
		width := min(opts.ChunkBytes, len(sample))
		step := max(MinDictionarySize, width/2)
		last := len(sample) - width
		for offset := 0; ; offset += step {
			if offset > last {
				offset = last
			}
			window := sample[offset : offset+width]
			hash := xxhash.Sum64(window)
			if existing := chunks[hash]; existing != nil {
				if existing.count != ^uint32(0) {
					existing.count++
				}
			} else if len(chunks) < opts.MaxChunks {
				chunks[hash] = &adaptiveChunk{hash: hash, data: append([]byte(nil), window...), count: 1}
			}
			windows++
			if windows%adaptiveCancelEvery == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if offset == last {
				break
			}
		}
	}
	if usableSamples == 0 || len(chunks) == 0 {
		return nil, ErrNoSamples
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ranked := make([]*adaptiveChunk, 0, len(chunks))
	for _, chunk := range chunks {
		ranked = append(ranked, chunk)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].count != ranked[j].count {
			return ranked[i].count > ranked[j].count
		}
		return ranked[i].hash < ranked[j].hash
	})

	selected := make([]*adaptiveChunk, 0, min(len(ranked), opts.Size/opts.ChunkBytes+1))
	selectedBytes := 0
	for _, chunk := range ranked {
		if selectedBytes >= opts.Size {
			break
		}
		selected = append(selected, chunk)
		selectedBytes += len(chunk.data)
	}
	history := make([]byte, 0, min(opts.Size, selectedBytes))
	// LZ4 matches favor bytes nearest the end of history, so the most frequent
	// chunks are appended last after selection by descending frequency.
	for i := len(selected) - 1; i >= 0; i-- {
		chunk := selected[i].data
		remaining := opts.Size - len(history)
		if remaining <= 0 {
			break
		}
		if len(chunk) > remaining {
			chunk = chunk[len(chunk)-remaining:]
		}
		history = append(history, chunk...)
		if i%adaptiveCancelEvery == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	if len(history) < MinDictionarySize {
		return nil, ErrNoSamples
	}
	return NewDictionaryLevel(opts.ID, history, opts.Level)
}

// DictionaryEvaluation reports candidate effectiveness on a held-out sample
// set. Byte totals include the block framing produced by Dictionary.Compress.
type DictionaryEvaluation struct {
	Samples             uint64
	RawBytes            uint64
	CandidateBytes      uint64
	CurrentBytes        uint64
	CandidateCompress   time.Duration
	CandidateDecompress time.Duration
	CurrentCompress     time.Duration
	CurrentDecompress   time.Duration
}

// SavingsRatio compares candidate bytes with the current dictionary, or raw
// bytes when there is no current dictionary. Positive values are improvements.
func (e DictionaryEvaluation) SavingsRatio() float64 {
	baseline := e.CurrentBytes
	if baseline == 0 {
		baseline = e.RawBytes
	}
	if baseline == 0 {
		return 0
	}
	return 1 - float64(e.CandidateBytes)/float64(baseline)
}

// ShouldPromote applies a minimum held-out compression gain. Latency totals are
// exported for policy/metrics but deliberately not folded into one hard-coded
// score; deployments can set their own CPU-versus-space tradeoff later.
func (e DictionaryEvaluation) ShouldPromote(minimumGain float64) (bool, error) {
	if minimumGain < 0 || minimumGain >= 1 {
		return false, ErrInvalidPromotionGain
	}
	return e.Samples > 0 && e.SavingsRatio() >= minimumGain, nil
}

// EvaluateDictionaryCandidate measures and verifies a candidate on samples
// disjoint from its training set. Cancellation is observed between bounded
// block codec calls.
func EvaluateDictionaryCandidate(
	ctx context.Context,
	candidate, current *Dictionary,
	samples [][]byte,
) (DictionaryEvaluation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if candidate == nil {
		return DictionaryEvaluation{}, ErrNilDictionary
	}
	var evaluation DictionaryEvaluation
	for _, sample := range samples {
		if len(sample) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return DictionaryEvaluation{}, err
		}
		evaluation.Samples++
		evaluation.RawBytes += uint64(len(sample))

		started := time.Now()
		compressed, err := candidate.Compress(sample)
		evaluation.CandidateCompress += time.Since(started)
		if err != nil {
			return DictionaryEvaluation{}, fmt.Errorf("compression: evaluate candidate compress: %w", err)
		}
		evaluation.CandidateBytes += uint64(len(compressed))
		started = time.Now()
		decoded, err := candidate.Decompress(compressed)
		evaluation.CandidateDecompress += time.Since(started)
		if err != nil {
			return DictionaryEvaluation{}, fmt.Errorf("compression: evaluate candidate decompress: %w", err)
		}
		matches := bytes.Equal(decoded.Data, sample)
		decoded.Release()
		if !matches {
			return DictionaryEvaluation{}, ErrCorruptBlock
		}

		if current != nil {
			started = time.Now()
			currentCompressed, err := current.Compress(sample)
			evaluation.CurrentCompress += time.Since(started)
			if err != nil {
				return DictionaryEvaluation{}, fmt.Errorf("compression: evaluate current compress: %w", err)
			}
			evaluation.CurrentBytes += uint64(len(currentCompressed))
			started = time.Now()
			currentDecoded, err := current.Decompress(currentCompressed)
			evaluation.CurrentDecompress += time.Since(started)
			if err != nil {
				return DictionaryEvaluation{}, fmt.Errorf("compression: evaluate current decompress: %w", err)
			}
			currentDecoded.Release()
		}
	}
	if evaluation.Samples == 0 {
		return DictionaryEvaluation{}, ErrNoSamples
	}
	return evaluation, nil
}
