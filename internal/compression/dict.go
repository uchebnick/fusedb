package compression

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	MinDictionarySize     = 8
	DefaultDictionarySize = 64 << 10
	DefaultZstdLevel      = 3
)

var (
	ErrNilDictionary       = errors.New("compression: nil dictionary")
	ErrEmptyDictionary     = errors.New("compression: empty dictionary")
	ErrZeroDictionaryID    = errors.New("compression: dictionary id must be non-zero")
	ErrNoSamples           = errors.New("compression: no samples provided")
	ErrDictionaryNotFound  = errors.New("compression: dictionary not found")
	ErrDuplicateDictionary = errors.New("compression: duplicate dictionary id")
)

// Dictionary is immutable runtime wrapper around one zstd dictionary.
//
// Same dictionary bytes must be used for both compression and decompression.
// Dictionary is safe for concurrent block-level Compress/Decompress calls.
type Dictionary struct {
	id      uint32
	raw     []byte
	level   int
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// TrainOptions configures dictionary training.
type TrainOptions struct {
	ID         uint32
	Size       int
	Level      int
	Samples    [][]byte
	CompatV155 bool
}

// Registry stores loaded dictionaries by ID.
type Registry struct {
	mu    sync.RWMutex
	dicts map[uint32]*Dictionary
}

// NewDictionary builds reusable zstd dictionary codec from raw dictionary bytes.
func NewDictionary(id uint32, raw []byte) (*Dictionary, error) {
	return NewDictionaryLevel(id, raw, DefaultZstdLevel)
}

// NewDictionaryLevel builds reusable zstd dictionary codec with explicit level.
func NewDictionaryLevel(id uint32, raw []byte, level int) (*Dictionary, error) {
	if id == 0 {
		return nil, ErrZeroDictionaryID
	}
	if len(raw) == 0 {
		return nil, ErrEmptyDictionary
	}
	if level == 0 {
		level = DefaultZstdLevel
	}

	levelOpt := zstd.EncoderLevelFromZstd(level)
	dictBytes := bytes.Clone(raw)

	encoder, err := zstd.NewWriter(
		nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(levelOpt),
		zstd.WithEncoderDict(dictBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("compression: create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(
		nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderDicts(dictBytes),
	)
	if err != nil {
		_ = encoder.Close()
		return nil, fmt.Errorf("compression: create zstd decoder: %w", err)
	}

	return &Dictionary{
		id:      id,
		raw:     dictBytes,
		level:   level,
		encoder: encoder,
		decoder: decoder,
	}, nil
}

// TrainDictionary trains zstd dictionary bytes from representative samples.
func TrainDictionary(opts TrainOptions) ([]byte, error) {
	if opts.ID == 0 {
		return nil, ErrZeroDictionaryID
	}
	if opts.Size == 0 {
		opts.Size = DefaultDictionarySize
	}
	if opts.Size < MinDictionarySize {
		return nil, fmt.Errorf("compression: dictionary size %d < %d", opts.Size, MinDictionarySize)
	}

	samples := normalizeSamples(opts.Samples)
	if len(samples) == 0 {
		return nil, ErrNoSamples
	}

	history := makeHistory(opts.Size, samples)
	if len(history) < MinDictionarySize {
		return nil, fmt.Errorf("compression: dictionary history %d < %d", len(history), MinDictionarySize)
	}

	level := zstd.EncoderLevelFromZstd(opts.Level)
	if opts.Level == 0 {
		level = zstd.EncoderLevelFromZstd(DefaultZstdLevel)
	}

	raw, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:         opts.ID,
		Contents:   samples,
		History:    history,
		Offsets:    [3]int{1, 4, 8},
		CompatV155: opts.CompatV155,
		Level:      level,
	})
	if err != nil {
		return nil, fmt.Errorf("compression: build zstd dictionary: %w", err)
	}
	return raw, nil
}

// ID returns stable dictionary identifier.
func (d *Dictionary) ID() uint32 {
	if d == nil {
		return 0
	}
	return d.id
}

// Level returns configured encoder level.
func (d *Dictionary) Level() int {
	if d == nil {
		return 0
	}
	return d.level
}

// Raw returns detached raw dictionary bytes.
func (d *Dictionary) Raw() []byte {
	if d == nil {
		return nil
	}
	return bytes.Clone(d.raw)
}

// Compress encodes one independent block with dictionary.
func (d *Dictionary) Compress(src []byte) ([]byte, error) {
	return d.CompressInto(nil, src)
}

// CompressInto encodes src and appends compressed bytes to dst.
func (d *Dictionary) CompressInto(dst, src []byte) ([]byte, error) {
	if d == nil {
		return nil, ErrNilDictionary
	}
	return d.encoder.EncodeAll(src, dst[:0]), nil
}

// Decompress decodes one independent block with dictionary.
func (d *Dictionary) Decompress(src []byte) ([]byte, error) {
	return d.DecompressInto(nil, src)
}

// DecompressInto decodes src and appends raw bytes to dst.
func (d *Dictionary) DecompressInto(dst, src []byte) ([]byte, error) {
	if d == nil {
		return nil, ErrNilDictionary
	}
	out, err := d.decoder.DecodeAll(src, dst[:0])
	if err != nil {
		return nil, fmt.Errorf("compression: decode zstd block: %w", err)
	}
	return out, nil
}

// Close releases held codec resources.
func (d *Dictionary) Close() error {
	if d == nil {
		return nil
	}

	var errs []error
	if d.encoder != nil {
		if err := d.encoder.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.decoder != nil {
		d.decoder.Close()
	}

	d.encoder = nil
	d.decoder = nil

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// NewRegistry creates empty dictionary registry.
func NewRegistry() *Registry {
	return &Registry{
		dicts: make(map[uint32]*Dictionary),
	}
}

// Add registers dictionary. Existing IDs are rejected.
func (r *Registry) Add(dict *Dictionary) error {
	if dict == nil {
		return ErrNilDictionary
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.dicts[dict.id]; exists {
		return fmt.Errorf("%w: %d", ErrDuplicateDictionary, dict.id)
	}
	r.dicts[dict.id] = dict
	return nil
}

// Get returns dictionary by ID.
func (r *Registry) Get(id uint32) (*Dictionary, bool) {
	if r == nil {
		return nil, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	dict, ok := r.dicts[id]
	return dict, ok
}

// MustGet returns dictionary or error when it is missing.
func (r *Registry) MustGet(id uint32) (*Dictionary, error) {
	dict, ok := r.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrDictionaryNotFound, id)
	}
	return dict, nil
}

// Remove deletes dictionary registration.
func (r *Registry) Remove(id uint32) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.dicts, id)
}

// Close releases all registered dictionaries.
func (r *Registry) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	for id, dict := range r.dicts {
		if err := dict.Close(); err != nil {
			errs = append(errs, fmt.Errorf("compression: close dictionary %d: %w", id, err))
		}
		delete(r.dicts, id)
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// Compress encodes src with dictionary selected by ID.
func (r *Registry) Compress(id uint32, src []byte) ([]byte, error) {
	dict, err := r.MustGet(id)
	if err != nil {
		return nil, err
	}
	return dict.Compress(src)
}

// Decompress decodes src with dictionary selected by ID.
func (r *Registry) Decompress(id uint32, src []byte) ([]byte, error) {
	dict, err := r.MustGet(id)
	if err != nil {
		return nil, err
	}
	return dict.Decompress(src)
}

func normalizeSamples(samples [][]byte) [][]byte {
	out := make([][]byte, 0, len(samples))
	for _, sample := range samples {
		if len(sample) == 0 {
			continue
		}
		out = append(out, bytes.Clone(sample))
	}
	return out
}

func makeHistory(size int, samples [][]byte) []byte {
	history := make([]byte, 0, size)
	for _, sample := range samples {
		if len(history) == size {
			break
		}

		remain := size - len(history)
		chunk := sample
		if len(chunk) > remain {
			chunk = chunk[:remain]
		}
		history = append(history, chunk...)
	}
	return history
}
