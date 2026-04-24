package compression

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"sync"

	"fusedb/internal/disk"

	"github.com/klauspost/compress/zstd"
)

const (
	MinDictionarySize     = 8
	DefaultDictionarySize = 64 << 10
	DefaultZstdLevel      = 3
)

var (
	ErrNilDictionary        = errors.New("compression: nil dictionary")
	ErrEmptyDictionary      = errors.New("compression: empty dictionary")
	ErrZeroDictionaryID     = errors.New("compression: dictionary id must be non-zero")
	ErrNoSamples            = errors.New("compression: no samples provided")
	ErrBuildDictionaryPanic = errors.New("compression: zstd dictionary build panic")
	ErrDictionaryNotFound   = errors.New("compression: dictionary not found")
	ErrDuplicateDictionary  = errors.New("compression: duplicate dictionary id")
	ErrDictionaryClosed     = errors.New("compression: dictionary closed")
	ErrNilDictionaryFS      = errors.New("compression: nil dictionary filesystem")
	ErrDictionaryStorage    = errors.New("compression: dictionary registry has no storage")
	ErrDictionaryIDMismatch = errors.New("compression: dictionary id mismatch")
)

// Dictionary is immutable runtime wrapper around one zstd dictionary.
//
// Same dictionary bytes must be used for both compression and decompression.
// Dictionary is safe for concurrent block-level Compress/Decompress calls.
type Dictionary struct {
	mu      sync.Mutex
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
	mu     sync.RWMutex
	loadMu sync.Mutex
	dicts  map[uint32]*registryEntry
	lru    *list.List
	limit  int
	fs     disk.FS
	dir    string
}

type registryEntry struct {
	dict *Dictionary
	elem *list.Element
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
func TrainDictionary(opts TrainOptions) (raw []byte, err error) {
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

	defer func() {
		if r := recover(); r != nil {
			raw = nil
			err = fmt.Errorf("%w: %v", ErrBuildDictionaryPanic, r)
		}
	}()

	raw, err = zstd.BuildDict(zstd.BuildDictOptions{
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
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.encoder == nil {
		return nil, ErrDictionaryClosed
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
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.decoder == nil {
		return nil, ErrDictionaryClosed
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
	d.mu.Lock()
	defer d.mu.Unlock()

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
	return newRegistry(0)
}

// NewLRURegistry creates memory-only dictionary registry with cache limit.
//
// limit <= 0 means unlimited. Eviction removes dictionary only from registry;
// active readers that already hold the dictionary keep working.
func NewLRURegistry(limit int) *Registry {
	return newRegistry(limit)
}

// NewPersistentRegistry creates registry backed by dictionary files.
//
// Loaded dictionaries stay cached in memory. Disk is used only for Save and
// first lookup after restart/recovery.
func NewPersistentRegistry(fs disk.FS, dir string, limit ...int) (*Registry, error) {
	if fs == nil {
		return nil, ErrNilDictionaryFS
	}
	if dir != "" {
		if err := fs.MkdirAll(dir); err != nil {
			return nil, err
		}
	}

	cacheLimit := 0
	if len(limit) > 0 {
		cacheLimit = limit[0]
	}
	registry := newRegistry(cacheLimit)
	registry.fs = fs
	registry.dir = dir
	return registry, nil
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
	r.storeLocked(dict)
	return nil
}

// Save persists dictionary and keeps it in the registry cache.
func (r *Registry) Save(dict *Dictionary) error {
	if dict == nil {
		return ErrNilDictionary
	}
	if !r.hasStorage() {
		return ErrDictionaryStorage
	}

	r.loadMu.Lock()
	defer r.loadMu.Unlock()

	r.mu.RLock()
	existing, exists := r.dicts[dict.id]
	r.mu.RUnlock()
	if exists && existing.dict != dict {
		return fmt.Errorf("%w: %d", ErrDuplicateDictionary, dict.id)
	}

	name := DictionaryFileName(r.dir, dict.ID())
	if err := SaveDictionary(r.fs, name, dict); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, exists := r.dicts[dict.id]; exists && existing.dict != dict {
		return fmt.Errorf("%w: %d", ErrDuplicateDictionary, dict.id)
	}
	r.storeLocked(dict)
	return nil
}

// Load reads dictionary from registry storage and caches it in memory.
func (r *Registry) Load(id uint32) (*Dictionary, error) {
	if !r.hasStorage() {
		return nil, ErrDictionaryStorage
	}

	if dict, ok := r.Get(id); ok {
		return dict, nil
	}

	r.loadMu.Lock()
	defer r.loadMu.Unlock()

	if dict, ok := r.Get(id); ok {
		return dict, nil
	}

	name := DictionaryFileName(r.dir, id)
	dict, err := LoadDictionary(r.fs, name)
	if err != nil {
		return nil, err
	}
	if dict.ID() != id {
		_ = dict.Close()
		return nil, fmt.Errorf("%w: want %d, got %d", ErrDictionaryIDMismatch, id, dict.ID())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, exists := r.dicts[id]; exists {
		_ = dict.Close()
		r.touchLocked(existing)
		return existing.dict, nil
	}
	r.storeLocked(dict)
	return dict, nil
}

// Get returns dictionary by ID.
func (r *Registry) Get(id uint32) (*Dictionary, bool) {
	if r == nil {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.dicts[id]
	if !ok {
		return nil, false
	}
	r.touchLocked(entry)
	return entry.dict, true
}

// MustGet returns dictionary or error when it is missing.
func (r *Registry) MustGet(id uint32) (*Dictionary, error) {
	dict, ok := r.Get(id)
	if ok {
		return dict, nil
	}
	if r.hasStorage() {
		return r.Load(id)
	}
	return nil, fmt.Errorf("%w: %d", ErrDictionaryNotFound, id)
}

// Remove deletes dictionary registration.
func (r *Registry) Remove(id uint32) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeLocked(id)
}

// Close releases all registered dictionaries.
func (r *Registry) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()

	dicts := make([]*Dictionary, 0, len(r.dicts))
	for id, dict := range r.dicts {
		dicts = append(dicts, dict.dict)
		r.removeLocked(id)
	}
	r.mu.Unlock()

	var errs []error
	for _, dict := range dicts {
		if err := dict.Close(); err != nil {
			errs = append(errs, fmt.Errorf("compression: close dictionary %d: %w", dict.ID(), err))
		}
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

func (r *Registry) hasStorage() bool {
	return r != nil && r.fs != nil
}

func newRegistry(limit int) *Registry {
	if limit < 0 {
		limit = 0
	}
	return &Registry{
		dicts: make(map[uint32]*registryEntry),
		lru:   list.New(),
		limit: limit,
	}
}

func (r *Registry) storeLocked(dict *Dictionary) {
	if existing, exists := r.dicts[dict.id]; exists {
		existing.dict = dict
		r.touchLocked(existing)
		return
	}

	entry := &registryEntry{
		dict: dict,
		elem: r.lru.PushFront(dict.id),
	}
	r.dicts[dict.id] = entry
	r.evictLocked()
}

func (r *Registry) touchLocked(entry *registryEntry) {
	if entry == nil || entry.elem == nil {
		return
	}
	r.lru.MoveToFront(entry.elem)
}

func (r *Registry) evictLocked() {
	for r.limit > 0 && len(r.dicts) > r.limit {
		back := r.lru.Back()
		if back == nil {
			return
		}
		id, ok := back.Value.(uint32)
		if !ok {
			r.lru.Remove(back)
			continue
		}
		r.removeLocked(id)
	}
}

func (r *Registry) removeLocked(id uint32) {
	entry, exists := r.dicts[id]
	if !exists {
		return
	}
	if entry.elem != nil {
		r.lru.Remove(entry.elem)
	}
	delete(r.dicts, id)
}
