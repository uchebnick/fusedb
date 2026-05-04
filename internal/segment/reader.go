package segment

import (
	"errors"
	"fmt"

	"fusedb/internal/compression"
	"fusedb/internal/disk"
)

var (
	ErrNilSegment                = errors.New("segment: nil segment")
	ErrMissingDictionaryRegistry = errors.New("segment: missing dictionary registry for compressed segment")
	ErrReaderFileNotOpen         = errors.New("segment: reader file is not open")
)

// Reader provides point-lookups over one frozen immutable segment.
type Reader struct {
	segment    *Segment
	file       disk.File
	dictionary *compression.Dictionary
	closed     bool
}

// NewReader binds a frozen segment to a lookup reader and opens its file.
func NewReader(segment *Segment, registry *compression.Registry) (*Reader, error) {
	if segment == nil {
		return nil, ErrNilSegment
	}
	if !segment.Frozen() {
		return nil, ErrSegmentNotFrozen
	}

	reader := &Reader{
		segment: segment,
	}
	if segment.Header.Compression == CompressionZstdDict {
		if registry == nil {
			return nil, ErrMissingDictionaryRegistry
		}
		dict, err := registry.MustGet(segment.Header.DictionaryID)
		if err != nil {
			return nil, err
		}
		reader.dictionary = dict
	}

	if segment.fs == nil || segment.path == "" {
		return nil, ErrNilFilesystem
	}
	file, err := segment.fs.Open(segment.path)
	if err != nil {
		return nil, err
	}
	reader.file = file
	return reader, nil
}

// OpenReader loads one segment from file and binds it to a lookup reader.
func OpenReader(fs disk.FS, path string, registry *compression.Registry) (*Reader, error) {
	segment, err := OpenSegment(fs, path)
	if err != nil {
		return nil, err
	}
	return NewReader(segment, registry)
}

// Segment returns the frozen segment metadata backing this reader.
func (r *Reader) Segment() *Segment {
	if r == nil {
		return nil
	}
	return r.segment
}

// MayContain checks the segment-wide bloom filter.
func (r *Reader) MayContain(key []byte) bool {
	if r == nil || r.segment == nil || r.closed {
		return false
	}
	return r.segment.Bloom.MayContain(key)
}

// Get performs a point lookup.
func (r *Reader) Get(key []byte) ([]byte, bool, error) {
	if r == nil || r.segment == nil || r.closed {
		return nil, false, ErrNilSegment
	}
	if !r.segment.Bloom.MayContain(key) {
		return nil, false, nil
	}

	entry, _, ok := r.segment.Index.FindBlock(key)
	if !ok {
		return nil, false, nil
	}

	block, err := r.readBlock(entry)
	if err != nil {
		return nil, false, err
	}
	value, ok := block.Find(key)
	return value, ok, nil
}

// Close releases file resources held by the reader.
func (r *Reader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func (r *Reader) readBlock(entry BlockIndexEntry) (Block, error) {
	payload, err := r.readBlockPayload(entry)
	if err != nil {
		return Block{}, err
	}
	if r.segment.Header.Compression == CompressionZstdDict {
		payload, err = r.dictionary.Decompress(payload)
		if err != nil {
			return Block{}, fmt.Errorf("segment: decompress block: %w", err)
		}
	}
	block, err := DecodeBlock(payload)
	if err != nil {
		return Block{}, fmt.Errorf("segment: decode block: %w", err)
	}
	return block, nil
}

func (r *Reader) readBlockPayload(entry BlockIndexEntry) ([]byte, error) {
	if r == nil || r.segment == nil || r.closed {
		return nil, ErrNilSegment
	}
	section, err := r.segment.blockSection(entry)
	if err != nil {
		return nil, err
	}
	if r.file == nil {
		return nil, ErrReaderFileNotOpen
	}
	return readSectionFrom(r.file, section)
}
