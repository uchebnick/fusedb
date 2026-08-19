package segment

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/disk"
)

const (
	DefaultTargetBlockSize       = 4 << 10
	DefaultBloomFilterFalseRate  = defaultFalsePositiveRate
	minEncodedBlockOverheadBytes = rawBlockHeaderSize + rawBlockChecksumSize + 4
)

var (
	ErrInvalidTargetBlockSize = errors.New("segment: invalid target block size")
	ErrMissingDictionary      = errors.New("segment: missing compression dictionary")
	ErrUnexpectedDictionary   = errors.New("segment: unexpected compression dictionary for raw segment")
	ErrTooManySegmentBlocks   = errors.New("segment: too many segment blocks")
	ErrSegmentDataTooLarge    = errors.New("segment: encoded segment data too large")
	ErrSegmentFrozen          = errors.New("segment: segment is frozen")
	ErrUnsortedSegmentEntries = errors.New("segment: segment entries out of order")
	ErrNilFilesystem          = errors.New("segment: nil filesystem")
	ErrZeroSegmentID          = errors.New("segment: zero segment id")
	ErrSegmentFileExists      = errors.New("segment: final segment file already exists")
	ErrSegmentAborted         = errors.New("segment: segment build aborted")
)

// Options configure one mutable segment writer.
type Options struct {
	FS                    disk.FS
	Dir                   string
	SegmentID             uint64
	Version               uint64
	ExpectedKeys          int
	TargetBlockSize       int
	BloomFalsePositive    float64
	Compression           CompressionKind
	CompressionDictionary *compression.Dictionary
	// ObserveRawBlock receives the encoded block before compression. The slice
	// is ephemeral; observers must copy bounded samples and return promptly.
	ObserveRawBlock func(raw []byte)
}

// NewSegment creates mutable segment writer state.
func NewSegment(opts Options) (*Segment, error) {
	fs := opts.FS
	if fs == nil {
		return nil, ErrNilFilesystem
	}
	if opts.SegmentID == 0 {
		return nil, ErrZeroSegmentID
	}

	targetBlockSize := opts.TargetBlockSize
	if targetBlockSize == 0 {
		targetBlockSize = DefaultTargetBlockSize
	}
	if targetBlockSize < minEncodedBlockOverheadBytes {
		return nil, fmt.Errorf("%w: %d", ErrInvalidTargetBlockSize, targetBlockSize)
	}

	bloomFalseRate := opts.BloomFalsePositive
	if bloomFalseRate == 0 {
		bloomFalseRate = DefaultBloomFilterFalseRate
	}
	bloom, err := NewBloomFilter(opts.ExpectedKeys, bloomFalseRate)
	if err != nil {
		return nil, fmt.Errorf("segment: create bloom filter: %w", err)
	}

	switch opts.Compression {
	case CompressionNone:
		if opts.CompressionDictionary != nil {
			return nil, ErrUnexpectedDictionary
		}
	case CompressionLZ4Dict:
		if opts.CompressionDictionary == nil {
			return nil, ErrMissingDictionary
		}
	default:
		return nil, fmt.Errorf("%w: %d", ErrInvalidCompressionKind, opts.Compression)
	}

	finalPath := SegmentFileName(opts.Dir, opts.SegmentID, opts.Version)
	tempPath := SegmentTempFileName(opts.Dir, opts.SegmentID, opts.Version)
	if opts.Dir != "" && opts.Dir != "." {
		if err := fs.MkdirAll(opts.Dir); err != nil {
			return nil, fmt.Errorf("segment: create dir: %w", err)
		}
	}
	if _, err := fs.Stat(finalPath); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrSegmentFileExists, finalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("segment: stat final file: %w", err)
	}

	file, err := fs.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("segment: create temp file: %w", err)
	}
	header := Header{
		Version:     opts.Version,
		Compression: opts.Compression,
	}
	if opts.Compression == CompressionLZ4Dict {
		header.DictionaryID = opts.CompressionDictionary.ID()
	}
	headerBytes, err := header.MarshalBinary()
	if err != nil {
		_ = file.Close()
		_ = fs.Remove(tempPath)
		return nil, fmt.Errorf("segment: encode initial header: %w", err)
	}
	if err := disk.WriteAll(file, headerBytes); err != nil {
		_ = file.Close()
		_ = fs.Remove(tempPath)
		return nil, fmt.Errorf("segment: write initial header: %w", err)
	}

	return &Segment{
		Header:           header,
		targetBlockSize:  targetBlockSize,
		Bloom:            bloom,
		compression:      opts.Compression,
		dictionary:       opts.CompressionDictionary,
		observeRawBlock:  opts.ObserveRawBlock,
		fs:               fs,
		file:             file,
		path:             finalPath,
		tempPath:         tempPath,
		currentBlockSize: rawBlockHeaderSize + rawBlockChecksumSize,
		Index: Index{
			entries: make([]BlockIndexEntry, 0),
		},
	}, nil
}

// Append adds one sorted entry into mutable segment state.
func (s *Segment) Append(entry BlockEntry) error {
	return s.append(entry, true)
}

// AppendUnsafe adds one sorted entry without copying key/value into the current block.
//
// Callers must keep entry bytes immutable until the segment flushes its current
// block. This is intended for merge paths where source entries are already
// immutable and short-lived.
func (s *Segment) AppendUnsafe(entry BlockEntry) error {
	return s.append(entry, false)
}

func (s *Segment) append(entry BlockEntry, clone bool) error {
	if s == nil {
		return ErrInvalidTargetBlockSize
	}
	if s.frozen {
		return ErrSegmentFrozen
	}
	if s.aborted {
		return ErrSegmentAborted
	}
	if len(s.Index.entries) >= math.MaxUint32 {
		return ErrTooManySegmentBlocks
	}
	if len(s.lastKey) > 0 && bytes.Compare(s.lastKey, entry.Key) >= 0 {
		return ErrUnsortedSegmentEntries
	}

	entrySize := rawBlockEntryHeaderSize + len(entry.Key) + len(entry.Value) + 4
	if s.currentBlock.Len() > 0 && s.currentBlockSize+entrySize > s.targetBlockSize {
		if err := s.flushBlock(); err != nil {
			return err
		}
	}

	if clone {
		if err := s.currentBlock.Add(entry); err != nil {
			return err
		}
	} else {
		if err := s.currentBlock.AddUnsafe(entry); err != nil {
			return err
		}
	}
	s.currentBlockSize += entrySize
	s.Bloom.Add(entry.Key)
	s.lastKey = bytes.Clone(entry.Key)
	return nil
}

// AppendKV adds one sorted key/value pair into mutable segment state.
func (s *Segment) AppendKV(key, value []byte) error {
	return s.Append(BlockEntry{Key: key, Value: value})
}

// AppendKVUnsafe adds one sorted key/value pair without copying bytes.
func (s *Segment) AppendKVUnsafe(key, value []byte) error {
	return s.AppendUnsafe(BlockEntry{Key: key, Value: value})
}

// Freeze finalizes segment bytes and makes the segment immutable.
func (s *Segment) Freeze() error {
	if s == nil {
		return ErrInvalidTargetBlockSize
	}
	if s.frozen {
		return nil
	}
	if s.aborted {
		return ErrSegmentAborted
	}

	if err := s.flushBlock(); err != nil {
		return err
	}

	bloomBytes, err := s.Bloom.MarshalBinary()
	if err != nil {
		return fmt.Errorf("segment: encode bloom filter: %w", err)
	}

	indexBytes, err := s.Index.MarshalBinary()
	if err != nil {
		return fmt.Errorf("segment: encode index: %w", err)
	}

	footer := Footer{
		BlockCount: uint32(s.Index.Len()),
		Data: Section{
			Offset: headerSize,
			Length: s.dataLength,
		},
		Bloom: Section{
			Offset: headerSize + s.dataLength,
			Length: uint64(len(bloomBytes)),
		},
		Index: Section{
			Offset: headerSize + s.dataLength + uint64(len(bloomBytes)),
			Length: uint64(len(indexBytes)),
		},
	}
	footerBytes, err := footer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("segment: encode footer: %w", err)
	}

	if err := disk.WriteAll(s.file, bloomBytes); err != nil {
		return fmt.Errorf("segment: write bloom section: %w", err)
	}
	if err := disk.WriteAll(s.file, indexBytes); err != nil {
		return fmt.Errorf("segment: write index section: %w", err)
	}
	if err := disk.WriteAll(s.file, footerBytes); err != nil {
		return fmt.Errorf("segment: write footer: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("segment: sync file: %w", err)
	}
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("segment: close file: %w", err)
	}
	s.file = nil
	if err := s.fs.Rename(s.tempPath, s.path); err != nil {
		return fmt.Errorf("segment: rename temp file: %w", err)
	}
	// The finalized path is visible now. Mark the object frozen before syncing
	// the directory so a commit-uncertain error can never make Abort delete (or
	// attempt to reuse) this complete output.
	s.Footer = footer
	s.frozen = true
	s.currentBlock = Block{}
	s.currentBlockSize = 0
	s.lastKey = nil
	s.dataLength = 0
	if err := s.fs.SyncDir(filepath.Dir(s.path)); err != nil {
		return fmt.Errorf("%w: segment: sync segment dir: %v", disk.ErrCommitUncertain, err)
	}
	return nil
}

// Abort closes and removes the mutable temp file.
// It is used when segment build/merge is cancelled before Freeze succeeds.
func (s *Segment) Abort() error {
	if s == nil {
		return ErrInvalidTargetBlockSize
	}
	if s.frozen {
		return ErrSegmentFrozen
	}
	if s.aborted {
		return nil
	}

	var closeErr error
	if s.file != nil {
		closeErr = s.file.Close()
		s.file = nil
	}

	var removeErr error
	if s.fs != nil && s.tempPath != "" {
		removeErr = s.fs.Remove(s.tempPath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
	}

	var syncErr error
	if s.fs != nil && s.tempPath != "" && removeErr == nil {
		syncErr = s.fs.SyncDir(filepath.Dir(s.tempPath))
	}

	s.aborted = true
	s.currentBlock = Block{}
	s.currentBlockSize = 0
	s.lastKey = nil
	s.dataLength = 0

	if closeErr != nil {
		return fmt.Errorf("segment: close temp file: %w", closeErr)
	}
	if removeErr != nil {
		return fmt.Errorf("segment: remove temp file: %w", removeErr)
	}
	if syncErr != nil {
		return fmt.Errorf("segment: sync segment dir: %w", syncErr)
	}
	return nil
}

func (s *Segment) flushBlock() error {
	if s.currentBlock.Empty() {
		return nil
	}
	if s.Index.Len() >= math.MaxUint32 {
		return ErrTooManySegmentBlocks
	}

	payload, err := s.currentBlock.MarshalBinary()
	if err != nil {
		return fmt.Errorf("segment: encode block %d: %w", s.Index.Len(), err)
	}
	s.observeBlock(payload)
	if s.compression == CompressionLZ4Dict {
		payload, err = s.dictionary.Compress(payload)
		if err != nil {
			return fmt.Errorf("segment: compress block %d: %w", s.Index.Len(), err)
		}
	}
	if len(payload) > math.MaxUint32 {
		return ErrSegmentDataTooLarge
	}

	offset := s.dataLength
	if err := disk.WriteAll(s.file, payload); err != nil {
		return fmt.Errorf("segment: write block %d: %w", s.Index.Len(), err)
	}
	s.dataLength += uint64(len(payload))
	if err := s.Index.AddBlock(s.currentBlock.Separator(), offset, uint32(len(payload))); err != nil {
		return fmt.Errorf("segment: index block %d: %w", s.Index.Len(), err)
	}

	s.currentBlock = Block{}
	s.currentBlockSize = rawBlockHeaderSize + rawBlockChecksumSize
	return nil
}

func (s *Segment) observeBlock(raw []byte) {
	if s.observeRawBlock == nil {
		return
	}
	// Sampling is optional optimization. A faulty observer must never abort a
	// segment commit or turn a successful mutation into data loss.
	defer func() { _ = recover() }()
	s.observeRawBlock(raw)
}
