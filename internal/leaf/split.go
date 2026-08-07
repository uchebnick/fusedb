package leaf

import (
	"bytes"
	"fmt"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/segment"
)

// SplitOptions configures a merge that may split its output.
type SplitOptions struct {
	// Base carries the shared segment settings. SegmentID and Version are
	// ignored: every output segment gets its own identity from Allocate.
	Base segment.Options

	// MaxBytes is the payload size at which the merge starts a new output
	// segment. Zero merges into a single segment regardless of size.
	//
	// The cut lands on a key boundary, so an output can overshoot MaxBytes by
	// at most one entry.
	MaxBytes int64

	// Allocate returns the identity of the next output segment. It is called
	// once per output, in order.
	Allocate func(index int) (segmentID, version uint64, err error)
}

// MergeResult describes one segment produced by MergeSplit.
type MergeResult struct {
	// LowKey is the inclusive lower bound of the range this output covers.
	// The first output inherits the lower bound of the merged leaf.
	LowKey []byte

	Segment *segment.Segment
	Reader  *segment.Reader

	Keys  int64
	Bytes int64
}

// MergeSplit rewrites the leaf segment together with its buffered operations,
// cutting the result into one or more segments.
//
// Cutting on accumulated bytes rather than on a precomputed median is what
// keeps this single-pass: the source is read once, and a new output starts as
// soon as the current one is full. The caller is responsible for installing the
// results into the tree and for retiring this leaf.
//
// On any failure every output produced so far is removed, so a failed merge
// leaves no partial segments behind.
func (l *Leaf) MergeSplit(opts SplitOptions) ([]MergeResult, error) {
	if l == nil || l.merger == nil {
		return nil, nil
	}
	if opts.Allocate == nil {
		return nil, fmt.Errorf("leaf: merge split requires an allocator")
	}

	// A false result means an earlier merge failed after freezing. Reusing that
	// frozen layer is required for progress.
	l.buffer.Freeze()

	segmentIter := emptySegmentIter
	segmentErr := func() error { return nil }
	if reader := l.reader.Load(); reader != nil {
		segmentIter, segmentErr = reader.IterWithErr()
	}

	// Sizing every output for the full merged key count keeps bloom filters on
	// the safe side of the target false positive rate. Sizing them for the
	// buffer alone, as the single-segment path used to, degrades the filter a
	// little more with every merge.
	expectedKeys := l.segmentKeys.Load() + l.buffer.FrozenLen()
	if expectedKeys < 0 {
		expectedKeys = 0
	}

	builder := &splitBuilder{
		opts:         opts,
		expectedKeys: expectedKeys,
		firstLowKey:  l.lowKey,
		compression:  l.merger.Compression,
	}

	stream := l.merger.MergeIter(segmentIter, l.buffer.IterFrozen())
	for key, value := range stream {
		if err := builder.add(key, value); err != nil {
			builder.cleanup()
			return nil, err
		}
	}

	if err := segmentErr(); err != nil {
		builder.cleanup()
		return nil, fmt.Errorf("leaf: merge source segment: %w", err)
	}

	results, err := builder.finish()
	if err != nil {
		builder.cleanup()
		return nil, err
	}
	return results, nil
}

// splitBuilder streams merged entries into a sequence of output segments.
type splitBuilder struct {
	opts         SplitOptions
	expectedKeys int64
	firstLowKey  []byte
	compression  *compression.Registry

	results []MergeResult

	current      *segment.Segment
	currentLow   []byte
	currentKeys  int64
	currentBytes int64
	nextLow      []byte
}

func (b *splitBuilder) add(key, value []byte) error {
	if b.current != nil && b.opts.MaxBytes > 0 && b.currentBytes >= b.opts.MaxBytes {
		if err := b.closeCurrent(); err != nil {
			return err
		}
		// The cut boundary is the first key of the next output, which becomes
		// that leaf's inclusive lower bound.
		b.nextLow = bytes.Clone(key)
	}

	if b.current == nil {
		if err := b.openNext(); err != nil {
			return err
		}
	}

	if err := b.current.AppendKVUnsafe(key, value); err != nil {
		return err
	}
	b.currentKeys++
	b.currentBytes += int64(len(key) + len(value))
	return nil
}

func (b *splitBuilder) openNext() error {
	index := len(b.results)
	segmentID, version, err := b.opts.Allocate(index)
	if err != nil {
		return err
	}

	opts := b.opts.Base
	opts.SegmentID = segmentID
	opts.Version = version
	opts.ExpectedKeys = int(b.expectedKeys)

	created, err := segment.NewSegment(opts)
	if err != nil {
		return fmt.Errorf("leaf: create output segment %d: %w", index, err)
	}

	b.current = created
	b.currentKeys = 0
	b.currentBytes = 0
	if index == 0 {
		b.currentLow = bytes.Clone(b.firstLowKey)
	} else {
		b.currentLow = b.nextLow
	}
	return nil
}

func (b *splitBuilder) closeCurrent() error {
	if b.current == nil {
		return nil
	}

	if err := b.current.Freeze(); err != nil {
		_ = b.current.Abort()
		b.current = nil
		return fmt.Errorf("leaf: freeze output segment %d: %w", len(b.results), err)
	}

	reader, err := segment.NewReader(b.current, b.compression)
	if err != nil {
		frozen := b.current
		b.current = nil
		_ = frozen.Remove()
		return fmt.Errorf("leaf: open output segment %d: %w", len(b.results), err)
	}

	b.results = append(b.results, MergeResult{
		LowKey:  b.currentLow,
		Segment: b.current,
		Reader:  reader,
		Keys:    b.currentKeys,
		Bytes:   b.currentBytes,
	})
	b.current = nil
	return nil
}

func (b *splitBuilder) finish() ([]MergeResult, error) {
	if err := b.closeCurrent(); err != nil {
		return nil, err
	}
	return b.results, nil
}

// cleanup discards every output built so far.
//
// A failed merge must not leave finalized segment files behind: they carry
// segment ids the manifest never learned about, and a later merge reusing an
// id would refuse to overwrite them.
func (b *splitBuilder) cleanup() {
	if b.current != nil {
		_ = b.current.Abort()
		b.current = nil
	}
	for _, result := range b.results {
		if result.Reader != nil {
			_ = result.Reader.Close()
		}
		if result.Segment != nil {
			_ = result.Segment.Remove()
		}
	}
	b.results = nil
}
