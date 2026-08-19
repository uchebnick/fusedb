package segment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

var (
	ErrBlockLayoutGap = errors.New("segment: block index does not cover data contiguously")
	ErrEmptyBlock     = errors.New("segment: indexed block is empty")
	ErrSeparator      = errors.New("segment: block separator does not match block data")
	ErrGlobalKeyOrder = errors.New("segment: keys are not strictly ordered across blocks")
	ErrBloomFalseNeg  = errors.New("segment: bloom filter has a false negative")
)

// VerifyReport summarizes a full payload verification of one immutable
// segment. FirstKey and LastKey are detached from reader-owned buffers.
type VerifyReport struct {
	Blocks    uint64
	Keys      uint64
	DataBytes uint64
	FirstKey  []byte
	LastKey   []byte
}

// Verify reads, decompresses, and checksum-validates every block. It also
// cross-checks the index, bloom filter, and global key ordering. This is more
// expensive than OpenReader, which validates only segment metadata.
func (r *Reader) Verify(ctx context.Context) (VerifyReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !r.acquire() {
		return VerifyReport{}, ErrNilSegment
	}
	defer r.release()

	report := VerifyReport{DataBytes: r.segment.Footer.Data.Length}
	entries := r.segment.Index.Entries()
	var expectedOffset uint64
	var previousKey []byte
	for blockIndex, indexEntry := range entries {
		if err := ctx.Err(); err != nil {
			return VerifyReport{}, err
		}
		if indexEntry.Offset != expectedOffset || indexEntry.Offset > report.DataBytes ||
			uint64(indexEntry.Length) > report.DataBytes-indexEntry.Offset {
			return VerifyReport{}, fmt.Errorf("%w: block %d offset=%d expected=%d length=%d data=%d",
				ErrBlockLayoutGap, blockIndex, indexEntry.Offset, expectedOffset, indexEntry.Length, report.DataBytes)
		}

		block, err := r.readBlock(indexEntry)
		if err != nil {
			return VerifyReport{}, fmt.Errorf("segment: verify block %d: %w", blockIndex, err)
		}
		if block.Empty() {
			return VerifyReport{}, fmt.Errorf("%w: block %d", ErrEmptyBlock, blockIndex)
		}
		if !bytes.Equal(block.Separator(), indexEntry.Separator) {
			return VerifyReport{}, fmt.Errorf("%w: block %d", ErrSeparator, blockIndex)
		}

		for _, entry := range block.Entries() {
			if previousKey != nil && bytes.Compare(previousKey, entry.Key) >= 0 {
				return VerifyReport{}, fmt.Errorf("%w: block %d key %q", ErrGlobalKeyOrder, blockIndex, entry.Key)
			}
			if !r.segment.Bloom.MayContain(entry.Key) {
				return VerifyReport{}, fmt.Errorf("%w: block %d key %q", ErrBloomFalseNeg, blockIndex, entry.Key)
			}
			if report.Keys == 0 {
				report.FirstKey = bytes.Clone(entry.Key)
			}
			previousKey = entry.Key
			report.LastKey = bytes.Clone(entry.Key)
			report.Keys++
		}
		report.Blocks++
		expectedOffset = indexEntry.Offset + uint64(indexEntry.Length)
	}
	if expectedOffset != report.DataBytes {
		return VerifyReport{}, fmt.Errorf("%w: covered=%d data=%d", ErrBlockLayoutGap, expectedOffset, report.DataBytes)
	}
	return report, nil
}
