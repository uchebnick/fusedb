package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/uchebnick/fusedb/internal/segment"
)

var (
	ErrSegmentVersionMismatch = errors.New("tree: segment version differs from manifest")
	ErrSegmentKeyCount        = errors.New("tree: segment key count differs from manifest")
	ErrSegmentOutsideLeaf     = errors.New("tree: segment key lies outside leaf range")
)

// VerifyReport summarizes immutable state referenced by one manifest.
type VerifyReport struct {
	Leaves     uint64
	Segments   uint64
	Blocks     uint64
	Keys       uint64
	DataBytes  uint64
	AppliedSeq uint64
}

// Verify validates the live manifest and fully reads every referenced segment.
// The structural mutex keeps the manifest snapshot stable while ordinary
// foreground reads and buffered writes remain lock-free.
func (t *Tree) Verify(ctx context.Context) (VerifyReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if t == nil || t.closed.Load() {
		return VerifyReport{}, ErrClosed
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed.Load() {
		return VerifyReport{}, ErrClosed
	}
	if err := t.manifest.Validate(); err != nil {
		return VerifyReport{}, fmt.Errorf("tree: verify manifest: %w", err)
	}

	report := VerifyReport{
		Leaves:     uint64(len(t.manifest.Leaves)),
		AppliedSeq: t.manifest.AppliedSeq,
	}
	for i, record := range t.manifest.Leaves {
		if err := ctx.Err(); err != nil {
			return VerifyReport{}, err
		}
		if record.SegmentID == 0 {
			continue
		}

		path := segment.SegmentFileName(t.dir, record.SegmentID, record.SegmentVersion)
		reader, err := segment.OpenReader(t.fs, path, t.registry)
		if err != nil {
			return VerifyReport{}, fmt.Errorf("tree: verify leaf %d open segment: %w", record.LeafID, err)
		}
		if reader.Segment().Header.Version != record.SegmentVersion {
			_ = reader.Close()
			return VerifyReport{}, fmt.Errorf("%w: leaf %d manifest=%d file=%d",
				ErrSegmentVersionMismatch, record.LeafID, record.SegmentVersion, reader.Segment().Header.Version)
		}
		segmentReport, verifyErr := reader.Verify(ctx)
		closeErr := reader.Close()
		if verifyErr != nil {
			return VerifyReport{}, fmt.Errorf("tree: verify leaf %d segment: %w", record.LeafID, verifyErr)
		}
		if closeErr != nil {
			return VerifyReport{}, fmt.Errorf("tree: verify leaf %d close segment: %w", record.LeafID, closeErr)
		}
		if segmentReport.Keys != record.Keys {
			return VerifyReport{}, fmt.Errorf("%w: leaf %d manifest=%d file=%d",
				ErrSegmentKeyCount, record.LeafID, record.Keys, segmentReport.Keys)
		}
		if segmentReport.Keys > 0 && bytes.Compare(segmentReport.FirstKey, record.LowKey) < 0 {
			return VerifyReport{}, fmt.Errorf("%w: leaf %d first key %q below %q",
				ErrSegmentOutsideLeaf, record.LeafID, segmentReport.FirstKey, record.LowKey)
		}
		if segmentReport.Keys > 0 && i+1 < len(t.manifest.Leaves) &&
			bytes.Compare(segmentReport.LastKey, t.manifest.Leaves[i+1].LowKey) >= 0 {
			return VerifyReport{}, fmt.Errorf("%w: leaf %d last key %q reaches next range %q",
				ErrSegmentOutsideLeaf, record.LeafID, segmentReport.LastKey, t.manifest.Leaves[i+1].LowKey)
		}

		report.Segments++
		report.Blocks += segmentReport.Blocks
		report.Keys += segmentReport.Keys
		report.DataBytes += segmentReport.DataBytes
	}
	return report, nil
}
