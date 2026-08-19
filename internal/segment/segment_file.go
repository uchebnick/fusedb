package segment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
)

const (
	segmentFileExt = ".seg"
	segmentTmpExt  = ".tmp"

	blockSizeBuf = 1 << 10
)

var blockBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, DefaultTargetBlockSize+blockSizeBuf)
		return &buf
	},
}

// PooledBuffer wraps a buffer from the pool with a Release method.
// Data is a slice referencing part of the pooled buffer.
type PooledBuffer struct {
	Data   []byte
	bufPtr *[]byte
}

// Release returns the buffer to the pool.
func (pb *PooledBuffer) Release() {
	if pb != nil && pb.bufPtr != nil {
		blockBufPool.Put(pb.bufPtr)
		pb.bufPtr = nil
	}
}

var (
	ErrShortSegmentFile     = errors.New("segment: short segment file")
	ErrCorruptSegmentLayout = errors.New("segment: corrupt segment layout")
	ErrBlockCountMismatch   = errors.New("segment: block count mismatch")
	ErrSegmentNotFrozen     = errors.New("segment: segment is not frozen")
	ErrBlockOutsideData     = errors.New("segment: block points outside data section")
	ErrSegmentSectionTooBig = errors.New("segment: section too large")
)

func SegmentFileName(dir string, segmentID, version uint64) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%020d-v%020d%s", segmentID, version, segmentFileExt))
}

func SegmentTempFileName(dir string, segmentID, version uint64) string {
	return SegmentFileName(dir, segmentID, version) + segmentTmpExt
}

// ParseSegmentFileName recognizes only canonical finalized or temporary
// segment basenames. Strict reconstruction prevents startup cleanup from ever
// treating an unrelated user file as an engine-owned segment.
func ParseSegmentFileName(name string) (segmentID, version uint64, temporary, ok bool) {
	base := filepath.Base(name)
	finalBase := base
	if strings.HasSuffix(finalBase, segmentTmpExt) {
		temporary = true
		finalBase = strings.TrimSuffix(finalBase, segmentTmpExt)
	}
	if !strings.HasPrefix(finalBase, "segment-") || !strings.HasSuffix(finalBase, segmentFileExt) {
		return 0, 0, false, false
	}
	identity := strings.TrimSuffix(strings.TrimPrefix(finalBase, "segment-"), segmentFileExt)
	parts := strings.Split(identity, "-v")
	if len(parts) != 2 {
		return 0, 0, false, false
	}
	segmentID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || segmentID == 0 {
		return 0, 0, false, false
	}
	version, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil || version == 0 {
		return 0, 0, false, false
	}
	want := filepath.Base(SegmentFileName("", segmentID, version))
	if temporary {
		want += segmentTmpExt
	}
	if base != want {
		return 0, 0, false, false
	}
	return segmentID, version, temporary, true
}

// OpenSegment loads one frozen segment from a finalized file.
func OpenSegment(fs disk.FS, path string) (*Segment, error) {
	if fs == nil {
		return nil, ErrNilFilesystem
	}

	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	// Read-only handle: a failed close cannot invalidate the segment just read.
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize < headerSize+footerSize {
		return nil, ErrShortSegmentFile
	}

	headerPB, err := readFullAt(f, 0, headerSize)
	if err != nil {
		return nil, fmt.Errorf("segment: read header: %w", err)
	}
	defer headerPB.Release()
	headerCopy := make([]byte, len(headerPB.Data))
	copy(headerCopy, headerPB.Data)
	header, err := DecodeHeader(headerCopy)
	if err != nil {
		return nil, fmt.Errorf("segment: decode header: %w", err)
	}

	footerPB, err := readFullAt(f, fileSize-footerSize, footerSize)
	if err != nil {
		return nil, fmt.Errorf("segment: read footer: %w", err)
	}
	defer footerPB.Release()
	footerCopy := make([]byte, len(footerPB.Data))
	copy(footerCopy, footerPB.Data)
	footer, err := DecodeFooter(footerCopy)
	if err != nil {
		return nil, fmt.Errorf("segment: decode footer: %w", err)
	}
	if err := validateSegmentLayout(uint64(fileSize), footer); err != nil {
		return nil, err
	}
	if footer.Index.Length > limits.MaxSegmentMetadataBytes || footer.Bloom.Length > limits.MaxSegmentMetadataBytes {
		return nil, ErrSegmentSectionTooBig
	}

	indexPB, err := readSectionFrom(f, footer.Index)
	if err != nil {
		return nil, fmt.Errorf("segment: read index: %w", err)
	}
	defer indexPB.Release()
	indexCopy := make([]byte, len(indexPB.Data))
	copy(indexCopy, indexPB.Data)
	index, err := DecodeIndex(indexCopy)
	if err != nil {
		return nil, fmt.Errorf("segment: decode index: %w", err)
	}
	if uint32(index.Len()) != footer.BlockCount {
		return nil, ErrBlockCountMismatch
	}

	bloomPB, err := readSectionFrom(f, footer.Bloom)
	if err != nil {
		return nil, fmt.Errorf("segment: read bloom filter: %w", err)
	}
	defer bloomPB.Release()
	bloomCopy := make([]byte, len(bloomPB.Data))
	copy(bloomCopy, bloomPB.Data)
	bloom, err := DecodeBloomFilter(bloomCopy)
	if err != nil {
		return nil, fmt.Errorf("segment: decode bloom filter: %w", err)
	}

	return &Segment{
		Header: header,
		Footer: footer,
		Index:  index,
		Bloom:  bloom,
		frozen: true,
		fs:     fs,
		path:   path,
	}, nil
}

func (s *Segment) readSection(section Section) ([]byte, error) {
	if s == nil || !s.frozen {
		return nil, ErrSegmentNotFrozen
	}
	if s.fs == nil || s.path == "" {
		return nil, ErrNilFilesystem
	}
	if section.Length == 0 {
		return []byte{}, nil
	}

	f, err := s.fs.Open(s.path)
	if err != nil {
		return nil, err
	}
	// Read-only handle: a failed close cannot invalidate the section just read.
	defer func() { _ = f.Close() }()

	pb, err := readSectionFrom(f, section)
	if err != nil {
		return nil, err
	}
	defer pb.Release()
	result := make([]byte, len(pb.Data))
	copy(result, pb.Data)
	return result, nil
}

func (s *Segment) readBlockPayload(entry BlockIndexEntry) ([]byte, error) {
	section, err := s.blockSection(entry)
	if err != nil {
		return nil, err
	}
	return s.readSection(section)
}

func (s *Segment) blockSection(entry BlockIndexEntry) (Section, error) {
	if uint64(entry.Length) > limits.MaxEncodedBlockBytes {
		return Section{}, ErrSegmentSectionTooBig
	}
	if entry.Offset > s.Footer.Data.Length || uint64(entry.Length) > s.Footer.Data.Length-entry.Offset {
		return Section{}, ErrBlockOutsideData
	}
	return Section{
		Offset: s.Footer.Data.Offset + entry.Offset,
		Length: uint64(entry.Length),
	}, nil
}

// Remove deletes a finalized segment file.
func (s *Segment) Remove() error {
	if s == nil {
		return ErrNilSegment
	}
	if s.fs == nil || s.path == "" {
		return ErrNilFilesystem
	}
	if err := s.fs.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.fs.SyncDir(filepath.Dir(s.path))
}

func validateSegmentLayout(fileSize uint64, footer Footer) error {
	if err := footer.Validate(); err != nil {
		return fmt.Errorf("segment: validate footer: %w", err)
	}

	dataEnd, err := footer.Data.EndOffset()
	if err != nil {
		return err
	}
	bloomEnd, err := footer.Bloom.EndOffset()
	if err != nil {
		return err
	}
	indexEnd, err := footer.Index.EndOffset()
	if err != nil {
		return err
	}

	if footer.Data.Offset < headerSize {
		return ErrCorruptSegmentLayout
	}
	if footer.Bloom.Offset != dataEnd {
		return ErrCorruptSegmentLayout
	}
	if footer.Index.Offset != bloomEnd {
		return ErrCorruptSegmentLayout
	}
	if indexEnd+footerSize != fileSize {
		return ErrCorruptSegmentLayout
	}
	return nil
}

func readSectionFrom(r io.ReaderAt, section Section) (PooledBuffer, error) {
	if section.Length == 0 {
		return PooledBuffer{}, nil
	}
	if section.Length > uint64(maxInt()) {
		return PooledBuffer{}, ErrSegmentSectionTooBig
	}
	if section.Offset > uint64(maxInt64()) {
		return PooledBuffer{}, ErrSegmentSectionTooBig
	}
	return readFullAt(r, int64(section.Offset), int(section.Length))
}

func readFullAt(r io.ReaderAt, off int64, size int) (PooledBuffer, error) {
	bufPtr := blockBufPool.Get().(*[]byte)
	poolBuf := *bufPtr

	var buf []byte
	if size <= cap(poolBuf) {
		buf = poolBuf[:size]
	} else {
		// Size exceeds pool buffer (reading index/bloom, not block data)
		buf = make([]byte, size)
	}

	read := 0
	for read < size {
		n, err := r.ReadAt(buf[read:], off+int64(read))
		read += n
		if errors.Is(err, io.EOF) && read == size {
			break
		}
		if err != nil {
			blockBufPool.Put(bufPtr)
			return PooledBuffer{}, err
		}
		if n == 0 {
			break
		}
	}
	if read != size {
		blockBufPool.Put(bufPtr)
		return PooledBuffer{}, io.ErrUnexpectedEOF
	}

	return PooledBuffer{
		Data:   buf[:read],
		bufPtr: bufPtr,
	}, nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func maxInt64() int64 {
	return int64(^uint64(0) >> 1)
}
