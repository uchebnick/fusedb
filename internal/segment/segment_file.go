package segment

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"fusedb/internal/disk"
)

const (
	segmentFileExt = ".seg"
	segmentTmpExt  = ".tmp"
)

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

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func writeAllAt(w io.WriterAt, off int64, data []byte) error {
	for len(data) > 0 {
		n, err := w.WriteAt(data, off)
		if err != nil {
			return err
		}
		data = data[n:]
		off += int64(n)
	}
	return nil
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
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize < headerSize+footerSize {
		return nil, ErrShortSegmentFile
	}

	headerBytes, err := readFullAt(f, 0, headerSize)
	if err != nil {
		return nil, fmt.Errorf("segment: read header: %w", err)
	}
	header, err := DecodeHeader(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("segment: decode header: %w", err)
	}

	footerBytes, err := readFullAt(f, fileSize-footerSize, footerSize)
	if err != nil {
		return nil, fmt.Errorf("segment: read footer: %w", err)
	}
	footer, err := DecodeFooter(footerBytes)
	if err != nil {
		return nil, fmt.Errorf("segment: decode footer: %w", err)
	}
	if err := validateSegmentLayout(uint64(fileSize), footer); err != nil {
		return nil, err
	}

	indexBytes, err := readSectionFrom(f, footer.Index)
	if err != nil {
		return nil, fmt.Errorf("segment: read index: %w", err)
	}
	index, err := DecodeIndex(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("segment: decode index: %w", err)
	}
	if uint32(index.Len()) != footer.BlockCount {
		return nil, ErrBlockCountMismatch
	}

	bloomBytes, err := readSectionFrom(f, footer.Bloom)
	if err != nil {
		return nil, fmt.Errorf("segment: read bloom filter: %w", err)
	}
	bloom, err := DecodeBloomFilter(bloomBytes)
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
	defer f.Close()

	return readSectionFrom(f, section)
}

func (s *Segment) readBlockPayload(entry BlockIndexEntry) ([]byte, error) {
	if uint64(entry.Offset)+uint64(entry.Length) > s.Footer.Data.Length {
		return nil, ErrBlockOutsideData
	}
	return s.readSection(Section{
		Offset: s.Footer.Data.Offset + entry.Offset,
		Length: uint64(entry.Length),
	})
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

func readSectionFrom(r io.ReaderAt, section Section) ([]byte, error) {
	if section.Length == 0 {
		return []byte{}, nil
	}
	if section.Length > uint64(maxInt()) {
		return nil, ErrSegmentSectionTooBig
	}
	if section.Offset > uint64(maxInt64()) {
		return nil, ErrSegmentSectionTooBig
	}
	return readFullAt(r, int64(section.Offset), int(section.Length))
}

func readFullAt(r io.ReaderAt, off int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	read := 0
	for read < size {
		n, err := r.ReadAt(buf[read:], off+int64(read))
		read += n
		if errors.Is(err, io.EOF) && read == size {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
	}
	if read != size {
		return nil, io.ErrUnexpectedEOF
	}
	return buf[:read], nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func maxInt64() int64 {
	return int64(^uint64(0) >> 1)
}
