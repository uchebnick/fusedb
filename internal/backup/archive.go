// Package backup owns the portable, checksummed FuseDB backup archive format.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"path"
	"path/filepath"
	"strings"

	"github.com/uchebnick/fusedb/internal/disk"
)

const (
	archiveMagic      = "FBAK"
	archiveVersion    = 1
	archiveHeaderSize = 4 + 4 + 4
	entryHeaderSize   = 4 + 8
	entryChecksumSize = 4
	archiveFooterSize = sha256.Size
	copyBufferSize    = 64 << 10
	maxEntryNameBytes = 4 << 10
	maxArchiveEntries = 1 << 20
)

var (
	ErrInvalidArchive      = errors.New("backup: invalid archive")
	ErrArchiveChecksum     = errors.New("backup: archive checksum mismatch")
	ErrEntryChecksum       = fmt.Errorf("%w: entry checksum mismatch", ErrArchiveChecksum)
	ErrUnsafeEntryName     = fmt.Errorf("%w: unsafe entry name", ErrInvalidArchive)
	ErrDuplicateEntry      = fmt.Errorf("%w: duplicate entry", ErrInvalidArchive)
	ErrDestinationNotEmpty = errors.New("backup: restore destination is not empty")
	ErrMissingManifest     = fmt.Errorf("%w: archive has no manifest", ErrInvalidArchive)
	ErrMissingWAL          = fmt.Errorf("%w: archive has no WAL", ErrInvalidArchive)
)

// Source describes one archive entry. Exactly one of Path or Data is used;
// Data takes precedence and may intentionally be empty.
type Source struct {
	Name string
	Path string
	Data []byte
}

// Entry is one verified file payload inside an archive.
type Entry struct {
	Name   string
	Offset int64
	Size   uint64
	CRC32  uint32
}

// Archive is the immutable index returned after a complete verification pass.
type Archive struct {
	Entries []Entry
	Bytes   uint64
}

// Write streams sources into an atomically replaced archive. Memory use is
// bounded by one copy buffer regardless of database size.
func Write(ctx context.Context, fs disk.FS, archivePath string, sources []Source) (Archive, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if fs == nil || archivePath == "" || len(sources) == 0 || len(sources) > maxArchiveEntries {
		return Archive{}, ErrInvalidArchive
	}
	seen := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if err := validateEntryName(source.Name); err != nil {
			return Archive{}, err
		}
		if _, duplicate := seen[source.Name]; duplicate {
			return Archive{}, fmt.Errorf("%w: %s", ErrDuplicateEntry, source.Name)
		}
		seen[source.Name] = struct{}{}
	}

	dir := filepath.Dir(archivePath)
	if err := fs.MkdirAll(dir); err != nil {
		return Archive{}, err
	}
	lock, err := fs.Lock(archivePath + ".lock")
	if err != nil {
		return Archive{}, fmt.Errorf("backup: lock archive: %w", err)
	}
	defer func() { _ = lock.Close() }()
	tmpPath := archivePath + ".tmp"
	file, err := fs.Create(tmpPath)
	if err != nil {
		return Archive{}, err
	}
	renamed := false
	defer func() {
		_ = file.Close()
		if !renamed {
			_ = fs.Remove(tmpPath)
		}
	}()

	digest := sha256.New()
	writer := &digestWriter{file: file, digest: digest}
	header := make([]byte, archiveHeaderSize)
	copy(header[:4], archiveMagic)
	binary.LittleEndian.PutUint32(header[4:8], archiveVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(sources)))
	if err := writer.write(header); err != nil {
		return Archive{}, err
	}

	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return Archive{}, err
		}
		_, err := writeSource(ctx, fs, writer, source)
		if err != nil {
			return Archive{}, fmt.Errorf("backup: write %s: %w", source.Name, err)
		}
	}
	if err := disk.WriteAll(file, digest.Sum(nil)); err != nil {
		return Archive{}, err
	}
	if err := file.Sync(); err != nil {
		return Archive{}, err
	}
	if err := file.Close(); err != nil {
		return Archive{}, err
	}
	if err := fs.Rename(tmpPath, archivePath); err != nil {
		return Archive{}, err
	}
	renamed = true
	if err := fs.SyncDir(dir); err != nil {
		return Archive{}, fmt.Errorf("%w: backup: sync archive directory: %v", disk.ErrCommitUncertain, err)
	}
	verified, err := Inspect(ctx, fs, archivePath)
	if err != nil {
		return Archive{}, fmt.Errorf("backup: verify published archive: %w", err)
	}
	return verified, nil
}

func writeSource(ctx context.Context, fs disk.FS, writer *digestWriter, source Source) (Entry, error) {
	var (
		reader io.ReaderAt
		size   uint64
		close  func()
	)
	if source.Data != nil {
		reader = bytesReaderAt(source.Data)
		size = uint64(len(source.Data))
		close = func() {}
	} else {
		file, err := fs.Open(source.Path)
		if err != nil {
			return Entry{}, err
		}
		close = func() { _ = file.Close() }
		info, err := file.Stat()
		if err != nil {
			close()
			return Entry{}, err
		}
		if info.Size() < 0 {
			close()
			return Entry{}, ErrInvalidArchive
		}
		reader = file
		size = uint64(info.Size())
	}
	defer close()
	if size > math.MaxInt64 {
		return Entry{}, ErrInvalidArchive
	}

	entryHeader := make([]byte, entryHeaderSize)
	binary.LittleEndian.PutUint32(entryHeader[:4], uint32(len(source.Name)))
	binary.LittleEndian.PutUint64(entryHeader[4:12], size)
	if err := writer.write(entryHeader); err != nil {
		return Entry{}, err
	}
	if err := writer.write([]byte(source.Name)); err != nil {
		return Entry{}, err
	}
	dataOffset := writer.offset
	checksum := crc32.NewIEEE()
	if err := copyReaderAt(ctx, writer, checksum, reader, 0, int64(size)); err != nil {
		return Entry{}, err
	}
	checksumBytes := make([]byte, entryChecksumSize)
	binary.LittleEndian.PutUint32(checksumBytes, checksum.Sum32())
	if err := writer.write(checksumBytes); err != nil {
		return Entry{}, err
	}
	return Entry{Name: source.Name, Offset: dataOffset, Size: size, CRC32: checksum.Sum32()}, nil
}

// Inspect verifies the complete archive, every entry CRC, all names, and the
// archive SHA-256 footer before returning offsets suitable for extraction.
func Inspect(ctx context.Context, fs disk.FS, archivePath string) (Archive, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if fs == nil || archivePath == "" {
		return Archive{}, ErrInvalidArchive
	}
	file, err := fs.Open(archivePath)
	if err != nil {
		return Archive{}, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return Archive{}, err
	}
	if info.Size() < archiveHeaderSize+archiveFooterSize {
		return Archive{}, ErrInvalidArchive
	}
	dataEnd := info.Size() - archiveFooterSize
	digest := sha256.New()
	offset := int64(0)
	header, err := readAndHash(file, digest, &offset, archiveHeaderSize, dataEnd)
	if err != nil {
		return Archive{}, err
	}
	if string(header[:4]) != archiveMagic || binary.LittleEndian.Uint32(header[4:8]) != archiveVersion {
		return Archive{}, ErrInvalidArchive
	}
	count := int(binary.LittleEndian.Uint32(header[8:12]))
	if count <= 0 || count > maxArchiveEntries {
		return Archive{}, ErrInvalidArchive
	}

	entries := make([]Entry, 0, count)
	seen := make(map[string]struct{}, count)
	buffer := make([]byte, copyBufferSize)
	for range count {
		if err := ctx.Err(); err != nil {
			return Archive{}, err
		}
		entryHeader, err := readAndHash(file, digest, &offset, entryHeaderSize, dataEnd)
		if err != nil {
			return Archive{}, err
		}
		nameLen := int(binary.LittleEndian.Uint32(entryHeader[:4]))
		size := binary.LittleEndian.Uint64(entryHeader[4:12])
		if nameLen <= 0 || nameLen > maxEntryNameBytes || size > math.MaxInt64 {
			return Archive{}, ErrInvalidArchive
		}
		nameBytes, err := readAndHash(file, digest, &offset, nameLen, dataEnd)
		if err != nil {
			return Archive{}, err
		}
		name := string(nameBytes)
		if err := validateEntryName(name); err != nil {
			return Archive{}, err
		}
		if _, duplicate := seen[name]; duplicate {
			return Archive{}, fmt.Errorf("%w: %s", ErrDuplicateEntry, name)
		}
		seen[name] = struct{}{}
		if uint64(dataEnd-offset) < size+entryChecksumSize {
			return Archive{}, ErrInvalidArchive
		}
		dataOffset := offset
		checksum := crc32.NewIEEE()
		remaining := int64(size)
		for remaining > 0 {
			if err := ctx.Err(); err != nil {
				return Archive{}, err
			}
			n := int64(len(buffer))
			if remaining < n {
				n = remaining
			}
			chunk := buffer[:n]
			if err := readFullAt(file, chunk, offset); err != nil {
				return Archive{}, err
			}
			_, _ = digest.Write(chunk)
			_, _ = checksum.Write(chunk)
			offset += n
			remaining -= n
		}
		crcBytes, err := readAndHash(file, digest, &offset, entryChecksumSize, dataEnd)
		if err != nil {
			return Archive{}, err
		}
		wantCRC := binary.LittleEndian.Uint32(crcBytes)
		if checksum.Sum32() != wantCRC {
			return Archive{}, fmt.Errorf("%w: %s", ErrEntryChecksum, name)
		}
		entries = append(entries, Entry{Name: name, Offset: dataOffset, Size: size, CRC32: wantCRC})
	}
	if offset != dataEnd {
		return Archive{}, ErrInvalidArchive
	}
	wantDigest := make([]byte, archiveFooterSize)
	if err := readFullAt(file, wantDigest, dataEnd); err != nil {
		return Archive{}, err
	}
	if !equalBytes(digest.Sum(nil), wantDigest) {
		return Archive{}, ErrArchiveChecksum
	}
	return Archive{Entries: entries, Bytes: uint64(info.Size())}, nil
}

// Restore verifies archivePath first, then extracts into an empty destination.
// MANIFEST is installed last, so an interrupted pre-commit restore is never an
// apparently valid database.
func Restore(ctx context.Context, fs disk.FS, archivePath, destination string) (Archive, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	verified, err := Inspect(ctx, fs, archivePath)
	if err != nil {
		return Archive{}, err
	}
	if destination == "" {
		return Archive{}, ErrInvalidArchive
	}
	if err := fs.MkdirAll(destination); err != nil {
		return Archive{}, err
	}
	lock, err := fs.Lock(filepath.Join(destination, "LOCK"))
	if err != nil {
		return Archive{}, err
	}
	defer func() { _ = lock.Close() }()
	files, err := fs.List(destination)
	if err != nil {
		return Archive{}, err
	}
	for _, name := range files {
		if filepath.Base(name) != "LOCK" {
			return Archive{}, ErrDestinationNotEmpty
		}
	}

	var manifestEntry *Entry
	ordered := make([]Entry, 0, len(verified.Entries))
	hasWAL := false
	for i := range verified.Entries {
		entry := verified.Entries[i]
		switch entry.Name {
		case "MANIFEST":
			copy := entry
			manifestEntry = &copy
		case "wal.log":
			hasWAL = true
			ordered = append(ordered, entry)
		default:
			ordered = append(ordered, entry)
		}
	}
	if manifestEntry == nil {
		return Archive{}, ErrMissingManifest
	}
	if !hasWAL {
		return Archive{}, ErrMissingWAL
	}
	ordered = append(ordered, *manifestEntry)

	archiveFile, err := fs.Open(archivePath)
	if err != nil {
		return Archive{}, err
	}
	defer func() { _ = archiveFile.Close() }()
	created := make([]string, 0, len(ordered))
	committed := false
	defer func() {
		if committed {
			return
		}
		for _, name := range created {
			_ = fs.Remove(name)
		}
	}()
	for _, entry := range ordered {
		if err := ctx.Err(); err != nil {
			return Archive{}, err
		}
		target := filepath.Join(destination, filepath.FromSlash(entry.Name))
		if err := writeEntryAtomically(ctx, fs, archiveFile, target, entry); err != nil {
			if errors.Is(err, disk.ErrCommitUncertain) {
				// The renamed target is visible. Preserve all dependencies and
				// require the operator to inspect or remove this destination.
				committed = true
			}
			return Archive{}, fmt.Errorf("backup: restore %s: %w", entry.Name, err)
		}
		created = append(created, target)
	}
	committed = true
	return verified, nil
}

func writeEntryAtomically(ctx context.Context, fs disk.FS, archive io.ReaderAt, target string, entry Entry) error {
	dir := filepath.Dir(target)
	if err := fs.MkdirAll(dir); err != nil {
		return err
	}
	tmp := target + ".restore.tmp"
	file, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		_ = file.Close()
		if !renamed {
			_ = fs.Remove(tmp)
		}
	}()
	checksum := crc32.NewIEEE()
	if err := copyReaderAt(ctx, file, checksum, archive, entry.Offset, int64(entry.Size)); err != nil {
		return err
	}
	if checksum.Sum32() != entry.CRC32 {
		return fmt.Errorf("%w: %s", ErrEntryChecksum, entry.Name)
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmp, target); err != nil {
		return err
	}
	renamed = true
	if err := fs.SyncDir(dir); err != nil {
		return fmt.Errorf("%w: backup: sync restored directory: %v", disk.ErrCommitUncertain, err)
	}
	return nil
}

func validateEntryName(name string) error {
	if name == "" || len(name) > maxEntryNameBytes || strings.Contains(name, "\\") ||
		path.IsAbs(name) || path.Clean(name) != name || name == "." || name == ".." || strings.HasPrefix(name, "../") {
		return fmt.Errorf("%w: %q", ErrUnsafeEntryName, name)
	}
	return nil
}

type digestWriter struct {
	file   disk.File
	digest hash.Hash
	offset int64
}

func (w *digestWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrShortWrite
	}
	if n > 0 {
		_, _ = w.digest.Write(p[:n])
		w.offset += int64(n)
	}
	return n, err
}

func (w *digestWriter) write(p []byte) error { return disk.WriteAll(w, p) }

type sliceReaderAt []byte

func bytesReaderAt(data []byte) io.ReaderAt { return sliceReaderAt(data) }

func (b sliceReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(b)) {
		if off == int64(len(b)) && len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func copyReaderAt(ctx context.Context, dst io.Writer, checksum hash.Hash, src io.ReaderAt, offset, size int64) error {
	buffer := make([]byte, copyBufferSize)
	remaining := size
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := int64(len(buffer))
		if remaining < n {
			n = remaining
		}
		chunk := buffer[:n]
		if err := readFullAt(src, chunk, offset); err != nil {
			return err
		}
		if err := disk.WriteAll(dst, chunk); err != nil {
			return err
		}
		if checksum != nil {
			_, _ = checksum.Write(chunk)
		}
		offset += n
		remaining -= n
	}
	return nil
}

func readAndHash(src io.ReaderAt, digest hash.Hash, offset *int64, size int, limit int64) ([]byte, error) {
	if size < 0 || int64(size) > limit-*offset {
		return nil, ErrInvalidArchive
	}
	buf := make([]byte, size)
	if err := readFullAt(src, buf, *offset); err != nil {
		return nil, err
	}
	_, _ = digest.Write(buf)
	*offset += int64(size)
	return buf, nil
}

func readFullAt(src io.ReaderAt, p []byte, offset int64) error {
	for len(p) > 0 {
		n, err := src.ReadAt(p, offset)
		offset += int64(n)
		p = p[n:]
		if err != nil {
			if errors.Is(err, io.EOF) && len(p) == 0 {
				return nil
			}
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var different byte
	for i := range a {
		different |= a[i] ^ b[i]
	}
	return different == 0
}
