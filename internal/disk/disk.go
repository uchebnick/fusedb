package disk

import (
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
)

var (
	// ErrFileTooLarge reports that a file exceeds the caller's allocation
	// budget. Decoders use this to fail closed before trusting an on-disk size.
	ErrFileTooLarge = errors.New("disk: file exceeds read limit")
	// ErrInvalidReadLimit reports a negative ReadFileLimited budget.
	ErrInvalidReadLimit = errors.New("disk: invalid file read limit")
)

// ReadFile reads the full file into memory.
func ReadFile(fs FS, name string) ([]byte, error) {
	return ReadFileLimited(fs, name, math.MaxInt64)
}

// ReadFileLimited reads the full file only when its size is at most maxBytes.
// It validates the stat result before converting it to int or allocating.
func ReadFileLimited(fs FS, name string, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, ErrInvalidReadLimit
	}
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	// Read-only handle: a failed close cannot invalidate bytes already read.
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size < 0 || size > maxBytes || uint64(size) > uint64(maxIntValue()) {
		return nil, fmt.Errorf("%w: %s has %d bytes (limit %d)", ErrFileTooLarge, name, size, maxBytes)
	}
	if size == 0 {
		return []byte{}, nil
	}

	buf := make([]byte, int(size))
	var off int64
	for off < size {
		n, err := f.ReadAt(buf[off:], off)
		if n < 0 || int64(n) > size-off {
			return nil, io.ErrUnexpectedEOF
		}
		off += int64(n)
		if errors.Is(err, io.EOF) && off == size {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrUnexpectedEOF
		}
	}
	return buf, nil
}

func maxIntValue() int { return int(^uint(0) >> 1) }

// WriteFileAtomically writes file contents through a temporary file and renames
// it into place once synced.
func WriteFileAtomically(fs FS, name string, data []byte) error {
	dir := filepath.Dir(name)
	if dir != "." && dir != "" {
		if err := fs.MkdirAll(dir); err != nil {
			return err
		}
	}

	tmpName := name + ".tmp"
	f, err := fs.Create(tmpName)
	if err != nil {
		return err
	}

	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = fs.Remove(tmpName)
		}
	}()

	if err := WriteAll(f, data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmpName, name); err != nil {
		return err
	}
	// From this point the target names the complete new file. Even if syncing
	// the directory fails, rollback cleanup must not treat the temp name as
	// authoritative or let callers delete dependencies of the visible target.
	ok = true
	if err := fs.SyncDir(dir); err != nil {
		return fmt.Errorf("%w: %v", ErrCommitUncertain, err)
	}
	return nil
}

// WriteAll writes the complete buffer or returns an error. It tolerates
// progress-making short writes even though io.Writer implementations should
// normally return a non-nil error in that case, and rejects zero progress or
// impossible byte counts without looping or slicing out of bounds.
func WriteAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// WriteAllAt writes the complete buffer beginning at offset. It returns the
// number of bytes accepted before failure so commit code can distinguish a
// definite pre-write failure from a possibly visible partial write.
func WriteAllAt(w io.WriterAt, data []byte, offset int64) (int, error) {
	written := 0
	for len(data) > 0 {
		n, err := w.WriteAt(data, offset+int64(written))
		if n < 0 || n > len(data) {
			return written, io.ErrShortWrite
		}
		written += n
		data = data[n:]
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
