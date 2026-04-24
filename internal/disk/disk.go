package disk

import (
	"errors"
	"io"
	"path/filepath"
)

// ReadFile reads the full file into memory.
func ReadFile(fs FS, name string) ([]byte, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size == 0 {
		return []byte{}, nil
	}

	buf := make([]byte, size)
	var off int64
	for off < size {
		n, err := f.ReadAt(buf[off:], off)
		off += int64(n)
		if errors.Is(err, io.EOF) && off == size {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
	}
	return buf[:off], nil
}

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

	if err := writeAll(f, data); err != nil {
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
	if err := fs.SyncDir(dir); err != nil {
		return err
	}
	ok = true
	return nil
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
