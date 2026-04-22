package disk

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"
)

type memFile struct {
	mu   sync.Mutex
	buf  *bytes.Buffer
	name string
}

func (f *memFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Read(p)
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.buf.Bytes()
	if int(off) >= len(b) {
		return 0, fmt.Errorf("offset out of range")
	}
	n := copy(p, b[off:])
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(p)
}

func (f *memFile) Close() error { return nil }
func (f *memFile) Sync() error  { return nil }
func (f *memFile) Stat() (os.FileInfo, error) {
	return &memFileInfo{name: f.name, size: int64(f.buf.Len())}, nil
}

type memFileInfo struct {
	name string
	size int64
}

func (i *memFileInfo) Name() string       { return i.name }
func (i *memFileInfo) Size() int64        { return i.size }
func (i *memFileInfo) Mode() os.FileMode  { return 0o644 }
func (i *memFileInfo) ModTime() time.Time { return time.Time{} }
func (i *memFileInfo) IsDir() bool        { return false }
func (i *memFileInfo) Sys() any           { return nil }

type memFS struct {
	mu    sync.Mutex
	files map[string]*memFile
}

// NewMemFS returns an in-memory FS for use in tests.
func NewMemFS() FS {
	return &memFS{files: make(map[string]*memFile)}
}

func (m *memFS) create(name string) (File, error) {
	f := &memFile{buf: &bytes.Buffer{}, name: name}
	m.files[name] = f
	return f, nil
}

func (m *memFS) Create(name string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.create(name)
}

func (m *memFS) Open(name string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[name]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", name)
	}
	return f, nil
}

func (m *memFS) OpenReadWrite(name string) (File, error) {
	return m.Open(name)
}

func (m *memFS) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)
	return nil
}

func (m *memFS) Rename(oldname, newname string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[oldname]
	if !ok {
		return fmt.Errorf("file not found: %s", oldname)
	}
	delete(m.files, oldname)
	f.name = newname
	m.files[newname] = f
	return nil
}

func (m *memFS) MkdirAll(_ string) error { return nil }

func (m *memFS) List(_ string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.files))
	for name := range m.files {
		names = append(names, name)
	}
	return names, nil
}

func (m *memFS) Stat(name string) (os.FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[name]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", name)
	}
	return &memFileInfo{name: f.name, size: int64(f.buf.Len())}, nil
}
