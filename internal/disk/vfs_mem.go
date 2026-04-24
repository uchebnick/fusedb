package disk

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type memNode struct {
	mu   sync.Mutex
	data []byte
	name string
}

type memFile struct {
	node    *memNode
	readOff int64
}

func (f *memFile) Read(p []byte) (int, error) {
	f.node.mu.Lock()
	defer f.node.mu.Unlock()

	if f.readOff >= int64(len(f.node.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.node.data[f.readOff:])
	f.readOff += int64(n)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.node.mu.Lock()
	defer f.node.mu.Unlock()

	if off >= int64(len(f.node.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.node.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return f.WriteAt(p, info.Size())
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.node.mu.Lock()
	defer f.node.mu.Unlock()

	end := int(off) + len(p)
	if end > len(f.node.data) {
		grown := make([]byte, end)
		copy(grown, f.node.data)
		f.node.data = grown
	}
	copy(f.node.data[off:end], p)
	return len(p), nil
}

func (f *memFile) Close() error { return nil }
func (f *memFile) Sync() error  { return nil }
func (f *memFile) Stat() (os.FileInfo, error) {
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	return &memFileInfo{name: f.node.name, size: int64(len(f.node.data))}, nil
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
	files map[string]*memNode
}

// NewMemFS returns an in-memory FS for use in tests.
func NewMemFS() FS {
	return &memFS{files: make(map[string]*memNode)}
}

func (m *memFS) create(name string) (File, error) {
	node := &memNode{name: name}
	m.files[name] = node
	return &memFile{node: node}, nil
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
		return nil, fmt.Errorf("file not found: %s: %w", name, os.ErrNotExist)
	}
	return &memFile{node: f}, nil
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
		return fmt.Errorf("file not found: %s: %w", oldname, os.ErrNotExist)
	}
	delete(m.files, oldname)
	f.name = newname
	m.files[newname] = f
	return nil
}

func (m *memFS) SyncDir(_ string) error { return nil }

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
		return nil, fmt.Errorf("file not found: %s: %w", name, os.ErrNotExist)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return &memFileInfo{name: f.name, size: int64(len(f.data))}, nil
}
