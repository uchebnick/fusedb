package disk

import (
	"os"
	"path/filepath"
)

type osFS struct{}

// DefaultFS is the default OS-backed filesystem.
var DefaultFS FS = osFS{}

func (osFS) Create(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (osFS) Open(name string) (File, error) {
	return os.Open(name)
}

func (osFS) OpenReadWrite(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR, 0o644)
}

func (osFS) Remove(name string) error {
	return os.Remove(name)
}

func (osFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (osFS) MkdirAll(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

func (osFS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = filepath.Join(dir, e.Name())
	}
	return names, nil
}

func (osFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}
