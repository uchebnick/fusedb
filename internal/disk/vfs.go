package disk

import (
	"io"
	"os"
)

// File is a readable, writable file abstraction.
// Write operations must be called sequentially.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Closer
	Sync() error
	Stat() (os.FileInfo, error)
}

// FS is the filesystem abstraction used by FuseDB.
// All disk IO goes through this interface.
// The default implementation uses the OS filesystem.
// Tests may substitute MemFS.
type FS interface {
	// Create creates the named file for reading and writing.
	// If the file already exists it is truncated.
	Create(name string) (File, error)

	// Open opens the named file for reading.
	Open(name string) (File, error)

	// OpenReadWrite opens the named file for reading and writing.
	OpenReadWrite(name string) (File, error)

	// Remove removes the named file.
	Remove(name string) error

	// Rename renames oldname to newname atomically where possible.
	Rename(oldname, newname string) error

	// SyncDir flushes directory metadata such as file create/remove/rename.
	SyncDir(dir string) error

	// MkdirAll creates the directory and all parents.
	MkdirAll(dir string) error

	// List returns the names of files in the directory.
	List(dir string) ([]string, error)

	// Stat returns info about the named file.
	Stat(name string) (os.FileInfo, error)
}
