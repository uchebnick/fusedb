package disk

import (
	"errors"
	"io"
	"os"
)

var (
	// ErrLocked reports that another database instance owns the requested lock.
	ErrLocked = errors.New("disk: lock already held")
	// ErrLockUnsupported reports that the platform cannot provide a
	// process-crash-safe advisory file lock.
	ErrLockUnsupported = errors.New("disk: file locking is unsupported")
	// ErrCommitUncertain reports that an atomic rename became visible but the
	// parent directory could not be synced. Callers must not roll the visible
	// target back or delete files it references; crash durability is unknown.
	ErrCommitUncertain = errors.New("disk: atomic replacement commit is uncertain")
)

// Lock is an exclusive advisory filesystem lock. Closing releases it; the
// lock is also released by the operating system when the process exits.
type Lock interface {
	io.Closer
}

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
	// Lock acquires a non-blocking exclusive lock associated with name.
	Lock(name string) (Lock, error)

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
