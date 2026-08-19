// Package faultfs provides deterministic filesystem fault injection for
// storage-engine tests. It deliberately wraps disk.FS rather than reaching for
// the OS, so the same campaign can run quickly against MemFS and selectively
// against a real temporary directory.
package faultfs

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/uchebnick/fusedb/internal/disk"
)

// Operation identifies one filesystem boundary where a rule may fire.
type Operation string

const (
	OpLock          Operation = "lock"
	OpCreate        Operation = "create"
	OpOpen          Operation = "open"
	OpOpenReadWrite Operation = "open-read-write"
	OpRemove        Operation = "remove"
	OpRename        Operation = "rename"
	OpSyncDir       Operation = "sync-dir"
	OpMkdirAll      Operation = "mkdir-all"
	OpList          Operation = "list"
	OpStat          Operation = "stat"
	OpRead          Operation = "read"
	OpReadAt        Operation = "read-at"
	OpWrite         Operation = "write"
	OpWriteAt       Operation = "write-at"
	OpSync          Operation = "sync"
	OpClose         Operation = "close"
)

// ErrInjected is used when a rule does not provide a more specific error.
var ErrInjected = errors.New("faultfs: injected filesystem failure")

// Rule describes a one-shot failure. At is the one-based matching call number;
// zero means the first. PathSuffix narrows the rule to a path suffix.
//
// MaxBytes applies to read/write calls. A positive value delegates only that
// prefix before returning Err (or nil for a progress-making short operation).
// ZeroProgress returns (0, nil), exercising defensive loop handling. After
// asks non-stream operations and Close/Sync to call the underlying method first
// and replace a successful result with Err.
type Rule struct {
	Operation    Operation
	PathSuffix   string
	At           int
	Err          error
	MaxBytes     int
	ZeroProgress bool
	After        bool
}

// Event records an observed filesystem operation.
type Event struct {
	Operation Operation
	Path      string
}

// FS wraps a filesystem with one armable one-shot rule.
type FS struct {
	disk.FS

	mu      sync.Mutex
	rule    Rule
	armed   bool
	seen    int
	fired   bool
	history []Event
}

var _ disk.FS = (*FS)(nil)

// New returns a disarmed fault-injection wrapper.
func New(base disk.FS) *FS {
	if base == nil {
		base = disk.NewMemFS()
	}
	return &FS{FS: base}
}

// Arm installs a new one-shot rule and resets its counters.
func (f *FS) Arm(rule Rule) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rule.At <= 0 {
		rule.At = 1
	}
	if rule.Err == nil && !rule.ZeroProgress && rule.MaxBytes == 0 {
		rule.Err = ErrInjected
	}
	f.rule = rule
	f.armed = true
	f.seen = 0
	f.fired = false
}

// Disarm removes the active rule without clearing operation history.
func (f *FS) Disarm() {
	f.mu.Lock()
	f.armed = false
	f.mu.Unlock()
}

// Fired reports whether the active rule has fired.
func (f *FS) Fired() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fired
}

// Seen reports how many operations matched the active rule.
func (f *FS) Seen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen
}

// History returns a detached operation trace.
func (f *FS) History() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Event(nil), f.history...)
}

func (f *FS) action(operation Operation, path string) (Rule, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.history = append(f.history, Event{Operation: operation, Path: path})
	if !f.armed || f.fired || f.rule.Operation != operation ||
		(f.rule.PathSuffix != "" && !strings.HasSuffix(path, f.rule.PathSuffix)) {
		return Rule{}, false
	}
	f.seen++
	if f.seen != f.rule.At {
		return Rule{}, false
	}
	f.fired = true
	return f.rule, true
}

func ruleError(rule Rule) error {
	if rule.Err != nil {
		return rule.Err
	}
	return ErrInjected
}

func (f *FS) Lock(name string) (disk.Lock, error) {
	rule, fire := f.action(OpLock, name)
	if fire && !rule.After {
		return nil, ruleError(rule)
	}
	lock, err := f.FS.Lock(name)
	if err == nil && fire {
		_ = lock.Close()
		return nil, ruleError(rule)
	}
	return lock, err
}

func (f *FS) Create(name string) (disk.File, error) {
	rule, fire := f.action(OpCreate, name)
	if fire && !rule.After {
		return nil, ruleError(rule)
	}
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	if fire {
		_ = file.Close()
		return nil, ruleError(rule)
	}
	return &fileWrapper{File: file, fs: f, path: name}, nil
}

func (f *FS) Open(name string) (disk.File, error) {
	rule, fire := f.action(OpOpen, name)
	if fire && !rule.After {
		return nil, ruleError(rule)
	}
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	if fire {
		_ = file.Close()
		return nil, ruleError(rule)
	}
	return &fileWrapper{File: file, fs: f, path: name}, nil
}

func (f *FS) OpenReadWrite(name string) (disk.File, error) {
	rule, fire := f.action(OpOpenReadWrite, name)
	if fire && !rule.After {
		return nil, ruleError(rule)
	}
	file, err := f.FS.OpenReadWrite(name)
	if err != nil {
		return nil, err
	}
	if fire {
		_ = file.Close()
		return nil, ruleError(rule)
	}
	return &fileWrapper{File: file, fs: f, path: name}, nil
}

func (f *FS) Remove(name string) error {
	return f.simple(OpRemove, name, func() error { return f.FS.Remove(name) })
}

func (f *FS) Rename(oldname, newname string) error {
	return f.simple(OpRename, newname, func() error { return f.FS.Rename(oldname, newname) })
}

func (f *FS) SyncDir(dir string) error {
	return f.simple(OpSyncDir, dir, func() error { return f.FS.SyncDir(dir) })
}

func (f *FS) MkdirAll(dir string) error {
	return f.simple(OpMkdirAll, dir, func() error { return f.FS.MkdirAll(dir) })
}

func (f *FS) List(dir string) ([]string, error) {
	rule, fire := f.action(OpList, dir)
	if fire && !rule.After {
		return nil, ruleError(rule)
	}
	names, err := f.FS.List(dir)
	if err == nil && fire {
		return nil, ruleError(rule)
	}
	return names, err
}

func (f *FS) Stat(name string) (os.FileInfo, error) {
	rule, fire := f.action(OpStat, name)
	if fire && !rule.After {
		return nil, ruleError(rule)
	}
	info, err := f.FS.Stat(name)
	if err == nil && fire {
		return nil, ruleError(rule)
	}
	return info, err
}

func (f *FS) simple(operation Operation, path string, call func() error) error {
	rule, fire := f.action(operation, path)
	if fire && !rule.After {
		return ruleError(rule)
	}
	if err := call(); err != nil {
		return err
	}
	if fire {
		return ruleError(rule)
	}
	return nil
}

type fileWrapper struct {
	disk.File
	fs   *FS
	path string
}

func (f *fileWrapper) Read(p []byte) (int, error) {
	if rule, ok := f.fs.action(OpRead, f.path); ok {
		return stream(rule, p, func(part []byte) (int, error) { return f.File.Read(part) })
	}
	return f.File.Read(p)
}

func (f *fileWrapper) ReadAt(p []byte, off int64) (int, error) {
	if rule, ok := f.fs.action(OpReadAt, f.path); ok {
		return stream(rule, p, func(part []byte) (int, error) { return f.File.ReadAt(part, off) })
	}
	return f.File.ReadAt(p, off)
}

func (f *fileWrapper) Write(p []byte) (int, error) {
	if rule, ok := f.fs.action(OpWrite, f.path); ok {
		return stream(rule, p, func(part []byte) (int, error) { return f.File.Write(part) })
	}
	return f.File.Write(p)
}

func (f *fileWrapper) WriteAt(p []byte, off int64) (int, error) {
	if rule, ok := f.fs.action(OpWriteAt, f.path); ok {
		return stream(rule, p, func(part []byte) (int, error) { return f.File.WriteAt(part, off) })
	}
	return f.File.WriteAt(p, off)
}

func (f *fileWrapper) Sync() error {
	return f.fs.simple(OpSync, f.path, f.File.Sync)
}

func (f *fileWrapper) Close() error {
	return f.fs.simple(OpClose, f.path, f.File.Close)
}

func (f *fileWrapper) Stat() (os.FileInfo, error) {
	rule, fire := f.fs.action(OpStat, f.path)
	if fire && !rule.After {
		return nil, ruleError(rule)
	}
	info, err := f.File.Stat()
	if err == nil && fire {
		return nil, ruleError(rule)
	}
	return info, err
}

func stream(rule Rule, buffer []byte, call func([]byte) (int, error)) (int, error) {
	if rule.ZeroProgress {
		return 0, nil
	}
	part := buffer
	if rule.MaxBytes > 0 && rule.MaxBytes < len(part) {
		part = part[:rule.MaxBytes]
	}
	if rule.MaxBytes == 0 && rule.Err != nil && !rule.After {
		return 0, rule.Err
	}
	n, err := call(part)
	if err != nil {
		return n, err
	}
	if rule.Err != nil {
		return n, rule.Err
	}
	if n == 0 && len(buffer) > 0 && len(part) > 0 {
		return 0, io.ErrNoProgress
	}
	return n, nil
}
