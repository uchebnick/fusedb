package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
)

const (
	// DefaultGroupCommitInterval is how often buffered records are flushed and
	// synced when SyncWrites is disabled.
	DefaultGroupCommitInterval = 200 * time.Microsecond

	// DefaultBufferSize is the initial capacity of the in-memory record buffer.
	DefaultBufferSize = 1 << 20

	tempSuffix = ".rotate.tmp"
)

// Options configures a WAL.
type Options struct {
	// FS is the filesystem to use. Defaults to disk.DefaultFS.
	FS disk.FS
	// Path is the log file path. Required.
	Path string
	// GroupCommitInterval is the group commit period. Defaults to
	// DefaultGroupCommitInterval.
	GroupCommitInterval time.Duration
	// SyncWrites makes every Append wait until its record is written and
	// synced.
	SyncWrites bool
	// BufferSize is the initial record buffer capacity. Defaults to
	// DefaultBufferSize.
	BufferSize int
}

// WAL is an append-only write-ahead log with group commit.
//
// Records are staged in memory and handed to a single background writer, which
// owns the file handle. Append is safe for concurrent use; every record gets a
// unique, monotonically increasing sequence number in the order it is staged.
type WAL struct {
	fs         disk.FS
	path       string
	groupEvery time.Duration
	syncWrites bool
	bufferSize int

	mu      sync.Mutex
	active  []byte
	nextSeq uint64
	closed  bool
	pending sync.WaitGroup

	lastSeq atomic.Uint64
	baseSeq atomic.Uint64

	flushCh  chan flushRequest
	rotateCh chan rotateRequest
	stop     chan struct{}
	done     chan error

	closeOnce sync.Once
	closeErr  error

	// Fields below are owned by the writer goroutine.
	file   disk.File
	offset int64

	recoveredTail bool
}

type flushRequest struct {
	done chan error
}

type rotateRequest struct {
	upToSeq uint64
	done    chan error
}

// Open opens or creates the log at opts.Path.
//
// An existing log is scanned to recover the next sequence number and the offset
// of the last intact record. A truncated trailing record left behind by a crash
// is dropped and overwritten by later appends; corruption before the tail is
// reported as an error.
func Open(opts Options) (*WAL, error) {
	if opts.Path == "" {
		return nil, ErrNoPath
	}
	fs := opts.FS
	if fs == nil {
		fs = disk.DefaultFS
	}
	groupEvery := opts.GroupCommitInterval
	if groupEvery <= 0 {
		groupEvery = DefaultGroupCommitInterval
	}
	bufferSize := opts.BufferSize
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}

	if dir := filepath.Dir(opts.Path); dir != "." && dir != "" {
		if err := fs.MkdirAll(dir); err != nil {
			return nil, err
		}
	}

	baseSeq, lastSeq, offset, tail, err := recoverFile(fs, opts.Path)
	if err != nil {
		return nil, err
	}

	file, err := fs.OpenReadWrite(opts.Path)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		fs:            fs,
		path:          opts.Path,
		groupEvery:    groupEvery,
		syncWrites:    opts.SyncWrites,
		bufferSize:    bufferSize,
		active:        make([]byte, 0, bufferSize),
		nextSeq:       lastSeq + 1,
		flushCh:       make(chan flushRequest),
		rotateCh:      make(chan rotateRequest),
		stop:          make(chan struct{}),
		done:          make(chan error, 1),
		file:          file,
		offset:        offset,
		recoveredTail: tail,
	}
	w.baseSeq.Store(baseSeq)
	w.lastSeq.Store(lastSeq)

	go w.run()
	return w, nil
}

// recoverFile inspects an existing log file, creating it when absent.
//
// A truncated trailing record is physically dropped by rewriting the file, so
// leftover bytes from an interrupted write are never mistaken for a record on a
// later pass.
func recoverFile(fs disk.FS, path string) (baseSeq, lastSeq uint64, end int64, tail bool, err error) {
	info, statErr := fs.Stat(path)
	switch {
	case errors.Is(statErr, os.ErrNotExist):
		return 1, 0, fileHeaderSize, false, installFile(fs, path, encodeFileHeader(1))
	case statErr != nil:
		return 0, 0, 0, false, statErr
	case info.Size() == 0:
		// A crash between file creation and the header write leaves an empty
		// file; treat it as a fresh log.
		return 1, 0, fileHeaderSize, false, installFile(fs, path, encodeFileHeader(1))
	}

	result, err := Iterate(fs, path, nil)
	if err != nil {
		return 0, 0, 0, false, err
	}

	if result.TruncatedTail {
		data, err := disk.ReadFile(fs, path)
		if err != nil {
			return 0, 0, 0, false, err
		}
		if int64(len(data)) < result.ValidEnd {
			return 0, 0, 0, false, ErrShortWALFile
		}
		if err := installFile(fs, path, data[:result.ValidEnd]); err != nil {
			return 0, 0, 0, false, err
		}
	}
	return result.BaseSeq, result.LastSeq, result.ValidEnd, result.TruncatedTail, nil
}

// LastSeq returns the most recently assigned sequence number.
//
// It returns BaseSeq-1 when no record has been appended yet.
func (w *WAL) LastSeq() uint64 { return w.lastSeq.Load() }

// BaseSeq returns the sequence number of the first record the current file can
// hold.
func (w *WAL) BaseSeq() uint64 { return w.baseSeq.Load() }

// Path returns the log file path.
func (w *WAL) Path() string { return w.path }

// RecoveredTruncatedTail reports whether Open dropped a partially written
// trailing record.
func (w *WAL) RecoveredTruncatedTail() bool { return w.recoveredTail }

// AppendPut logs a put and returns the assigned sequence number.
func (w *WAL) AppendPut(key, value []byte) (uint64, error) {
	return w.append(recordKindPut, key, value)
}

// AppendDelete logs a delete tombstone and returns the assigned sequence
// number.
func (w *WAL) AppendDelete(key []byte) (uint64, error) {
	return w.append(recordKindDelete, key, nil)
}

// AppendInc logs a counter increment and returns the assigned sequence number.
//
// The delta is encoded exactly like ops.NewInc so the record payload can be
// handed to ops.DecodeInc unchanged.
func (w *WAL) AppendInc(key []byte, delta int64) (uint64, error) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], delta)
	return w.append(recordKindInc, key, buf[:n])
}

// Append logs an arbitrary operation and returns the assigned sequence number.
func (w *WAL) Append(record Record) (uint64, error) {
	kind, err := recordKindOf(record.Kind)
	if err != nil {
		return 0, err
	}
	return w.append(kind, record.Key, record.Payload)
}

func (w *WAL) append(kind byte, key, payload []byte) (uint64, error) {
	if !lengthFits(len(key)) || !lengthFits(len(payload)) {
		return 0, ErrRecordTooLarge
	}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return 0, ErrClosed
	}
	seq := w.nextSeq
	w.nextSeq++
	w.active = appendRecord(w.active, kind, seq, key, payload)
	w.lastSeq.Store(seq)
	if !w.syncWrites {
		w.mu.Unlock()
		return seq, nil
	}
	// Keep the writer goroutine alive until this request is answered.
	w.pending.Add(1)
	w.mu.Unlock()
	defer w.pending.Done()

	return seq, w.requestFlush()
}

// Sync flushes every staged record and syncs the file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.pending.Add(1)
	w.mu.Unlock()
	defer w.pending.Done()

	return w.requestFlush()
}

func (w *WAL) requestFlush() error {
	request := flushRequest{done: make(chan error, 1)}
	w.flushCh <- request
	return <-request.done
}

// Truncate discards records with a sequence number at or below upToSeq.
//
// Surviving records are rewritten into a fresh file whose header declares
// BaseSeq = upToSeq+1. The replacement is installed through a temporary file,
// an atomic rename and a directory sync, so a crash at any point leaves either
// the old or the new complete log in place.
func (w *WAL) Truncate(upToSeq uint64) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.pending.Add(1)
	w.mu.Unlock()
	defer w.pending.Done()

	request := rotateRequest{upToSeq: upToSeq, done: make(chan error, 1)}
	w.rotateCh <- request
	return <-request.done
}

// Close flushes staged records, stops the writer goroutine and closes the file.
//
// Close is idempotent; later calls return the result of the first one.
func (w *WAL) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()

		// No new flush or rotate request can be created once closed is set, so
		// waiting here guarantees nobody blocks on a channel the writer
		// goroutine will no longer serve.
		w.pending.Wait()

		close(w.stop)
		w.closeErr = <-w.done
	})
	return w.closeErr
}

func (w *WAL) run() {
	ticker := time.NewTicker(w.groupEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = w.flushActive()
		case request := <-w.flushCh:
			request.done <- w.flushActive()
		case request := <-w.rotateCh:
			request.done <- w.rotate(request.upToSeq)
		case <-w.stop:
			err := w.flushActive()
			w.done <- w.closeFile(err)
			return
		}
	}
}

func (w *WAL) flushActive() error {
	data := w.swapActive()
	if len(data) == 0 {
		return nil
	}
	if w.file == nil {
		return ErrClosed
	}
	if err := w.writeAt(data); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) swapActive() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.active) == 0 {
		return nil
	}
	data := w.active
	w.active = make([]byte, 0, max(cap(data), w.bufferSize))
	return data
}

func (w *WAL) writeAt(data []byte) error {
	for len(data) > 0 {
		n, err := w.file.WriteAt(data, w.offset)
		w.offset += int64(n)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("wal: short write")
		}
		data = data[n:]
	}
	return nil
}

// rotate runs on the writer goroutine and replaces the log file with one that
// only holds records above upToSeq.
func (w *WAL) rotate(upToSeq uint64) error {
	// Persist everything staged so far; the rewrite reads from the file.
	if err := w.flushActive(); err != nil {
		return err
	}
	if last := w.lastSeq.Load(); upToSeq > last {
		upToSeq = last
	}

	newBase := upToSeq + 1
	buf := encodeFileHeader(newBase)
	_, err := Iterate(w.fs, w.path, func(record Record) error {
		if record.Seq < newBase {
			return nil
		}
		kind, err := recordKindOf(record.Kind)
		if err != nil {
			return err
		}
		buf = appendRecord(buf, kind, record.Seq, record.Key, record.Payload)
		return nil
	})
	if err != nil {
		return err
	}

	// The handle must be released before the rename so the platform can swap
	// the file underneath it.
	oldOffset := w.offset
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	if err := installFile(w.fs, w.path, buf); err != nil {
		// The original file survived the failed rotation; reattach to it so the
		// log stays usable.
		w.reopen(oldOffset)
		return err
	}

	file, err := w.fs.OpenReadWrite(w.path)
	if err != nil {
		return err
	}
	w.file = file
	w.offset = int64(len(buf))
	w.baseSeq.Store(newBase)
	return nil
}

func (w *WAL) reopen(offset int64) {
	file, err := w.fs.OpenReadWrite(w.path)
	if err != nil {
		return
	}
	w.file = file
	w.offset = offset
}

// installFile writes data through a temporary file and renames it into place
// once synced.
func installFile(fs disk.FS, path string, data []byte) error {
	tmpPath := path + tempSuffix
	file, err := fs.Create(tmpPath)
	if err != nil {
		return err
	}

	renamed := false
	defer func() {
		if !renamed {
			_ = file.Close()
			_ = fs.Remove(tmpPath)
		}
	}()

	if len(data) > 0 {
		if _, err := file.WriteAt(data, 0); err != nil {
			return err
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmpPath, path); err != nil {
		return err
	}
	renamed = true

	if dir := filepath.Dir(path); dir != "." && dir != "" {
		return fs.SyncDir(dir)
	}
	return nil
}

func (w *WAL) closeFile(prevErr error) error {
	if w.file == nil {
		return prevErr
	}
	err := w.file.Close()
	w.file = nil
	if prevErr != nil {
		return prevErr
	}
	return err
}
