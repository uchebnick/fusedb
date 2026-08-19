package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
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
	spare   []byte
	nextSeq uint64
	closed  bool
	pending sync.WaitGroup

	lastSeq atomic.Uint64
	baseSeq atomic.Uint64
	// persistenceErr is latched after the first write or sync failure. The
	// current handle must not accept more records or checkpoint from a sequence
	// that may describe a record absent from the in-memory tree.
	persistenceErr atomic.Pointer[error]

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
	done     chan error
	coalesce bool
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
		flushCh:       make(chan flushRequest, 4096),
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
		if err := installPrefix(fs, path, result.ValidEnd); err != nil {
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

// AppendBatch appends all mutations as one checksummed WAL record. Replay can
// apply individual members according to their leaf watermarks while the
// record's single sequence number preserves the atomic commit boundary.
func (w *WAL) AppendBatch(mutations []BatchMutation) (uint64, int, error) {
	payload, err := EncodeBatch(mutations)
	if err != nil {
		return 0, 0, err
	}
	seq, err := w.append(recordKindBatch, nil, payload)
	return seq, len(payload), err
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
	if !lengthFits(len(key)) || !lengthFits(len(payload)) ||
		len(key) > limits.MaxKeyBytes || len(payload) > limits.MaxWALPayloadBytes {
		return 0, ErrRecordTooLarge
	}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return 0, ErrClosed
	}
	if err := w.Err(); err != nil {
		w.mu.Unlock()
		return 0, err
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

	return seq, w.requestFlush(true)
}

// Sync flushes every staged record and syncs the file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	if err := w.Err(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.pending.Add(1)
	w.mu.Unlock()
	defer w.pending.Done()

	return w.requestFlush(false)
}

func (w *WAL) requestFlush(coalesce bool) error {
	request := flushRequest{done: make(chan error, 1), coalesce: coalesce}
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
	if err := w.Err(); err != nil {
		w.mu.Unlock()
		return err
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
	groupTimer := time.NewTimer(time.Hour)
	if !groupTimer.Stop() {
		<-groupTimer.C
	}
	defer groupTimer.Stop()
	requests := make([]flushRequest, 0, 64)

	for {
		select {
		case <-ticker.C:
			_ = w.flushActive()
		case request := <-w.flushCh:
			requests = append(requests[:0], request)
			if !request.coalesce {
				err := w.flushActive()
				request.done <- err
				continue
			}
			groupTimer.Reset(w.groupEvery)
		collect:
			for len(requests) < cap(w.flushCh) {
				select {
				case next := <-w.flushCh:
					requests = append(requests, next)
					if !next.coalesce {
						break collect
					}
				case <-groupTimer.C:
					break collect
				}
			}
			if !groupTimer.Stop() {
				select {
				case <-groupTimer.C:
				default:
				}
			}
			err := w.flushActive()
			for _, pending := range requests {
				pending.done <- err
			}
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
	if err := w.Err(); err != nil {
		return err
	}
	data := w.swapActive()
	if len(data) == 0 {
		return nil
	}
	if w.file == nil {
		w.recycle(data)
		return ErrClosed
	}
	written, err := w.writeAt(data)
	if err != nil {
		// A malformed zero-progress/invalid-count writer cannot prove that no
		// bytes became visible, so classify it conservatively as uncertain.
		uncertain := written > 0 || errors.Is(err, io.ErrShortWrite)
		failure := w.failPersistence(err, uncertain)
		w.recycle(data)
		return failure
	}
	if err := w.file.Sync(); err != nil {
		failure := w.failPersistence(err, true)
		w.recycle(data)
		return failure
	}
	w.recycle(data)
	return nil
}

func (w *WAL) swapActive() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.active) == 0 {
		return nil
	}
	data := w.active
	if w.spare != nil {
		w.active = w.spare[:0]
		w.spare = nil
	} else {
		w.active = make([]byte, 0, max(cap(data), w.bufferSize))
	}
	return data
}

func (w *WAL) recycle(data []byte) {
	if data == nil {
		return
	}
	w.mu.Lock()
	if w.spare == nil || cap(data) > cap(w.spare) {
		w.spare = data[:0]
	}
	w.mu.Unlock()
}

func (w *WAL) writeAt(data []byte) (int, error) {
	written, err := disk.WriteAllAt(w.file, data, w.offset)
	w.offset += int64(written)
	return written, err
}

// Err reports the first terminal persistence failure, if any.
func (w *WAL) Err() error {
	if w == nil {
		return nil
	}
	if err := w.persistenceErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (w *WAL) failPersistence(cause error, uncertain bool) error {
	if cause == nil {
		return nil
	}
	var failure error
	if uncertain {
		failure = fmt.Errorf("%w: %w: %w", ErrPersistence, disk.ErrCommitUncertain, cause)
	} else {
		failure = fmt.Errorf("%w: %w", ErrPersistence, cause)
	}
	w.persistenceErr.CompareAndSwap(nil, &failure)
	return w.Err()
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

	// The handle must be released before the rename so the platform can swap
	// the file underneath it.
	oldOffset := w.offset
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	newBase := upToSeq + 1
	newOffset := int64(fileHeaderSize)
	if err := installFileStreaming(w.fs, w.path, func(file disk.File) error {
		if _, err := disk.WriteAllAt(file, encodeFileHeader(newBase), 0); err != nil {
			return err
		}
		var recordBuf []byte
		_, err := Iterate(w.fs, w.path, func(record Record) error {
			if record.Seq < newBase {
				return nil
			}
			kind, err := recordKindOf(record.Kind)
			if err != nil {
				return err
			}
			recordBuf = appendRecord(recordBuf[:0], kind, record.Seq, record.Key, record.Payload)
			if _, err := disk.WriteAllAt(file, recordBuf, newOffset); err != nil {
				return err
			}
			newOffset += int64(len(recordBuf))
			return nil
		})
		return err
	}); err != nil {
		if errors.Is(err, disk.ErrCommitUncertain) {
			file, reopenErr := w.fs.OpenReadWrite(w.path)
			if reopenErr != nil {
				return errors.Join(err, reopenErr)
			}
			w.file = file
			w.offset = newOffset
			w.baseSeq.Store(newBase)
			return err
		}
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
	w.offset = newOffset
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
	return installFileStreaming(fs, path, func(file disk.File) error {
		_, err := disk.WriteAllAt(file, data, 0)
		return err
	})
}

// installPrefix replaces path with its first size bytes without materializing
// the WAL in memory. It is used after torn-tail recovery, where the retained
// prefix may be many gigabytes.
func installPrefix(fs disk.FS, path string, size int64) error {
	if size < 0 {
		return ErrShortWALFile
	}
	source, err := fs.Open(path)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = source.Close()
		}
	}()

	err = installFileStreaming(fs, path, func(destination disk.File) error {
		buffer := make([]byte, readerBufferSize)
		var offset int64
		for offset < size {
			chunk := int64(len(buffer))
			if remaining := size - offset; remaining < chunk {
				chunk = remaining
			}
			part := buffer[:int(chunk)]
			n, readErr := source.ReadAt(part, offset)
			if n != len(part) || (readErr != nil && !errors.Is(readErr, io.EOF)) {
				return ErrShortWALFile
			}
			if _, err := disk.WriteAllAt(destination, part, offset); err != nil {
				return err
			}
			offset += int64(n)
		}
		closeErr := source.Close()
		closed = true
		return closeErr
	})
	return err
}

func installFileStreaming(fs disk.FS, path string, write func(disk.File) error) error {
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

	if write != nil {
		if err := write(file); err != nil {
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
		if err := fs.SyncDir(dir); err != nil {
			return fmt.Errorf("%w: %v", disk.ErrCommitUncertain, err)
		}
	}
	return nil
}

func (w *WAL) closeFile(prevErr error) error {
	if prevErr == nil {
		prevErr = w.Err()
	}
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
