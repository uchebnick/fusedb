package oneleafdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	walOpPut byte = iota + 1
	walOpDelete
	walOpInc
)

const (
	walHeaderSize        = 17
	defaultWALBufferSize = 1 << 20
)

var ErrWALClosed = errors.New("oneleafdb wal: closed")

type wal struct {
	file       *os.File
	groupEvery time.Duration
	syncWrites bool

	mu     sync.Mutex
	active []byte
	flush  chan walFlushRequest
	stop   chan struct{}
	done   chan error

	closeOnce sync.Once
	closed    atomic.Bool
}

type walFlushRequest struct {
	done chan error
}

func openWAL(path string, groupEvery time.Duration) (*wal, error) {
	return openWALWithSync(path, groupEvery, false)
}

func openWALWithSync(path string, groupEvery time.Duration, syncWrites bool) (*wal, error) {
	if path == "" {
		return nil, nil
	}
	if groupEvery <= 0 {
		groupEvery = DefaultWALGroupCommitInterval
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	w := &wal{
		file:       file,
		groupEvery: groupEvery,
		syncWrites: syncWrites,
		active:     make([]byte, 0, defaultWALBufferSize),
		flush:      make(chan walFlushRequest, 1),
		stop:       make(chan struct{}),
		done:       make(chan error, 1),
	}
	go w.run()
	return w, nil
}

func (w *wal) appendPut(key, value []byte) error {
	return w.appendRecord(walOpPut, key, value)
}

func (w *wal) appendDelete(key []byte) error {
	return w.appendRecord(walOpDelete, key, nil)
}

func (w *wal) appendInc(key []byte, delta int64) error {
	var payload [8]byte
	binary.LittleEndian.PutUint64(payload[:], uint64(delta))
	return w.appendRecord(walOpInc, key, payload[:])
}

func (w *wal) appendRecord(kind byte, key, payload []byte) error {
	if w == nil {
		return nil
	}
	if w.closed.Load() {
		return ErrWALClosed
	}
	if len(key) > int(^uint32(0)) || len(payload) > int(^uint32(0)) {
		return fmt.Errorf("oneleafdb wal: record too large")
	}

	w.mu.Lock()
	if w.closed.Load() {
		w.mu.Unlock()
		return ErrWALClosed
	}
	w.appendRecordLocked(kind, key, payload)
	w.mu.Unlock()

	if !w.syncWrites {
		return nil
	}
	return w.flushAndWait()
}

func (w *wal) appendRecordLocked(kind byte, key, payload []byte) {
	w.active = append(w.active, kind)
	w.active = binary.LittleEndian.AppendUint64(w.active, uint64(len(key)))
	w.active = binary.LittleEndian.AppendUint64(w.active, uint64(len(payload)))
	w.active = append(w.active, key...)
	w.active = append(w.active, payload...)
}

func (w *wal) flushAndWait() error {
	request := walFlushRequest{done: make(chan error, 1)}
	select {
	case w.flush <- request:
	case <-w.stop:
		return ErrWALClosed
	}
	return <-request.done
}

func (w *wal) run() {
	ticker := time.NewTicker(w.groupEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = w.flushActive()
		case request := <-w.flush:
			request.done <- w.flushActive()
		case <-w.stop:
			err := w.flushActive()
			w.finishPendingFlushRequests(err)
			w.done <- w.closeFile(err)
			return
		}
	}
}

func (w *wal) finishPendingFlushRequests(err error) {
	for {
		select {
		case request := <-w.flush:
			request.done <- err
		default:
			return
		}
	}
}

func (w *wal) flushActive() error {
	data := w.swapActive()
	if len(data) == 0 {
		return nil
	}
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *wal) swapActive() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.active) == 0 {
		return nil
	}
	data := w.active
	w.active = make([]byte, 0, max(cap(data), defaultWALBufferSize))
	return data
}

func (w *wal) closeFile(prevErr error) error {
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

func (w *wal) close() error {
	if w == nil {
		return nil
	}

	var err error
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed.Store(true)
		w.mu.Unlock()
		close(w.stop)
		err = <-w.done
	})
	return err
}
