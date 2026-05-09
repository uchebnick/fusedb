package oneleafdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	walOpPut byte = iota + 1
	walOpDelete
	walOpInc
)

type wal struct {
	file *os.File
	buf  [17]byte
}

func openWAL(path string) (*wal, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{file: file}, nil
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
	if w == nil || w.file == nil {
		return nil
	}
	if len(key) > int(^uint32(0)) || len(payload) > int(^uint32(0)) {
		return fmt.Errorf("oneleafdb wal: record too large")
	}
	w.buf[0] = kind
	binary.LittleEndian.PutUint64(w.buf[1:9], uint64(len(key)))
	binary.LittleEndian.PutUint64(w.buf[9:17], uint64(len(payload)))
	if _, err := w.file.Write(w.buf[:]); err != nil {
		return err
	}
	if _, err := w.file.Write(key); err != nil {
		return err
	}
	if _, err := w.file.Write(payload); err != nil {
		return err
	}
	return nil
}

func (w *wal) close() error {
	if w == nil || w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
