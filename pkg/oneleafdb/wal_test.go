package oneleafdb

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWALGroupWriterFlushesRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oneleaf.wal")
	w, err := openWAL(path, time.Hour)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	if err := w.appendPut([]byte("a"), []byte("one")); err != nil {
		t.Fatalf("append put: %v", err)
	}
	if err := w.appendDelete([]byte("b")); err != nil {
		t.Fatalf("append delete: %v", err)
	}
	if err := w.appendInc([]byte("c"), 7); err != nil {
		t.Fatalf("append inc: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	records := readWALRecords(t, path)
	if len(records) != 3 {
		t.Fatalf("records = %d, want 3", len(records))
	}
	if records[0].kind != walOpPut || string(records[0].key) != "a" || string(records[0].payload) != "one" {
		t.Fatalf("bad put record: %+v", records[0])
	}
	if records[1].kind != walOpDelete || string(records[1].key) != "b" || len(records[1].payload) != 0 {
		t.Fatalf("bad delete record: %+v", records[1])
	}
	if records[2].kind != walOpInc || string(records[2].key) != "c" {
		t.Fatalf("bad inc record: %+v", records[2])
	}
	if got := int64(binary.LittleEndian.Uint64(records[2].payload)); got != 7 {
		t.Fatalf("inc delta = %d, want 7", got)
	}
}

func TestWALSyncModeFlushesBeforeReturn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oneleaf.wal")
	w, err := openWALWithSync(path, time.Hour, true)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.close()

	if err := w.appendPut([]byte("a"), []byte("one")); err != nil {
		t.Fatalf("sync append: %v", err)
	}

	records := readWALRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
}

func TestWALConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oneleaf.wal")
	w, err := openWAL(path, 100*time.Microsecond)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	const goroutines = 8
	const perGoroutine = 128
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := []byte{byte(id)}
			for i := 0; i < perGoroutine; i++ {
				if err := w.appendPut(key, []byte{byte(i)}); err != nil {
					t.Errorf("append put: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if err := w.close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	records := readWALRecords(t, path)
	if len(records) != goroutines*perGoroutine {
		t.Fatalf("records = %d, want %d", len(records), goroutines*perGoroutine)
	}
}

func TestWALRejectsAppendAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oneleaf.wal")
	w, err := openWAL(path, time.Microsecond)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
	if err := w.appendPut([]byte("a"), []byte("b")); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("append after close err = %v, want %v", err, ErrWALClosed)
	}
}

type walTestRecord struct {
	kind    byte
	key     []byte
	payload []byte
}

func readWALRecords(t *testing.T, path string) []walTestRecord {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open wal file: %v", err)
	}
	defer file.Close()

	var records []walTestRecord
	for {
		header := make([]byte, walHeaderSize)
		if _, err := io.ReadFull(file, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("short wal header: %v", err)
				}
				return records
			}
			t.Fatalf("read wal header: %v", err)
		}

		keyLen := int(binary.LittleEndian.Uint64(header[1:9]))
		payloadLen := int(binary.LittleEndian.Uint64(header[9:17]))
		key := make([]byte, keyLen)
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(file, key); err != nil {
			t.Fatalf("read wal key: %v", err)
		}
		if _, err := io.ReadFull(file, payload); err != nil {
			t.Fatalf("read wal payload: %v", err)
		}
		records = append(records, walTestRecord{
			kind:    header[0],
			key:     key,
			payload: payload,
		})
	}
}
