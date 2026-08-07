package wal

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/ops"
)

const testPath = "db/000001.wal"

func newTestWAL(t *testing.T, fs disk.FS, syncWrites bool) *WAL {
	t.Helper()

	w, err := Open(Options{
		FS:                  fs,
		Path:                testPath,
		SyncWrites:          syncWrites,
		GroupCommitInterval: 200 * time.Microsecond,
	})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	return w
}

func readBytes(t *testing.T, fs disk.FS, path string) []byte {
	t.Helper()

	data, err := disk.ReadFile(fs, path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	return data
}

func rewriteBytes(t *testing.T, fs disk.FS, path string, data []byte) {
	t.Helper()

	file, err := fs.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if len(data) > 0 {
		if _, err := file.WriteAt(data, 0); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}
}

func TestRoundTripAllKinds(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)

	if got := w.BaseSeq(); got != 1 {
		t.Fatalf("base seq = %d, want 1", got)
	}
	if got := w.LastSeq(); got != 0 {
		t.Fatalf("last seq = %d, want 0", got)
	}

	const total = 30
	type expected struct {
		kind ops.OpKind
		key  string
		seq  uint64
	}
	want := make([]expected, 0, total)

	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%03d", i)
		var (
			seq uint64
			err error
		)
		kind := ops.OpKind(i % 3)

		switch kind {
		case ops.OpPut:
			seq, err = w.AppendPut([]byte(key), []byte(fmt.Sprintf("value-%03d", i)))
		case ops.OpDelete:
			seq, err = w.AppendDelete([]byte(key))
		case ops.OpInc:
			seq, err = w.AppendInc([]byte(key), int64(i)-15)
		}
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("append %d: seq = %d, want %d", i, seq, i+1)
		}
		want = append(want, expected{kind: kind, key: key, seq: seq})
	}

	if got := w.LastSeq(); got != total {
		t.Fatalf("last seq = %d, want %d", got, total)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.TruncatedTail {
		t.Fatal("unexpected truncated tail")
	}
	if result.BaseSeq != 1 || result.LastSeq != total || result.Count != total {
		t.Fatalf("result = %+v", result)
	}
	if len(records) != total {
		t.Fatalf("read %d records, want %d", len(records), total)
	}

	for i, record := range records {
		if record.Kind != want[i].kind {
			t.Fatalf("record %d: kind = %d, want %d", i, record.Kind, want[i].kind)
		}
		if record.Seq != want[i].seq {
			t.Fatalf("record %d: seq = %d, want %d", i, record.Seq, want[i].seq)
		}
		if string(record.Key) != want[i].key {
			t.Fatalf("record %d: key = %q, want %q", i, record.Key, want[i].key)
		}

		switch record.Kind {
		case ops.OpPut:
			if got, wantValue := string(record.Payload), fmt.Sprintf("value-%03d", i); got != wantValue {
				t.Fatalf("record %d: payload = %q, want %q", i, got, wantValue)
			}
		case ops.OpDelete:
			if len(record.Payload) != 0 {
				t.Fatalf("record %d: delete payload = %q", i, record.Payload)
			}
		case ops.OpInc:
			if got, wantDelta := ops.DecodeInc(record.Op()), int64(i)-15; got != wantDelta {
				t.Fatalf("record %d: delta = %d, want %d", i, got, wantDelta)
			}
		}
	}
}

func TestEmptyKeyAndValue(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)

	if _, err := w.AppendPut(nil, nil); err != nil {
		t.Fatalf("append empty put: %v", err)
	}
	if _, err := w.AppendPut([]byte{}, []byte("v")); err != nil {
		t.Fatalf("append empty key: %v", err)
	}
	if _, err := w.AppendDelete(nil); err != nil {
		t.Fatalf("append empty delete: %v", err)
	}
	if _, err := w.AppendInc(nil, 0); err != nil {
		t.Fatalf("append empty inc: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.TruncatedTail || result.Count != 4 {
		t.Fatalf("result = %+v", result)
	}

	if len(records[0].Key) != 0 || len(records[0].Payload) != 0 {
		t.Fatalf("record 0 = %+v", records[0])
	}
	if len(records[1].Key) != 0 || string(records[1].Payload) != "v" {
		t.Fatalf("record 1 = %+v", records[1])
	}
	if records[2].Kind != ops.OpDelete || len(records[2].Key) != 0 {
		t.Fatalf("record 2 = %+v", records[2])
	}
	if got := ops.DecodeInc(records[3].Op()); got != 0 {
		t.Fatalf("record 3 delta = %d, want 0", got)
	}
}

func TestEmptyLogHasNoRecords(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("read %d records, want 0", len(records))
	}
	if result.TruncatedTail || result.BaseSeq != 1 || result.LastSeq != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func writeSampleLog(t *testing.T, fs disk.FS, count int) {
	t.Helper()

	w := newTestWAL(t, fs, true)
	for i := 0; i < count; i++ {
		if _, err := w.AppendPut([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestTruncatedTailIsNotAnError(t *testing.T) {
	fs := disk.NewMemFS()
	writeSampleLog(t, fs, 5)

	data := readBytes(t, fs, testPath)
	rewriteBytes(t, fs, testPath, data[:len(data)-5])

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if !result.TruncatedTail {
		t.Fatal("truncated tail not reported")
	}
	if len(records) != 4 || result.Count != 4 || result.LastSeq != 4 {
		t.Fatalf("records = %d, result = %+v", len(records), result)
	}
	for i, record := range records {
		if string(record.Key) != fmt.Sprintf("key-%02d", i) {
			t.Fatalf("record %d: key = %q", i, record.Key)
		}
	}
	if result.ValidEnd != int64(len(data)-22) {
		t.Fatalf("valid end = %d, want %d", result.ValidEnd, len(data)-22)
	}
}

func TestTruncatedTailIsDroppedOnReopen(t *testing.T) {
	fs := disk.NewMemFS()
	writeSampleLog(t, fs, 5)

	data := readBytes(t, fs, testPath)
	rewriteBytes(t, fs, testPath, data[:len(data)-3])

	w := newTestWAL(t, fs, true)
	if !w.RecoveredTruncatedTail() {
		t.Fatal("reopen did not report a truncated tail")
	}
	if got := w.LastSeq(); got != 4 {
		t.Fatalf("last seq = %d, want 4", got)
	}

	seq, err := w.AppendPut([]byte("after-crash"), []byte("ok"))
	if err != nil {
		t.Fatalf("append after crash: %v", err)
	}
	if seq != 5 {
		t.Fatalf("seq = %d, want 5", seq)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.TruncatedTail {
		t.Fatal("tail still reported after recovery")
	}
	if len(records) != 5 {
		t.Fatalf("read %d records, want 5", len(records))
	}
	if string(records[4].Key) != "after-crash" || records[4].Seq != 5 {
		t.Fatalf("last record = %+v", records[4])
	}
}

func TestCorruptedTrailingRecordIsTreatedAsTail(t *testing.T) {
	fs := disk.NewMemFS()
	writeSampleLog(t, fs, 5)

	data := readBytes(t, fs, testPath)
	// Flip a key byte of the last record; the record still ends at EOF, so this
	// is indistinguishable from an interrupted write and must not be an error.
	data[len(data)-10] ^= 0xff
	rewriteBytes(t, fs, testPath, data)

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if !result.TruncatedTail {
		t.Fatal("truncated tail not reported")
	}
	if len(records) != 4 {
		t.Fatalf("read %d records, want 4", len(records))
	}
}

func TestCorruptionInsideFileIsDetected(t *testing.T) {
	fs := disk.NewMemFS()
	writeSampleLog(t, fs, 5)

	data := readBytes(t, fs, testPath)
	// Byte 5 of the first record lands inside its key.
	data[fileHeaderSize+5] ^= 0xff
	rewriteBytes(t, fs, testPath, data)

	_, _, err := ReadAll(fs, testPath)
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("err = %v, want ErrCorruptRecord", err)
	}

	if _, err := Open(Options{FS: fs, Path: testPath}); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("open err = %v, want ErrCorruptRecord", err)
	}
}

func TestCorruptKindByteIsDetected(t *testing.T) {
	fs := disk.NewMemFS()
	writeSampleLog(t, fs, 5)

	data := readBytes(t, fs, testPath)
	data[fileHeaderSize] = 0
	rewriteBytes(t, fs, testPath, data)

	if _, _, err := ReadAll(fs, testPath); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("err = %v, want ErrCorruptRecord", err)
	}
}

func TestCorruptHeaderIsDetected(t *testing.T) {
	fs := disk.NewMemFS()
	writeSampleLog(t, fs, 2)

	data := readBytes(t, fs, testPath)
	data[0] = 'X'
	rewriteBytes(t, fs, testPath, data)

	if _, _, err := ReadAll(fs, testPath); !errors.Is(err, ErrMagicMismatch) {
		t.Fatalf("err = %v, want ErrMagicMismatch", err)
	}
}

func TestTruncateDropsAppliedRecords(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)

	for i := 0; i < 10; i++ {
		if _, err := w.AppendPut([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if err := w.Truncate(4); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := w.BaseSeq(); got != 5 {
		t.Fatalf("base seq = %d, want 5", got)
	}
	if got := w.LastSeq(); got != 10 {
		t.Fatalf("last seq = %d, want 10", got)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all after truncate: %v", err)
	}
	if result.BaseSeq != 5 || len(records) != 6 {
		t.Fatalf("records = %d, result = %+v", len(records), result)
	}
	for i, record := range records {
		if record.Seq != uint64(i+5) {
			t.Fatalf("record %d: seq = %d, want %d", i, record.Seq, i+5)
		}
		if string(record.Key) != fmt.Sprintf("key-%02d", i+4) {
			t.Fatalf("record %d: key = %q", i, record.Key)
		}
	}

	seq, err := w.AppendPut([]byte("post"), []byte("truncate"))
	if err != nil {
		t.Fatalf("append after truncate: %v", err)
	}
	if seq != 11 {
		t.Fatalf("seq = %d, want 11", seq)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records, _, err = ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all after reopen: %v", err)
	}
	if len(records) != 7 || records[6].Seq != 11 {
		t.Fatalf("records = %d, last = %+v", len(records), records[len(records)-1])
	}

	// The rotation temporary file must not be left behind.
	if _, err := fs.Stat(testPath + tempSuffix); err == nil {
		t.Fatal("rotation temp file was not removed")
	}
}

func TestTruncateBeyondLastSeqClampsToNextSeq(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)

	for i := 0; i < 3; i++ {
		if _, err := w.AppendPut([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Truncate(100); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := w.BaseSeq(); got != 4 {
		t.Fatalf("base seq = %d, want 4", got)
	}

	seq, err := w.AppendPut([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if seq != 4 {
		t.Fatalf("seq = %d, want 4", seq)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(records) != 1 || records[0].Seq != 4 || result.BaseSeq != 4 {
		t.Fatalf("records = %d, result = %+v", len(records), result)
	}
}

func TestConcurrentAppend(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, false)

	const (
		writers        = 8
		perWriter      = 100
		expectedTotal  = writers * perWriter
		concurrentKeyF = "w%02d-k%03d"
	)

	seqs := make([][]uint64, writers)
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			local := make([]uint64, 0, perWriter)
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf(concurrentKeyF, g, i)
				seq, err := w.AppendPut([]byte(key), []byte(key))
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				local = append(local, seq)
			}
			seqs[g] = local
		}(g)
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	seen := make(map[uint64]bool, expectedTotal)
	for g := range seqs {
		for i, seq := range seqs[g] {
			if seen[seq] {
				t.Fatalf("duplicate seq %d", seq)
			}
			seen[seq] = true
			if i > 0 && seq <= seqs[g][i-1] {
				t.Fatalf("writer %d: seq %d not after %d", g, seq, seqs[g][i-1])
			}
		}
	}
	for seq := uint64(1); seq <= expectedTotal; seq++ {
		if !seen[seq] {
			t.Fatalf("missing seq %d", seq)
		}
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.TruncatedTail {
		t.Fatal("unexpected truncated tail")
	}
	if len(records) != expectedTotal {
		t.Fatalf("read %d records, want %d", len(records), expectedTotal)
	}

	keys := make(map[string]bool, expectedTotal)
	for i, record := range records {
		if record.Seq != uint64(i+1) {
			t.Fatalf("record %d: seq = %d, want %d", i, record.Seq, i+1)
		}
		if string(record.Key) != string(record.Payload) {
			t.Fatalf("record %d: key %q != payload %q", i, record.Key, record.Payload)
		}
		if keys[string(record.Key)] {
			t.Fatalf("duplicate key %q", record.Key)
		}
		keys[string(record.Key)] = true
	}
	for g := 0; g < writers; g++ {
		for i := 0; i < perWriter; i++ {
			if !keys[fmt.Sprintf(concurrentKeyF, g, i)] {
				t.Fatalf("missing key w%02d-k%03d", g, i)
			}
		}
	}
}

func TestConcurrentAppendWithSyncWrites(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if _, err := w.AppendPut([]byte(fmt.Sprintf("s%d-%d", g, i)), []byte("v")); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.Count != 100 || result.LastSeq != 100 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTruncateDuringConcurrentAppends(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, false)

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := w.AppendPut([]byte(fmt.Sprintf("r%d-%d", g, i)), []byte("v")); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := w.Truncate(w.LastSeq()); err != nil {
				t.Errorf("truncate: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()

	lastSeq := w.LastSeq()
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.LastSeq != lastSeq {
		t.Fatalf("last seq = %d, want %d", result.LastSeq, lastSeq)
	}
	for i, record := range records {
		if record.Seq != result.BaseSeq+uint64(i) {
			t.Fatalf("record %d: seq = %d, want %d", i, record.Seq, result.BaseSeq+uint64(i))
		}
	}
}

func TestCloseIsIdempotentAndLeaksNoGoroutines(t *testing.T) {
	fs := disk.NewMemFS()
	before := goroutineCount()

	w := newTestWAL(t, fs, false)
	for i := 0; i < 50; i++ {
		if _, err := w.AppendPut([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := w.Close(); err != nil {
			t.Fatalf("close %d: %v", i+2, err)
		}
	}

	if _, err := w.AppendPut([]byte("k"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close: err = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("sync after close: err = %v, want ErrClosed", err)
	}
	if err := w.Truncate(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("truncate after close: err = %v, want ErrClosed", err)
	}

	if after := waitForGoroutines(before); after > before {
		t.Fatalf("goroutines: before = %d, after = %d", before, after)
	}

	_, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.Count != 50 {
		t.Fatalf("result = %+v", result)
	}
}

func TestConcurrentCloseIsSafe(t *testing.T) {
	fs := disk.NewMemFS()
	before := goroutineCount()
	w := newTestWAL(t, fs, false)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = w.AppendPut([]byte("k"), []byte("v"))
			if err := w.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		}()
	}
	wg.Wait()

	if after := waitForGoroutines(before); after > before {
		t.Fatalf("goroutines: before = %d, after = %d", before, after)
	}
}

func TestSyncPersistsBufferedRecords(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, false)
	defer w.Close()

	for i := 0; i < 10; i++ {
		if _, err := w.AppendPut([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	_, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if result.Count != 10 {
		t.Fatalf("result = %+v", result)
	}
}

func TestOpenRejectsEmptyPath(t *testing.T) {
	if _, err := Open(Options{FS: disk.NewMemFS()}); !errors.Is(err, ErrNoPath) {
		t.Fatalf("err = %v, want ErrNoPath", err)
	}
}

func TestAppendRejectsUnknownKind(t *testing.T) {
	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)
	defer w.Close()

	if _, err := w.Append(Record{Kind: ops.OpKind(200), Key: []byte("k")}); !errors.Is(err, ErrInvalidOpKind) {
		t.Fatalf("err = %v, want ErrInvalidOpKind", err)
	}
}

func TestIterateStopsOnCallbackError(t *testing.T) {
	fs := disk.NewMemFS()
	writeSampleLog(t, fs, 5)

	sentinel := errors.New("stop")
	count := 0
	result, err := Iterate(fs, testPath, func(Record) error {
		count++
		if count == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if count != 2 || result.Count != 2 {
		t.Fatalf("count = %d, result = %+v", count, result)
	}
}

func goroutineCount() int {
	runtime.GC()
	return runtime.NumGoroutine()
}

func waitForGoroutines(target int) int {
	deadline := time.Now().Add(2 * time.Second)
	count := runtime.NumGoroutine()
	for count > target && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		count = runtime.NumGoroutine()
	}
	return count
}
