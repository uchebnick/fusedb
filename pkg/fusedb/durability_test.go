package fusedb_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

// openDB opens a database in a temporary directory with test-sized limits.
//
// The leaf and merge sizes are deliberately small so that split and merge paths
// are exercised by test-sized data instead of only by production volumes.
func openDB(t *testing.T, dir string) *fusedb.DB {
	t.Helper()

	db, err := fusedb.Open(fusedb.Options{
		Dir:         dir,
		CacheSize:   256 << 10,
		MergeSize:   64 << 10,
		MaxLeafSize: 128 << 10,
	})
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return db
}

func key(i int) []byte {
	return []byte(fmt.Sprintf("key:%08d", i))
}

func payload(i int) []byte {
	return []byte(fmt.Sprintf("value-%08d-%s", i, "0123456789abcdef0123456789abcdef"))
}

// TestReopenPreservesData is the property the engine previously failed
// outright: data written before a clean close must still be readable after
// reopening the same directory.
func TestReopenPreservesData(t *testing.T) {
	dir := t.TempDir()
	const count = 2000

	db := openDB(t, dir)
	for i := range count {
		if err := db.Put(key(i), payload(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openDB(t, dir)
	defer reopened.Close()

	for i := range count {
		got, found, err := reopened.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("key %d missing after reopen", i)
		}
		if string(got) != string(payload(i)) {
			t.Fatalf("key %d: got %q want %q", i, got, payload(i))
		}
	}
}

// TestReopenAndWriteRepeatedly covers the second half of the old failure: after
// a restart the engine used to refuse to merge because the segment file for the
// version it recomputed already existed.
func TestReopenAndWriteRepeatedly(t *testing.T) {
	dir := t.TempDir()
	const rounds = 5
	const perRound = 400

	for round := range rounds {
		db := openDB(t, dir)
		for i := range perRound {
			id := round*perRound + i
			if err := db.Put(key(id), payload(id)); err != nil {
				t.Fatalf("round %d put %d: %v", round, id, err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatalf("round %d close: %v", round, err)
		}
	}

	db := openDB(t, dir)
	defer db.Close()
	for i := range rounds * perRound {
		got, found, err := db.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("key %d written in an earlier session is missing", i)
		}
		if string(got) != string(payload(i)) {
			t.Fatalf("key %d: got %q want %q", i, got, payload(i))
		}
	}
}

// TestCrashRecoveryReplaysLog simulates a crash by abandoning the database
// without closing it, which leaves the writes only in the log.
func TestCrashRecoveryReplaysLog(t *testing.T) {
	dir := t.TempDir()
	const count = 500

	db, err := fusedb.Open(fusedb.Options{
		Dir:           dir,
		MergeSize:     1 << 30, // never merge, so nothing reaches a segment
		WALSyncWrites: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := range count {
		if err := db.Put(key(i), payload(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Deliberately not closed: this is what a crash leaves behind.

	recovered := openDB(t, dir)
	defer recovered.Close()

	for i := range count {
		got, found, err := recovered.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("key %d lost after crash: log was not replayed", i)
		}
		if string(got) != string(payload(i)) {
			t.Fatalf("key %d: got %q want %q", i, got, payload(i))
		}
	}
}

// TestCountersSurviveRestart guards the subtlest recovery bug available here:
// replaying a log record that a segment already contains would double the
// counter rather than leave it alone.
func TestCountersSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	const counters = 50
	const increments = 20

	db := openDB(t, dir)
	for range increments {
		for c := range counters {
			if err := db.Inc([]byte(fmt.Sprintf("counter:%03d", c)), 1); err != nil {
				t.Fatalf("inc: %v", err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen, add more, close again: the second session must build on the
	// first, not restart from zero and not double what it inherited.
	db = openDB(t, dir)
	for c := range counters {
		if err := db.Inc([]byte(fmt.Sprintf("counter:%03d", c)), 5); err != nil {
			t.Fatalf("inc: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	final := openDB(t, dir)
	defer final.Close()
	for c := range counters {
		raw, found, err := final.Get([]byte(fmt.Sprintf("counter:%03d", c)))
		if err != nil {
			t.Fatalf("get counter %d: %v", c, err)
		}
		if !found {
			t.Fatalf("counter %d missing", c)
		}
		got := decodeCounter(t, raw)
		if want := int64(increments + 5); got != want {
			t.Fatalf("counter %d: got %d want %d", c, got, want)
		}
	}
}

// decodeCounter reads the varint counter encoding the engine stores for Inc.
func decodeCounter(t *testing.T, raw []byte) int64 {
	t.Helper()

	if len(raw) == 0 {
		t.Fatalf("empty counter payload")
	}
	// The trailing byte is the value kind tag.
	value, n := binary.Varint(raw[:len(raw)-1])
	if n <= 0 {
		t.Fatalf("bad counter encoding %v", raw)
	}
	return value
}

// TestDeletesSurviveRestart checks that a tombstone is not resurrected by a
// merge or by log replay.
func TestDeletesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	const count = 600

	db := openDB(t, dir)
	for i := range count {
		if err := db.Put(key(i), payload(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for i := 0; i < count; i += 2 {
		if err := db.Delete(key(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openDB(t, dir)
	defer reopened.Close()

	for i := range count {
		_, found, err := reopened.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if want := i%2 == 1; found != want {
			t.Fatalf("key %d: found=%v want %v", i, found, want)
		}
	}
}

// TestSplitProducesMultipleSegments verifies the point of the leaf tree: enough
// data must end up spread across more than one segment file.
func TestSplitProducesMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	const count = 20000

	db := openDB(t, dir)
	for i := range count {
		if err := db.Put(key(i), payload(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	stats := db.Stats()
	if stats.Leaves < 2 {
		t.Fatalf("expected the keyspace to split, got %d leaf", stats.Leaves)
	}
	t.Logf("leaves after %d keys: %d", count, stats.Leaves)

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segments, err := filepath.Glob(filepath.Join(dir, "segment-*.seg"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("expected multiple segment files, got %d: %v", len(segments), segments)
	}

	reopened := openDB(t, dir)
	defer reopened.Close()
	for i := range count {
		got, found, err := reopened.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("key %d lost across split", i)
		}
		if string(got) != string(payload(i)) {
			t.Fatalf("key %d: got %q want %q", i, got, payload(i))
		}
	}
}

// TestConcurrentWritesDuringMerge is the race-detector workload: writers keep
// going while merges and splits reshape the tree underneath them.
func TestConcurrentWritesDuringMerge(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	defer db.Close()

	const writers = 8
	const perWriter = 1500

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range perWriter {
				id := worker*perWriter + i
				if err := db.Put(key(id), payload(id)); err != nil {
					t.Errorf("writer %d put %d: %v", worker, id, err)
					return
				}
			}
		}(w)
	}

	// A concurrent reader keeps the lookup path under load while the tree is
	// being restructured.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(1))
		for range writers * perWriter {
			if _, _, err := db.Get(key(rng.Intn(writers * perWriter))); err != nil {
				t.Errorf("concurrent get: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	for i := range writers * perWriter {
		got, found, err := db.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("key %d lost under concurrent merge", i)
		}
		if string(got) != string(payload(i)) {
			t.Fatalf("key %d: got %q want %q", i, got, payload(i))
		}
	}
}

// TestConcurrentIncrementsAreExact checks that counter coalescing does not lose
// or duplicate increments when many goroutines hit the same key.
func TestConcurrentIncrementsAreExact(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	defer db.Close()

	const workers = 8
	const perWorker = 2000
	counter := []byte("shared:counter")

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				if err := db.Inc(counter, 1); err != nil {
					t.Errorf("inc: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	raw, found, err := db.Get(counter)
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	if !found {
		t.Fatal("counter missing")
	}
	if got, want := decodeCounter(t, raw), int64(workers*perWorker); got != want {
		t.Fatalf("counter: got %d want %d", got, want)
	}
}

// TestOverwritesKeepLatestValue exercises the merge resolution path: a key
// rewritten many times, across merges, must read back as its last value.
func TestOverwritesKeepLatestValue(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	defer db.Close()

	const keys = 200
	const generations = 12

	for generation := range generations {
		for i := range keys {
			value := fmt.Appendf(nil, "gen-%02d-key-%04d-%s", generation, i, "padding-padding-padding")
			if err := db.Put(key(i), value); err != nil {
				t.Fatalf("gen %d put %d: %v", generation, i, err)
			}
		}
		if err := db.Merge(); err != nil {
			t.Fatalf("gen %d merge: %v", generation, err)
		}
	}

	for i := range keys {
		want := fmt.Sprintf("gen-%02d-key-%04d-%s", generations-1, i, "padding-padding-padding")
		got, found, err := db.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("key %d missing", i)
		}
		if string(got) != want {
			t.Fatalf("key %d: got %q want %q", i, got, want)
		}
	}
}

// TestBinaryKeys covers inputs that used to be mishandled: keys with NUL bytes,
// high bytes, and an empty value.
func TestBinaryKeys(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	defer db.Close()

	cases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"nul bytes", []byte{0x00, 0x01, 0x00, 0xff}, []byte("binary")},
		{"empty value", []byte("has-empty-value"), []byte{}},
		{"high bytes", []byte{0xff, 0xfe, 0xfd}, []byte{0x00, 0xff}},
	}

	for _, tc := range cases {
		if err := db.Put(tc.key, tc.value); err != nil {
			t.Fatalf("%s: put: %v", tc.name, err)
		}
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	for _, tc := range cases {
		got, found, err := db.Get(tc.key)
		if err != nil {
			t.Fatalf("%s: get: %v", tc.name, err)
		}
		if !found {
			t.Fatalf("%s: key missing after merge", tc.name)
		}
		if string(got) != string(tc.value) {
			t.Fatalf("%s: got %q want %q", tc.name, got, tc.value)
		}
	}
}

// TestEmptyKeyIsRejected pins the one input the engine does not accept.
//
// The block format encodes a key length that cannot be zero, so an empty key
// has to be refused at the door. What matters is that it is refused cleanly:
// this used to panic inside the value cache.
func TestEmptyKeyIsRejected(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	defer db.Close()

	if err := db.Put(nil, []byte("value")); !errors.Is(err, fusedb.ErrEmptyKey) {
		t.Fatalf("put with empty key: got %v, want ErrEmptyKey", err)
	}
	if err := db.Delete([]byte{}); !errors.Is(err, fusedb.ErrEmptyKey) {
		t.Fatalf("delete with empty key: got %v, want ErrEmptyKey", err)
	}
	if err := db.Inc([]byte{}, 1); !errors.Is(err, fusedb.ErrEmptyKey) {
		t.Fatalf("inc with empty key: got %v, want ErrEmptyKey", err)
	}

	// A read of a key that can never exist is a miss, not an error.
	if _, found, err := db.Get([]byte{}); err != nil || found {
		t.Fatalf("get with empty key: found=%v err=%v, want a clean miss", found, err)
	}
}

// TestCallerBuffersAreNotAliased pins the ownership contract in both
// directions: the engine must not write into a caller's buffer, and a caller
// must not be able to reach into the engine through a returned value.
func TestCallerBuffersAreNotAliased(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	defer db.Close()

	backing := []byte("VALUE-AND-MORE-BYTES")
	if err := db.Put([]byte("owned"), backing[:5]); err != nil {
		t.Fatalf("put: %v", err)
	}
	if string(backing) != "VALUE-AND-MORE-BYTES" {
		t.Fatalf("engine wrote into the caller's buffer: %q", backing)
	}

	first, _, err := db.Get([]byte("owned"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	first[0] = 'X'

	second, _, err := db.Get([]byte("owned"))
	if err != nil {
		t.Fatalf("get again: %v", err)
	}
	if string(second) != "VALUE" {
		t.Fatalf("mutating a returned value corrupted the database: %q", second)
	}
}

// TestStatsReportProgress is a light sanity check that the reported buffer size
// actually tracks writes and merges.
func TestStatsReportProgress(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	defer db.Close()

	for i := range 200 {
		if err := db.Put(key(i), payload(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if buffered := db.Stats().BufferedBytes; buffered == 0 {
		t.Fatal("expected buffered bytes after writes")
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if buffered := db.Stats().BufferedBytes; buffered != 0 {
		t.Fatalf("expected an empty buffer after merge, got %d", buffered)
	}
}
