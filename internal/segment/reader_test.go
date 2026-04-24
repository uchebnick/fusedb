package segment

import (
	"bytes"
	"testing"

	"fusedb/internal/compression"
	"fusedb/internal/disk"
)

func TestReaderGetRaw(t *testing.T) {
	fs := disk.NewMemFS()
	segment, err := NewSegment(Options{
		FS:                 fs,
		Dir:                "segments",
		SegmentID:          301,
		Version:            1,
		ExpectedKeys:       3,
		TargetBlockSize:    64,
		BloomFalsePositive: 0.01,
		Compression:        CompressionNone,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}

	for _, entry := range []BlockEntry{
		{Key: []byte("alpha"), Value: []byte("1")},
		{Key: []byte("beta"), Value: []byte("2")},
		{Key: []byte("delta"), Value: []byte("3")},
	} {
		if err := segment.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	reader, err := NewReader(segment, nil)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	value, ok, err := reader.Get([]byte("beta"))
	if err != nil {
		t.Fatalf("get beta: %v", err)
	}
	if !ok {
		t.Fatal("expected beta value")
	}
	if !bytes.Equal(value, []byte("2")) {
		t.Fatalf("beta value = %q, want %q", value, []byte("2"))
	}

	value, ok, err = reader.Get([]byte("missing"))
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if ok || value != nil {
		t.Fatalf("missing lookup = (%q, %v), want nil,false", value, ok)
	}
}

func TestReaderIteratorRaw(t *testing.T) {
	fs := disk.NewMemFS()
	segment, err := NewSegment(Options{
		FS:                 fs,
		Dir:                "segments",
		SegmentID:          305,
		Version:            1,
		ExpectedKeys:       5,
		TargetBlockSize:    64,
		BloomFalsePositive: 0.01,
		Compression:        CompressionNone,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}

	want := []BlockEntry{
		{Key: []byte("alpha"), Value: []byte("1")},
		{Key: []byte("beta"), Value: []byte("2")},
		{Key: []byte("delta"), Value: []byte("3")},
		{Key: []byte("gamma"), Value: []byte("4")},
		{Key: []byte("omega"), Value: []byte("5")},
	}
	for _, entry := range want {
		if err := segment.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	reader, err := NewReader(segment, nil)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	it := reader.NewIterator()
	defer it.Close()

	var got []BlockEntry
	for ok := it.First(); ok; ok = it.Next() {
		entry, ok := it.Entry()
		if !ok {
			t.Fatal("iterator valid but entry missing")
		}
		got = append(got, entry)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("iterated entries = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("entry %d = (%q,%q), want (%q,%q)", i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}

	if !it.Seek([]byte("charlie")) {
		t.Fatal("expected seek to lower-bound delta")
	}
	if !bytes.Equal(it.Key(), []byte("delta")) || !bytes.Equal(it.Value(), []byte("3")) {
		t.Fatalf("seek charlie = (%q,%q), want delta,3", it.Key(), it.Value())
	}
	if !it.Next() {
		t.Fatal("expected next after delta")
	}
	if !bytes.Equal(it.Key(), []byte("gamma")) {
		t.Fatalf("next key = %q, want gamma", it.Key())
	}
	if it.Seek([]byte("zzz")) {
		t.Fatal("seek past end should be invalid")
	}
	if it.Err() != nil {
		t.Fatalf("seek past end error = %v, want nil", it.Err())
	}
}

func TestOpenReaderCompressed(t *testing.T) {
	fs := disk.NewMemFS()
	dict := mustTestDictionary(t, 41, [][]byte{
		[]byte("tenant=a|region=eu|state=active|count=1"),
		[]byte("tenant=a|region=eu|state=active|count=2"),
		[]byte("tenant=b|region=us|state=disabled|count=9"),
		[]byte("tenant=b|region=us|state=active|count=12"),
	})
	registry := compression.NewRegistry()
	if err := registry.Add(dict); err != nil {
		t.Fatalf("add dictionary to registry: %v", err)
	}

	segment, err := NewSegment(Options{
		FS:                    fs,
		Dir:                   "segments",
		SegmentID:             302,
		Version:               2,
		ExpectedKeys:          4,
		TargetBlockSize:       96,
		BloomFalsePositive:    0.01,
		Compression:           CompressionZstdDict,
		CompressionDictionary: dict,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}

	for _, entry := range []BlockEntry{
		{Key: []byte("alpha"), Value: []byte("tenant=a|region=eu|state=active|count=1")},
		{Key: []byte("beta"), Value: []byte("tenant=a|region=eu|state=active|count=2")},
		{Key: []byte("delta"), Value: []byte("tenant=b|region=us|state=disabled|count=9")},
		{Key: []byte("omega"), Value: []byte("tenant=b|region=us|state=active|count=12")},
	} {
		if err := segment.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	reader, err := OpenReader(fs, segment.Path(), registry)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}

	value, ok, err := reader.Get([]byte("omega"))
	if err != nil {
		t.Fatalf("get omega: %v", err)
	}
	if !ok {
		t.Fatal("expected omega value")
	}
	if !bytes.Equal(value, []byte("tenant=b|region=us|state=active|count=12")) {
		t.Fatalf("omega value = %q", value)
	}
}

func TestReaderIteratorCompressed(t *testing.T) {
	fs := disk.NewMemFS()
	dict := mustTestDictionary(t, 43, [][]byte{
		[]byte("tenant=a|region=eu|state=active|count=1"),
		[]byte("tenant=a|region=eu|state=active|count=2"),
		[]byte("tenant=b|region=us|state=disabled|count=9"),
		[]byte("tenant=b|region=us|state=active|count=12"),
	})
	registry := compression.NewRegistry()
	if err := registry.Add(dict); err != nil {
		t.Fatalf("add dictionary to registry: %v", err)
	}

	segment, err := NewSegment(Options{
		FS:                    fs,
		Dir:                   "segments",
		SegmentID:             306,
		Version:               1,
		ExpectedKeys:          4,
		TargetBlockSize:       96,
		BloomFalsePositive:    0.01,
		Compression:           CompressionZstdDict,
		CompressionDictionary: dict,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}

	want := []BlockEntry{
		{Key: []byte("alpha"), Value: []byte("tenant=a|region=eu|state=active|count=1")},
		{Key: []byte("beta"), Value: []byte("tenant=a|region=eu|state=active|count=2")},
		{Key: []byte("delta"), Value: []byte("tenant=b|region=us|state=disabled|count=9")},
		{Key: []byte("omega"), Value: []byte("tenant=b|region=us|state=active|count=12")},
	}
	for _, entry := range want {
		if err := segment.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	reader, err := OpenReader(fs, segment.Path(), registry)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}

	it := reader.NewIterator()
	defer it.Close()

	for i, ok := 0, it.First(); ok; i, ok = i+1, it.Next() {
		if i >= len(want) {
			t.Fatalf("iterator returned extra key %q", it.Key())
		}
		if !bytes.Equal(it.Key(), want[i].Key) || !bytes.Equal(it.Value(), want[i].Value) {
			t.Fatalf("entry %d = (%q,%q), want (%q,%q)", i, it.Key(), it.Value(), want[i].Key, want[i].Value)
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestOpenReaderCompressedLoadsDictionaryFromPersistentRegistry(t *testing.T) {
	fs := disk.NewMemFS()
	dict := mustTestDictionary(t, 42, [][]byte{
		[]byte("tenant=a|region=eu|state=active|count=1"),
		[]byte("tenant=a|region=eu|state=active|count=2"),
		[]byte("tenant=b|region=us|state=disabled|count=9"),
		[]byte("tenant=b|region=us|state=active|count=12"),
	})

	writerRegistry, err := compression.NewPersistentRegistry(fs, "dicts")
	if err != nil {
		t.Fatalf("new writer registry: %v", err)
	}
	if err := writerRegistry.Save(dict); err != nil {
		t.Fatalf("save dictionary: %v", err)
	}

	segment, err := NewSegment(Options{
		FS:                    fs,
		Dir:                   "segments",
		SegmentID:             304,
		Version:               2,
		ExpectedKeys:          2,
		TargetBlockSize:       96,
		BloomFalsePositive:    0.01,
		Compression:           CompressionZstdDict,
		CompressionDictionary: dict,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	if err := segment.AppendKV([]byte("alpha"), []byte("tenant=a|region=eu|state=active|count=1")); err != nil {
		t.Fatalf("append alpha: %v", err)
	}
	if err := segment.AppendKV([]byte("omega"), []byte("tenant=b|region=us|state=active|count=12")); err != nil {
		t.Fatalf("append omega: %v", err)
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	recoveryRegistry, err := compression.NewPersistentRegistry(fs, "dicts")
	if err != nil {
		t.Fatalf("new recovery registry: %v", err)
	}
	t.Cleanup(func() { _ = recoveryRegistry.Close() })

	reader, err := OpenReader(fs, segment.Path(), recoveryRegistry)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}

	value, ok, err := reader.Get([]byte("omega"))
	if err != nil {
		t.Fatalf("get omega: %v", err)
	}
	if !ok {
		t.Fatal("expected omega value")
	}
	if !bytes.Equal(value, []byte("tenant=b|region=us|state=active|count=12")) {
		t.Fatalf("omega value = %q", value)
	}
}

func TestReaderCompressedRequiresRegistry(t *testing.T) {
	fs := disk.NewMemFS()
	dict := mustTestDictionary(t, 51, [][]byte{
		[]byte("tenant=a|region=eu|state=active|count=1"),
		[]byte("tenant=b|region=us|state=disabled|count=9"),
	})
	segment, err := NewSegment(Options{
		FS:                    fs,
		Dir:                   "segments",
		SegmentID:             303,
		Version:               3,
		ExpectedKeys:          2,
		Compression:           CompressionZstdDict,
		CompressionDictionary: dict,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	if err := segment.AppendKV([]byte("alpha"), []byte("tenant=a|region=eu|state=active|count=1")); err != nil {
		t.Fatalf("append alpha: %v", err)
	}
	if err := segment.AppendKV([]byte("delta"), []byte("tenant=b|region=us|state=disabled|count=9")); err != nil {
		t.Fatalf("append delta: %v", err)
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	if _, err := NewReader(segment, nil); err != ErrMissingDictionaryRegistry {
		t.Fatalf("new reader without registry = %v, want %v", err, ErrMissingDictionaryRegistry)
	}
}
