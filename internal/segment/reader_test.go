package segment

import (
	"bytes"
	"errors"
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

	got := collectReaderEntries(reader.Iter())
	if len(got) != len(want) {
		t.Fatalf("iterated entries = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("entry %d = (%q,%q), want (%q,%q)", i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}

	fromCharlie := collectReaderEntries(reader.IterFrom([]byte("charlie")))
	if len(fromCharlie) < 2 {
		t.Fatalf("iter from charlie returned %d entries, want at least 2", len(fromCharlie))
	}
	if !bytes.Equal(fromCharlie[0].Key, []byte("delta")) || !bytes.Equal(fromCharlie[0].Value, []byte("3")) {
		t.Fatalf("iter from charlie first = (%q,%q), want delta,3", fromCharlie[0].Key, fromCharlie[0].Value)
	}
	if !bytes.Equal(fromCharlie[1].Key, []byte("gamma")) {
		t.Fatalf("iter from charlie second key = %q, want gamma", fromCharlie[1].Key)
	}
	if got := collectReaderEntries(reader.IterFrom([]byte("zzz"))); len(got) != 0 {
		t.Fatalf("iter from zzz = %v, want empty", got)
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
	t.Cleanup(func() { _ = reader.Close() })

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
	t.Cleanup(func() { _ = reader.Close() })

	got := collectReaderEntries(reader.Iter())
	if len(got) != len(want) {
		t.Fatalf("iterated entries = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("entry %d = (%q,%q), want (%q,%q)", i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
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
	t.Cleanup(func() { _ = reader.Close() })

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

func TestReaderCloseReleasesReader(t *testing.T) {
	fs := disk.NewMemFS()
	segment, err := NewSegment(Options{
		FS:        fs,
		Dir:       "segments",
		SegmentID: 307,
		Version:   1,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	if err := segment.AppendKV([]byte("alpha"), []byte("1")); err != nil {
		t.Fatalf("append alpha: %v", err)
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	reader, err := OpenReader(fs, segment.Path(), nil)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	if _, _, err := reader.Get([]byte("alpha")); err != ErrNilSegment {
		t.Fatalf("get after close = %v, want %v", err, ErrNilSegment)
	}
}

func TestReaderRequiresOpenFile(t *testing.T) {
	fs := disk.NewMemFS()
	segment, err := NewSegment(Options{
		FS:        fs,
		Dir:       "segments",
		SegmentID: 308,
		Version:   1,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	if err := segment.AppendKV([]byte("alpha"), []byte("1")); err != nil {
		t.Fatalf("append alpha: %v", err)
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	reader := &Reader{segment: segment}

	_, _, err = reader.Get([]byte("alpha"))
	if !errors.Is(err, ErrReaderFileNotOpen) {
		t.Fatalf("get without open file = %v, want %v", err, ErrReaderFileNotOpen)
	}
}

func collectReaderEntries(seq func(func([]byte, []byte) bool)) []BlockEntry {
	var entries []BlockEntry
	for key, value := range seq {
		entries = append(entries, BlockEntry{
			Key:   key,
			Value: value,
		})
	}
	return entries
}
