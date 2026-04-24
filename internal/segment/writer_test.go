package segment

import (
	"bytes"
	"errors"
	"testing"

	"fusedb/internal/compression"
	"fusedb/internal/disk"
)

func TestSegmentFreezeRaw(t *testing.T) {
	fs := disk.NewMemFS()
	segment, err := NewSegment(Options{
		FS:                 fs,
		Dir:                "segments",
		SegmentID:          11,
		Version:            1,
		TargetBlockSize:    64,
		BloomFalsePositive: 0.01,
		Compression:        CompressionNone,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}

	for _, entry := range []BlockEntry{
		{Key: []byte("alpha"), Value: []byte("tenant=a|state=active|count=1")},
		{Key: []byte("beta"), Value: []byte("tenant=a|state=active|count=2")},
		{Key: []byte("delta"), Value: []byte("tenant=b|state=disabled|count=9")},
	} {
		if err := segment.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if _, err := fs.Stat(SegmentTempFileName("segments", 11, 1)); err != nil {
		t.Fatalf("expected temp file after block writes: %v", err)
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze raw segment: %v", err)
	}

	if !segment.Frozen() {
		t.Fatal("segment should be frozen")
	}
	if segment.Header.Compression != CompressionNone {
		t.Fatalf("compression = %d, want raw", segment.Header.Compression)
	}
	if segment.Header.Version != 1 {
		t.Fatalf("version = %d, want 1", segment.Header.Version)
	}
	if segment.Header.DictionaryID != 0 {
		t.Fatalf("dict id = %d, want 0", segment.Header.DictionaryID)
	}
	if segment.Footer.BlockCount != 3 {
		t.Fatalf("block count = %d, want 3", segment.Footer.BlockCount)
	}
	if segment.Path() != SegmentFileName("segments", 11, 1) {
		t.Fatalf("path = %q, want %q", segment.Path(), SegmentFileName("segments", 11, 1))
	}
	if segment.Len() == 0 {
		t.Fatal("expected encoded segment bytes")
	}
	if _, err := fs.Stat(segment.Path()); err != nil {
		t.Fatalf("expected finalized file: %v", err)
	}

	headerBytes, err := EncodeHeader(segment.Header)
	if err != nil {
		t.Fatalf("encode header: %v", err)
	}
	if len(headerBytes) != headerSize {
		t.Fatalf("header len = %d, want %d", len(headerBytes), headerSize)
	}
	footerBytes, err := EncodeFooter(segment.Footer)
	if err != nil {
		t.Fatalf("encode footer: %v", err)
	}
	if len(footerBytes) != footerSize {
		t.Fatalf("footer len = %d, want %d", len(footerBytes), footerSize)
	}
	dataBytes, err := segment.readSection(segment.Footer.Data)
	if err != nil {
		t.Fatalf("read data section: %v", err)
	}
	if uint64(len(dataBytes)) != segment.Footer.Data.Length {
		t.Fatalf("data len = %d, want %d", len(dataBytes), segment.Footer.Data.Length)
	}

	for i, key := range [][]byte{[]byte("alpha"), []byte("beta"), []byte("delta")} {
		if !segment.Bloom.MayContain(key) {
			t.Fatalf("segment bloom lost key %d %q", i, key)
		}
	}

	for _, tc := range []struct {
		key   []byte
		value []byte
	}{
		{key: []byte("alpha"), value: []byte("tenant=a|state=active|count=1")},
		{key: []byte("beta"), value: []byte("tenant=a|state=active|count=2")},
		{key: []byte("delta"), value: []byte("tenant=b|state=disabled|count=9")},
	} {
		entry, _, ok := segment.Index.FindBlock(tc.key)
		if !ok {
			t.Fatalf("expected index hit for %q", tc.key)
		}
		payload, err := segment.readBlockPayload(entry)
		if err != nil {
			t.Fatalf("read block payload for %q: %v", tc.key, err)
		}
		block, err := DecodeBlock(payload)
		if err != nil {
			t.Fatalf("decode block for %q: %v", tc.key, err)
		}
		value, ok := block.Find(tc.key)
		if !ok {
			t.Fatalf("expected key %q in block", tc.key)
		}
		if !bytes.Equal(value, tc.value) {
			t.Fatalf("value for %q = %q, want %q", tc.key, value, tc.value)
		}
	}

	if err := segment.AppendKV([]byte("omega"), []byte("late")); err != ErrSegmentFrozen {
		t.Fatalf("append after freeze = %v, want %v", err, ErrSegmentFrozen)
	}
}

func TestSegmentFreezeCompressed(t *testing.T) {
	dict := mustTestDictionary(t, 17, [][]byte{
		[]byte("tenant=a|region=eu|state=active|count=1"),
		[]byte("tenant=a|region=eu|state=active|count=2"),
		[]byte("tenant=b|region=us|state=disabled|count=9"),
		[]byte("tenant=b|region=us|state=active|count=12"),
	})

	fs := disk.NewMemFS()
	segment, err := NewSegment(Options{
		FS:                    fs,
		Dir:                   "segments",
		SegmentID:             22,
		Version:               8,
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
		t.Fatalf("freeze compressed segment: %v", err)
	}

	if segment.Header.Compression != CompressionZstdDict {
		t.Fatalf("compression = %d, want zstd dict", segment.Header.Compression)
	}
	if segment.Header.Version != 8 {
		t.Fatalf("version = %d, want 8", segment.Header.Version)
	}
	if segment.Header.DictionaryID != dict.ID() {
		t.Fatalf("dict id = %d, want %d", segment.Header.DictionaryID, dict.ID())
	}
	if segment.Footer.BlockCount == 0 {
		t.Fatal("expected at least one block")
	}
	if segment.Path() != SegmentFileName("segments", 22, 8) {
		t.Fatalf("path = %q, want %q", segment.Path(), SegmentFileName("segments", 22, 8))
	}

	indexBytes, err := segment.readSection(segment.Footer.Index)
	if err != nil {
		t.Fatalf("read index section: %v", err)
	}
	if len(indexBytes) == 0 {
		t.Fatal("expected encoded index bytes")
	}

	decodedIndex, err := DecodeIndex(indexBytes)
	if err != nil {
		t.Fatalf("decode index bytes: %v", err)
	}
	if decodedIndex.Len() != segment.Index.Len() {
		t.Fatalf("decoded index len = %d, want %d", decodedIndex.Len(), segment.Index.Len())
	}

	for _, tc := range []struct {
		key   []byte
		value []byte
	}{
		{key: []byte("alpha"), value: []byte("tenant=a|region=eu|state=active|count=1")},
		{key: []byte("omega"), value: []byte("tenant=b|region=us|state=active|count=12")},
	} {
		entry, _, ok := segment.Index.FindBlock(tc.key)
		if !ok {
			t.Fatalf("expected index hit for %q", tc.key)
		}
		payload, err := segment.readBlockPayload(entry)
		if err != nil {
			t.Fatalf("read payload for %q: %v", tc.key, err)
		}
		payload, err = dict.Decompress(payload)
		if err != nil {
			t.Fatalf("decompress block for %q: %v", tc.key, err)
		}
		block, err := DecodeBlock(payload)
		if err != nil {
			t.Fatalf("decode decompressed block for %q: %v", tc.key, err)
		}
		value, ok := block.Find(tc.key)
		if !ok {
			t.Fatalf("expected key %q in block", tc.key)
		}
		if !bytes.Equal(value, tc.value) {
			t.Fatalf("value for %q = %q, want %q", tc.key, value, tc.value)
		}
	}

	for _, key := range [][]byte{[]byte("alpha"), []byte("beta"), []byte("delta"), []byte("omega")} {
		if !segment.Bloom.MayContain(key) {
			t.Fatalf("segment bloom lost key %q", key)
		}
	}
}

func TestSegmentRejectsUnsortedEntries(t *testing.T) {
	segment, err := NewSegment(Options{
		FS:        disk.NewMemFS(),
		Dir:       "segments",
		SegmentID: 33,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	if err := segment.AppendKV([]byte("beta"), []byte("2")); err != nil {
		t.Fatalf("append beta: %v", err)
	}
	if err := segment.AppendKV([]byte("alpha"), []byte("1")); err != ErrUnsortedSegmentEntries {
		t.Fatalf("append unsorted = %v, want %v", err, ErrUnsortedSegmentEntries)
	}
}

func TestNewSegmentRejectsExistingFinalFile(t *testing.T) {
	fs := disk.NewMemFS()
	segment, err := NewSegment(Options{
		FS:        fs,
		Dir:       "segments",
		SegmentID: 44,
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

	_, err = NewSegment(Options{
		FS:        fs,
		Dir:       "segments",
		SegmentID: 44,
		Version:   1,
	})
	if !errors.Is(err, ErrSegmentFileExists) {
		t.Fatalf("new segment with existing final file = %v, want %v", err, ErrSegmentFileExists)
	}
}

func mustTestDictionary(t *testing.T, id uint32, samples [][]byte) *compression.Dictionary {
	t.Helper()

	raw, err := compression.TrainDictionary(compression.TrainOptions{
		ID:      id,
		Size:    64,
		Level:   3,
		Samples: samples,
	})
	if err != nil {
		t.Fatalf("train dictionary: %v", err)
	}

	dict, err := compression.NewDictionaryLevel(id, raw, 3)
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	t.Cleanup(func() { _ = dict.Close() })
	return dict
}
