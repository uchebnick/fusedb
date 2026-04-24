package segment

import (
	"bytes"
	"testing"

	"fusedb/internal/disk"
)

func TestOpenSegmentRaw(t *testing.T) {
	fs := disk.NewMemFS()

	segment, err := NewSegment(Options{
		FS:                 fs,
		Dir:                "segments",
		SegmentID:          101,
		Version:            3,
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

	opened, err := OpenSegment(fs, segment.Path())
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if !opened.Frozen() {
		t.Fatal("opened segment should be frozen")
	}
	if opened.Path() != segment.Path() {
		t.Fatalf("path = %q, want %q", opened.Path(), segment.Path())
	}
	if opened.Header != segment.Header {
		t.Fatalf("header mismatch: %#v vs %#v", opened.Header, segment.Header)
	}
	if opened.Footer != segment.Footer {
		t.Fatalf("footer mismatch: %#v vs %#v", opened.Footer, segment.Footer)
	}

	onDisk, err := disk.ReadFile(fs, segment.Path())
	if err != nil {
		t.Fatalf("read on-disk file: %v", err)
	}
	headerBytes, err := EncodeHeader(opened.Header)
	if err != nil {
		t.Fatalf("encode header: %v", err)
	}
	footerBytes, err := EncodeFooter(opened.Footer)
	if err != nil {
		t.Fatalf("encode footer: %v", err)
	}
	dataBytes, err := opened.readSection(opened.Footer.Data)
	if err != nil {
		t.Fatalf("read data section: %v", err)
	}
	bloomBytes, err := EncodeBloomFilter(opened.Bloom)
	if err != nil {
		t.Fatalf("encode bloom: %v", err)
	}
	indexBytes, err := EncodeIndex(opened.Index)
	if err != nil {
		t.Fatalf("encode index: %v", err)
	}
	rebuilt := make([]byte, 0, len(headerBytes)+len(dataBytes)+len(bloomBytes)+len(indexBytes)+len(footerBytes))
	rebuilt = append(rebuilt, headerBytes...)
	rebuilt = append(rebuilt, dataBytes...)
	rebuilt = append(rebuilt, bloomBytes...)
	rebuilt = append(rebuilt, indexBytes...)
	rebuilt = append(rebuilt, footerBytes...)
	if !bytes.Equal(onDisk, rebuilt) {
		t.Fatal("rebuilt file bytes differ from on-disk segment")
	}

	entry, _, ok := opened.Index.FindBlock([]byte("beta"))
	if !ok {
		t.Fatal("expected index hit for beta")
	}
	payload, err := opened.readBlockPayload(entry)
	if err != nil {
		t.Fatalf("read indexed block: %v", err)
	}
	block, err := DecodeBlock(payload)
	if err != nil {
		t.Fatalf("decode indexed block: %v", err)
	}
	value, ok := block.Find([]byte("beta"))
	if !ok {
		t.Fatal("expected beta value")
	}
	if !bytes.Equal(value, []byte("2")) {
		t.Fatalf("beta value = %q, want %q", value, []byte("2"))
	}
}

func TestOpenSegmentCompressed(t *testing.T) {
	fs := disk.NewMemFS()
	dict := mustTestDictionary(t, 31, [][]byte{
		[]byte("tenant=a|region=eu|state=active|count=1"),
		[]byte("tenant=a|region=eu|state=active|count=2"),
		[]byte("tenant=b|region=us|state=disabled|count=9"),
	})

	segment, err := NewSegment(Options{
		FS:                    fs,
		Dir:                   "segments",
		SegmentID:             202,
		Version:               9,
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
	} {
		if err := segment.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if err := segment.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}

	opened, err := OpenSegment(fs, segment.Path())
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if opened.Header != segment.Header {
		t.Fatalf("header mismatch: %#v vs %#v", opened.Header, segment.Header)
	}
	if opened.Footer != segment.Footer {
		t.Fatalf("footer mismatch: %#v vs %#v", opened.Footer, segment.Footer)
	}
	if !opened.Bloom.MayContain([]byte("alpha")) {
		t.Fatal("opened bloom lost alpha")
	}

	entry, _, ok := opened.Index.FindBlock([]byte("delta"))
	if !ok {
		t.Fatal("expected index hit for delta")
	}
	payload, err := opened.readBlockPayload(entry)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	payload, err = dict.Decompress(payload)
	if err != nil {
		t.Fatalf("decompress payload: %v", err)
	}
	block, err := DecodeBlock(payload)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}
	value, ok := block.Find([]byte("delta"))
	if !ok {
		t.Fatal("expected delta value")
	}
	if !bytes.Equal(value, []byte("tenant=b|region=us|state=disabled|count=9")) {
		t.Fatalf("delta value = %q", value)
	}
}
