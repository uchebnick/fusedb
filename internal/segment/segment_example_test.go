package segment

import (
	"bytes"
	"testing"

	"fusedb/internal/compression"
	"fusedb/internal/disk"
)

func TestSegmentEndToEndExample(t *testing.T) {
	fs := disk.DefaultFS
	dir := t.TempDir()

	writer, err := NewSegment(Options{
		FS:                 fs,
		Dir:                dir,
		SegmentID:          9001,
		Version:            1,
		ExpectedKeys:       4,
		TargetBlockSize:    64,
		BloomFalsePositive: 0.01,
		Compression:        CompressionNone,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}

	entries := []BlockEntry{
		{Key: []byte("account:001"), Value: []byte("balance=10")},
		{Key: []byte("account:002"), Value: []byte("balance=20")},
		{Key: []byte("account:003"), Value: []byte("balance=30")},
		{Key: []byte("account:004"), Value: []byte("balance=40")},
	}
	for _, entry := range entries {
		if err := writer.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}

	if err := writer.Freeze(); err != nil {
		t.Fatalf("freeze segment: %v", err)
	}
	if !writer.Frozen() {
		t.Fatal("segment should be frozen after Freeze")
	}

	reader, err := OpenReader(fs, writer.Path(), nil)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}

	value, ok, err := reader.Get([]byte("account:003"))
	if err != nil {
		t.Fatalf("get account:003: %v", err)
	}
	if !ok {
		t.Fatal("expected account:003")
	}
	if !bytes.Equal(value, []byte("balance=30")) {
		t.Fatalf("account:003 value = %q, want balance=30", value)
	}

	scanned := collectExampleEntries(reader.Iter())
	assertEntriesEqual(t, scanned, entries)

	fromAccount2 := collectExampleEntries(reader.IterFrom([]byte("account:002")))
	if len(fromAccount2) == 0 {
		t.Fatal("expected iter from account:002 hit")
	}
	if !bytes.Equal(fromAccount2[0].Key, []byte("account:002")) || !bytes.Equal(fromAccount2[0].Value, []byte("balance=20")) {
		t.Fatalf("iter from account:002 = (%q, %q), want (account:002, balance=20)", fromAccount2[0].Key, fromAccount2[0].Value)
	}
}

func TestCompressedSegmentEndToEndExample(t *testing.T) {
	fs := disk.DefaultFS
	dir := t.TempDir()
	dictDir := t.TempDir()

	entries := []BlockEntry{
		{Key: []byte("account:001"), Value: []byte("tenant=a|region=eu|balance=10")},
		{Key: []byte("account:002"), Value: []byte("tenant=a|region=eu|balance=20")},
		{Key: []byte("account:003"), Value: []byte("tenant=b|region=us|balance=30")},
		{Key: []byte("account:004"), Value: []byte("tenant=b|region=us|balance=40")},
	}

	samples, err := encodedBlockTrainingSamples(entries, 96)
	if err != nil {
		t.Fatalf("build training samples: %v", err)
	}
	dict, err := compression.PretrainDictionary(compression.PretrainOptions{
		ID:      1001,
		Size:    128,
		Level:   3,
		Samples: samples,
	})
	if err != nil {
		t.Fatalf("pretrain dictionary: %v", err)
	}
	defer dict.Close()

	writerRegistry, err := compression.NewPersistentRegistry(fs, dictDir)
	if err != nil {
		t.Fatalf("new writer registry: %v", err)
	}
	if err := writerRegistry.Save(dict); err != nil {
		t.Fatalf("save dictionary: %v", err)
	}

	writer, err := NewSegment(Options{
		FS:                    fs,
		Dir:                   dir,
		SegmentID:             9002,
		Version:               1,
		ExpectedKeys:          len(entries),
		TargetBlockSize:       96,
		BloomFalsePositive:    0.01,
		Compression:           CompressionLZ4Dict,
		CompressionDictionary: dict,
	})
	if err != nil {
		t.Fatalf("new compressed segment: %v", err)
	}

	for _, entry := range entries {
		if err := writer.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if err := writer.Freeze(); err != nil {
		t.Fatalf("freeze compressed segment: %v", err)
	}

	recoveryRegistry, err := compression.NewPersistentRegistry(fs, dictDir)
	if err != nil {
		t.Fatalf("new recovery registry: %v", err)
	}
	defer recoveryRegistry.Close()

	reader, err := OpenReader(fs, writer.Path(), recoveryRegistry)
	if err != nil {
		t.Fatalf("open compressed reader: %v", err)
	}

	value, ok, err := reader.Get([]byte("account:003"))
	if err != nil {
		t.Fatalf("get account:003: %v", err)
	}
	if !ok {
		t.Fatal("expected account:003")
	}
	if !bytes.Equal(value, []byte("tenant=b|region=us|balance=30")) {
		t.Fatalf("account:003 value = %q, want tenant=b|region=us|balance=30", value)
	}

	scanned := collectExampleEntries(reader.Iter())
	assertEntriesEqual(t, scanned, entries)

	fromAccount2 := collectExampleEntries(reader.IterFrom([]byte("account:002")))
	if len(fromAccount2) == 0 {
		t.Fatal("expected iter from account:002 hit")
	}
	if !bytes.Equal(fromAccount2[0].Key, []byte("account:002")) || !bytes.Equal(fromAccount2[0].Value, []byte("tenant=a|region=eu|balance=20")) {
		t.Fatalf("iter from account:002 = (%q, %q), want (account:002, tenant=a|region=eu|balance=20)", fromAccount2[0].Key, fromAccount2[0].Value)
	}
}

func assertEntriesEqual(t *testing.T, got, want []BlockEntry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("entries len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("entry %d = (%q, %q), want (%q, %q)", i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
}

func collectExampleEntries(seq func(func([]byte, []byte) bool)) []BlockEntry {
	var entries []BlockEntry
	for key, value := range seq {
		entries = append(entries, BlockEntry{
			Key:   key,
			Value: value,
		})
	}
	return entries
}

func encodedBlockTrainingSamples(entries []BlockEntry, targetBlockSize int) ([][]byte, error) {
	samples := make([][]byte, 0)
	current := Block{}
	currentSize := rawBlockHeaderSize + rawBlockChecksumSize

	for _, entry := range entries {
		entrySize := rawBlockEntryHeaderSize + len(entry.Key) + len(entry.Value) + 4
		if current.Len() > 0 && currentSize+entrySize > targetBlockSize {
			sample, err := current.MarshalBinary()
			if err != nil {
				return nil, err
			}
			samples = append(samples, sample)
			current = Block{}
			currentSize = rawBlockHeaderSize + rawBlockChecksumSize
		}

		if err := current.Add(entry); err != nil {
			return nil, err
		}
		currentSize += entrySize
	}

	if !current.Empty() {
		sample, err := current.MarshalBinary()
		if err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}
	return samples, nil
}
