package segment

import (
	"context"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

func TestReaderVerifyReadsEveryBlock(t *testing.T) {
	fs := disk.NewMemFS()
	created, err := NewSegment(Options{
		FS:                 fs,
		Dir:                "segments",
		SegmentID:          401,
		Version:            2,
		ExpectedKeys:       4,
		TargetBlockSize:    32,
		BloomFalsePositive: 0.01,
	})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	for _, entry := range []BlockEntry{
		{Key: []byte("alpha"), Value: []byte("value-alpha")},
		{Key: []byte("beta"), Value: []byte("value-beta")},
		{Key: []byte("delta"), Value: []byte("value-delta")},
		{Key: []byte("omega"), Value: []byte("value-omega")},
	} {
		if err := created.Append(entry); err != nil {
			t.Fatalf("append %q: %v", entry.Key, err)
		}
	}
	if err := created.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	reader, err := OpenReader(fs, created.Path(), nil)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	report, err := reader.Verify(context.Background())
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.Keys != 4 || report.Blocks < 2 || string(report.FirstKey) != "alpha" || string(report.LastKey) != "omega" {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestReaderVerifyDetectsBlockCorruption(t *testing.T) {
	fs := disk.NewMemFS()
	created, err := NewSegment(Options{FS: fs, Dir: "segments", SegmentID: 402, Version: 1, ExpectedKeys: 1})
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	if err := created.Append(BlockEntry{Key: []byte("key"), Value: []byte("value")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := created.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	reader, err := OpenReader(fs, created.Path(), nil)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	file, err := fs.OpenReadWrite(created.Path())
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	one := []byte{0}
	if _, err := file.ReadAt(one, headerSize); err != nil {
		t.Fatalf("read payload byte: %v", err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one, headerSize); err != nil {
		t.Fatalf("corrupt payload byte: %v", err)
	}
	_ = file.Close()

	if _, err := reader.Verify(context.Background()); err == nil {
		t.Fatal("verify accepted a corrupted block")
	}
}
