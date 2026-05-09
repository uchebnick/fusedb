package disk

import (
	"path/filepath"
	"testing"
)

var benchIOSink byte

func BenchmarkMemFSReadAt4KB(b *testing.B) {
	fs := NewMemFS()
	f, err := fs.Create("bench")
	if err != nil {
		b.Fatal(err)
	}
	data := make([]byte, 4<<10)
	if _, err := f.WriteAt(data, 0); err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, len(data))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.ReadAt(buf, 0); err != nil {
			b.Fatal(err)
		}
		benchIOSink = buf[0]
	}
}

func BenchmarkMemFSWriteAt4KB(b *testing.B) {
	fs := NewMemFS()
	f, err := fs.Create("bench")
	if err != nil {
		b.Fatal(err)
	}
	data := make([]byte, 4<<10)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.WriteAt(data, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOSReadAt4KB(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench")
	f, err := DefaultFS.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	data := make([]byte, 4<<10)
	if _, err := f.WriteAt(data, 0); err != nil {
		b.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		b.Fatal(err)
	}

	buf := make([]byte, len(data))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.ReadAt(buf, 0); err != nil {
			b.Fatal(err)
		}
		benchIOSink = buf[0]
	}
}

func BenchmarkOSWriteAt4KB(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench")
	f, err := DefaultFS.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	data := make([]byte, 4<<10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.WriteAt(data, 0); err != nil {
			b.Fatal(err)
		}
	}
}
