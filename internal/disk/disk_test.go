package disk

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReadWriteFileAtomically(t *testing.T) {
	fs := NewMemFS()
	name := "state/dict.bin"
	want := []byte("hello world")

	if err := WriteFileAtomically(fs, name, want); err != nil {
		t.Fatalf("write file atomically: %v", err)
	}

	got, err := ReadFile(fs, name)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read file = %q, want %q", got, want)
	}
}

func TestReadFileLimitedRejectsBeforeAllocation(t *testing.T) {
	fs := NewMemFS()
	if err := WriteFileAtomically(fs, "state.bin", []byte("sixsix")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFileLimited(fs, "state.bin", 5); !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("ReadFileLimited error = %v, want ErrFileTooLarge", err)
	}
	if _, err := ReadFileLimited(fs, "state.bin", -1); !errors.Is(err, ErrInvalidReadLimit) {
		t.Fatalf("negative limit error = %v, want ErrInvalidReadLimit", err)
	}
}

type zeroProgressReadFS struct{ FS }

func (f zeroProgressReadFS) Open(name string) (File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return zeroProgressReadFile{File: file}, nil
}

type zeroProgressReadFile struct{ File }

func (f zeroProgressReadFile) ReadAt([]byte, int64) (int, error) { return 0, nil }

func TestReadFileRejectsZeroProgress(t *testing.T) {
	base := NewMemFS()
	if err := WriteFileAtomically(base, "state.bin", []byte("content")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(zeroProgressReadFS{FS: base}, "state.bin"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadFile error = %v, want io.ErrUnexpectedEOF", err)
	}
}

var errInjectedIO = errors.New("injected I/O failure")

type atomicFaultFS struct {
	FS
	op    string
	fired bool
}

func (f *atomicFaultFS) fail(op string) bool {
	if f.op != op || f.fired {
		return false
	}
	f.fired = true
	return true
}

func (f *atomicFaultFS) Create(name string) (File, error) {
	if f.fail("create") {
		return nil, errInjectedIO
	}
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &atomicFaultFile{File: file, faults: f}, nil
}

func (f *atomicFaultFS) Rename(oldName, newName string) error {
	if f.fail("rename") {
		return errInjectedIO
	}
	return f.FS.Rename(oldName, newName)
}

func (f *atomicFaultFS) SyncDir(dir string) error {
	if f.fail("sync-dir") {
		return errInjectedIO
	}
	return f.FS.SyncDir(dir)
}

type atomicFaultFile struct {
	File
	faults *atomicFaultFS
}

func (f *atomicFaultFile) Write(p []byte) (int, error) {
	if f.faults.fail("write") {
		limit := min(3, len(p))
		n, _ := f.File.Write(p[:limit])
		return n, errInjectedIO
	}
	return f.File.Write(p)
}

func (f *atomicFaultFile) Sync() error {
	if f.faults.fail("sync") {
		return errInjectedIO
	}
	return f.File.Sync()
}

func (f *atomicFaultFile) Close() error {
	if f.faults.fail("close") {
		return errInjectedIO
	}
	return f.File.Close()
}

func TestWriteFileAtomicallyFaultMatrix(t *testing.T) {
	for _, operation := range []string{"create", "write", "sync", "close", "rename", "sync-dir"} {
		t.Run(operation, func(t *testing.T) {
			base := NewMemFS()
			path := "state/MANIFEST"
			oldData := []byte("complete-old-state")
			newData := []byte("complete-new-state-with-more-bytes")
			if err := WriteFileAtomically(base, path, oldData); err != nil {
				t.Fatalf("seed old state: %v", err)
			}

			faults := &atomicFaultFS{FS: base, op: operation}
			err := WriteFileAtomically(faults, path, newData)
			if err == nil || !faults.fired {
				t.Fatalf("fault %q was not returned: %v", operation, err)
			}
			if operation == "sync-dir" && !errors.Is(err, ErrCommitUncertain) {
				t.Fatalf("sync-dir error = %v, want ErrCommitUncertain", err)
			}

			got, readErr := ReadFile(base, path)
			if readErr != nil {
				t.Fatalf("read target: %v", readErr)
			}
			if !bytes.Equal(got, oldData) && !bytes.Equal(got, newData) {
				t.Fatalf("target is partial: %q", got)
			}
			if operation == "sync-dir" && !bytes.Equal(got, newData) {
				t.Fatalf("renamed target = %q, want complete new state", got)
			}
			if _, statErr := base.Stat(path + ".tmp"); statErr == nil {
				t.Fatal("temporary file was not cleaned up")
			}
		})
	}
}

type pathologicalWriter struct {
	mode  string
	data  []byte
	calls int
}

func (w *pathologicalWriter) Write(p []byte) (int, error) {
	w.calls++
	switch w.mode {
	case "zero":
		return 0, nil
	case "invalid":
		return len(p) + 1, nil
	case "short":
		if len(p) > 1 && w.calls == 1 {
			w.data = append(w.data, p[:1]...)
			return 1, nil
		}
	}
	w.data = append(w.data, p...)
	return len(p), nil
}

func TestWriteAllHandlesPathologicalWriters(t *testing.T) {
	for _, test := range []struct {
		mode    string
		wantErr error
	}{
		{mode: "short"},
		{mode: "zero", wantErr: io.ErrShortWrite},
		{mode: "invalid", wantErr: io.ErrShortWrite},
	} {
		t.Run(test.mode, func(t *testing.T) {
			writer := &pathologicalWriter{mode: test.mode}
			err := WriteAll(writer, []byte("complete"))
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if test.wantErr == nil && string(writer.data) != "complete" {
				t.Fatalf("data = %q", writer.data)
			}
		})
	}
}

type pathologicalWriterAt struct {
	pathologicalWriter
}

func (w *pathologicalWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	return w.Write(p)
}

func TestWriteAllAtReportsProgressAndRejectsInvalidCounts(t *testing.T) {
	for _, test := range []struct {
		mode        string
		wantWritten int
		wantErr     error
	}{
		{mode: "short", wantWritten: len("complete")},
		{mode: "zero", wantErr: io.ErrShortWrite},
		{mode: "invalid", wantErr: io.ErrShortWrite},
	} {
		t.Run(test.mode, func(t *testing.T) {
			writer := &pathologicalWriterAt{pathologicalWriter: pathologicalWriter{mode: test.mode}}
			written, err := WriteAllAt(writer, []byte("complete"), 17)
			if written != test.wantWritten || !errors.Is(err, test.wantErr) {
				t.Fatalf("WriteAllAt = (%d,%v), want (%d,%v)", written, err, test.wantWritten, test.wantErr)
			}
		})
	}
}
