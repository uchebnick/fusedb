package backup

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

var errInjectedIO = errors.New("injected backup I/O failure")

type faultFS struct {
	disk.FS
	op    string
	fired bool
}

func (f *faultFS) fail(op string) bool {
	if f.op != op || f.fired {
		return false
	}
	f.fired = true
	return true
}

func (f *faultFS) Create(name string) (disk.File, error) {
	if f.fail("create") {
		return nil, errInjectedIO
	}
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, faults: f}, nil
}

func (f *faultFS) Rename(oldName, newName string) error {
	if f.fail("rename") {
		return errInjectedIO
	}
	return f.FS.Rename(oldName, newName)
}

func (f *faultFS) SyncDir(dir string) error {
	if f.fail("sync-dir") {
		return errInjectedIO
	}
	return f.FS.SyncDir(dir)
}

type faultFile struct {
	disk.File
	faults *faultFS
}

func (f *faultFile) Write(p []byte) (int, error) {
	if f.faults.fail("write") {
		limit := min(3, len(p))
		n, _ := f.File.Write(p[:limit])
		return n, errInjectedIO
	}
	return f.File.Write(p)
}

func (f *faultFile) Sync() error {
	if f.faults.fail("sync") {
		return errInjectedIO
	}
	return f.File.Sync()
}

func (f *faultFile) Close() error {
	if f.faults.fail("close") {
		return errInjectedIO
	}
	return f.File.Close()
}

func TestWriteInspectRestoreRoundTrip(t *testing.T) {
	fs := disk.NewMemFS()
	sources := []Source{
		{Name: "MANIFEST", Data: []byte("manifest-state")},
		{Name: "wal.log", Data: []byte("wal-state")},
		{Name: "segment-1.seg", Data: bytes.Repeat([]byte("segment"), 20_000)},
		{Name: "dictionaries/dict-00000007.zdict", Data: []byte("dictionary")},
	}

	written, err := Write(context.Background(), fs, "backups/db.fbak", sources)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	inspected, err := Inspect(context.Background(), fs, "backups/db.fbak")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspected.Bytes != written.Bytes || len(inspected.Entries) != len(sources) {
		t.Fatalf("inspect = %+v, written = %+v", inspected, written)
	}

	if _, err := Restore(context.Background(), fs, "backups/db.fbak", "restored"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	for _, source := range sources {
		got, err := disk.ReadFile(fs, "restored/"+source.Name)
		if err != nil {
			t.Fatalf("read %s: %v", source.Name, err)
		}
		if !bytes.Equal(got, source.Data) {
			t.Fatalf("restored %s differs", source.Name)
		}
	}
}

func TestInspectRejectsPayloadAndFooterCorruption(t *testing.T) {
	for _, tc := range []struct {
		name    string
		offset  int64
		wantErr error
	}{
		{name: "payload", offset: archiveHeaderSize + entryHeaderSize + int64(len("MANIFEST")), wantErr: ErrEntryChecksum},
		{name: "footer", offset: -1, wantErr: ErrArchiveChecksum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := disk.NewMemFS()
			_, err := Write(context.Background(), fs, "backup.fbak", []Source{
				{Name: "MANIFEST", Data: []byte("manifest")},
				{Name: "wal.log", Data: []byte("wal")},
			})
			if err != nil {
				t.Fatalf("write: %v", err)
			}
			file, err := fs.OpenReadWrite("backup.fbak")
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			offset := tc.offset
			if offset < 0 {
				info, statErr := file.Stat()
				if statErr != nil {
					t.Fatalf("stat: %v", statErr)
				}
				offset = info.Size() - 1
			}
			if _, err := file.WriteAt([]byte{0xff}, offset); err != nil {
				t.Fatalf("corrupt: %v", err)
			}
			_ = file.Close()

			if _, err := Inspect(context.Background(), fs, "backup.fbak"); !errors.Is(err, tc.wantErr) {
				t.Fatalf("inspect error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestRestoreRequiresCompleteArchiveAndEmptyDestination(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sources []Source
		wantErr error
	}{
		{name: "manifest", sources: []Source{{Name: "wal.log", Data: []byte("wal")}}, wantErr: ErrMissingManifest},
		{name: "wal", sources: []Source{{Name: "MANIFEST", Data: []byte("manifest")}}, wantErr: ErrMissingWAL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := disk.NewMemFS()
			if _, err := Write(context.Background(), fs, "backup.fbak", tc.sources); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, err := Restore(context.Background(), fs, "backup.fbak", "restore"); !errors.Is(err, tc.wantErr) {
				t.Fatalf("restore error = %v, want %v", err, tc.wantErr)
			}
		})
	}

	fs := disk.NewMemFS()
	if _, err := Write(context.Background(), fs, "backup.fbak", []Source{
		{Name: "MANIFEST", Data: []byte("manifest")},
		{Name: "wal.log", Data: []byte("wal")},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := disk.WriteFileAtomically(fs, "restore/unrelated", []byte("keep")); err != nil {
		t.Fatalf("seed destination: %v", err)
	}
	if _, err := Restore(context.Background(), fs, "backup.fbak", "restore"); !errors.Is(err, ErrDestinationNotEmpty) {
		t.Fatalf("restore error = %v, want ErrDestinationNotEmpty", err)
	}
}

func TestCancellationCleansTemporaryAndPartialFiles(t *testing.T) {
	fs := disk.NewMemFS()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Write(cancelled, fs, "backup.fbak", []Source{{Name: "MANIFEST", Data: []byte("state")}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("write error = %v, want context.Canceled", err)
	}
	if _, err := fs.Stat("backup.fbak.tmp"); err == nil {
		t.Fatal("write left a temporary file")
	}

	if _, err := Write(context.Background(), fs, "backup.fbak", []Source{
		{Name: "MANIFEST", Data: []byte("manifest")},
		{Name: "wal.log", Data: []byte("wal")},
	}); err != nil {
		t.Fatalf("seed backup: %v", err)
	}
	if _, err := Restore(cancelled, fs, "backup.fbak", "restore"); !errors.Is(err, context.Canceled) {
		t.Fatalf("restore error = %v, want context.Canceled", err)
	}
	if _, err := fs.Stat("restore/MANIFEST"); err == nil {
		t.Fatal("cancelled restore installed MANIFEST")
	}
}

func TestEntryNamesCannotEscapeDestination(t *testing.T) {
	for _, name := range []string{"../escape", "/absolute", "a/../../escape", `a\b`} {
		if _, err := Write(context.Background(), disk.NewMemFS(), "backup.fbak", []Source{{Name: name, Data: []byte("x")}}); !errors.Is(err, ErrUnsafeEntryName) {
			t.Fatalf("Write name %q error = %v, want ErrUnsafeEntryName", name, err)
		}
	}
}

func TestArchiveAndRestoreLocksRejectConcurrentOwners(t *testing.T) {
	fs := disk.NewMemFS()
	archiveLock, err := fs.Lock("backup.fbak.lock")
	if err != nil {
		t.Fatalf("lock archive: %v", err)
	}
	if _, err := Write(context.Background(), fs, "backup.fbak", []Source{{Name: "MANIFEST", Data: []byte("x")}}); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("concurrent Write error = %v, want disk.ErrLocked", err)
	}
	_ = archiveLock.Close()

	if _, err := Write(context.Background(), fs, "backup.fbak", []Source{
		{Name: "MANIFEST", Data: []byte("manifest")},
		{Name: "wal.log", Data: []byte("wal")},
	}); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	restoreLock, err := fs.Lock("restore/LOCK")
	if err != nil {
		t.Fatalf("lock restore: %v", err)
	}
	defer restoreLock.Close()
	if _, err := Restore(context.Background(), fs, "backup.fbak", "restore"); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("concurrent Restore error = %v, want disk.ErrLocked", err)
	}
}

func TestWriteFaultMatrixNeverPublishesPartialArchive(t *testing.T) {
	for _, operation := range []string{"create", "write", "sync", "close", "rename", "sync-dir"} {
		t.Run(operation, func(t *testing.T) {
			base := disk.NewMemFS()
			path := "backups/db.fbak"
			oldSources := []Source{{Name: "MANIFEST", Data: []byte("old")}, {Name: "wal.log", Data: []byte("old-wal")}}
			if _, err := Write(context.Background(), base, path, oldSources); err != nil {
				t.Fatalf("seed archive: %v", err)
			}
			faults := &faultFS{FS: base, op: operation}
			_, err := Write(context.Background(), faults, path, []Source{
				{Name: "MANIFEST", Data: bytes.Repeat([]byte("new"), 100)},
				{Name: "wal.log", Data: bytes.Repeat([]byte("new-wal"), 100)},
			})
			if err == nil || !faults.fired {
				t.Fatalf("fault %q was not returned: %v", operation, err)
			}
			if operation == "sync-dir" && !errors.Is(err, disk.ErrCommitUncertain) {
				t.Fatalf("sync-dir error = %v, want ErrCommitUncertain", err)
			}
			if _, inspectErr := Inspect(context.Background(), base, path); inspectErr != nil {
				t.Fatalf("published archive is partial: %v", inspectErr)
			}
			if _, statErr := base.Stat(path + ".tmp"); statErr == nil {
				t.Fatal("temporary archive was not cleaned up")
			}
		})
	}
}

func TestRestoreFaultMatrixNeverInstallsManifestEarly(t *testing.T) {
	for _, operation := range []string{"create", "write", "sync", "close", "rename", "sync-dir"} {
		t.Run(operation, func(t *testing.T) {
			base := disk.NewMemFS()
			if _, err := Write(context.Background(), base, "backup.fbak", []Source{
				{Name: "MANIFEST", Data: []byte("manifest")},
				{Name: "wal.log", Data: bytes.Repeat([]byte("wal"), 100)},
				{Name: "segment-1.seg", Data: bytes.Repeat([]byte("segment"), 100)},
			}); err != nil {
				t.Fatalf("seed backup: %v", err)
			}
			faults := &faultFS{FS: base, op: operation}
			_, err := Restore(context.Background(), faults, "backup.fbak", "restore")
			if err == nil || !faults.fired {
				t.Fatalf("fault %q was not returned: %v", operation, err)
			}
			if operation == "sync-dir" && !errors.Is(err, disk.ErrCommitUncertain) {
				t.Fatalf("sync-dir error = %v, want ErrCommitUncertain", err)
			}
			if _, statErr := base.Stat("restore/MANIFEST"); statErr == nil {
				t.Fatal("failed restore installed MANIFEST")
			}
		})
	}
}

func FuzzInspectNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("FBAK"))
	f.Add(bytes.Repeat([]byte{0xff}, archiveHeaderSize+archiveFooterSize))

	seedFS := disk.NewMemFS()
	if _, err := Write(context.Background(), seedFS, "seed.fbak", []Source{
		{Name: "MANIFEST", Data: []byte("manifest")},
		{Name: "wal.log", Data: []byte("wal")},
	}); err != nil {
		f.Fatalf("create valid seed: %v", err)
	}
	valid, err := disk.ReadFile(seedFS, "seed.fbak")
	if err != nil {
		f.Fatalf("read valid seed: %v", err)
	}
	f.Add(valid)

	f.Fuzz(func(t *testing.T, data []byte) {
		fs := disk.NewMemFS()
		if err := disk.WriteFileAtomically(fs, "fuzz.fbak", data); err != nil {
			t.Fatalf("seed fuzz archive: %v", err)
		}
		_, _ = Inspect(context.Background(), fs, "fuzz.fbak")
	})
}
