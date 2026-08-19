package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/backup"
	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/dbformat"
	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/scheduler"
	"github.com/uchebnick/fusedb/internal/wal"
)

func TestOpenMigratesLegacyManifestsBeforePublishingFormat(t *testing.T) {
	for _, version := range []uint32{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			fs := disk.NewMemFS()
			dir := "legacy-manifest"
			if err := disk.WriteFileAtomically(fs, manifest.FileName(dir), encodeLegacyEmptyManifest(version)); err != nil {
				t.Fatal(err)
			}

			db, err := OpenDB(DBOptions{Dir: dir, FS: fs, DisableWAL: true})
			if err != nil {
				t.Fatalf("open v%d: %v", version, err)
			}
			if got := db.Format().Epoch; got != dbformat.CurrentEpoch {
				t.Fatalf("format epoch = %d, want %d", got, dbformat.CurrentEpoch)
			}
			persistedFormat, err := dbformat.Load(fs, dir)
			if err != nil {
				t.Fatal(err)
			}
			if persistedFormat != dbformat.Current() {
				t.Fatalf("format = %+v, want %+v", persistedFormat, dbformat.Current())
			}
			persistedManifest, err := manifest.Load(fs, manifest.FileName(dir))
			if err != nil {
				t.Fatal(err)
			}
			if got := persistedManifest.SourceVersion(); got != manifest.CurrentFormatVersion {
				t.Fatalf("manifest version = %d, want %d", got, manifest.CurrentFormatVersion)
			}
			if err := db.CloseWithoutCheckpoint(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenMigratesEpochOneAfterBoundedDecode(t *testing.T) {
	fs := disk.NewMemFS()
	dir := "epoch-one"
	seed, err := OpenDB(DBOptions{Dir: dir, FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := seed.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
	legacy := epochOneDescriptor()
	encoded, err := dbformat.Encode(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.WriteFileAtomically(fs, dbformat.FileName(dir), encoded); err != nil {
		t.Fatal(err)
	}

	migrated, err := OpenDB(DBOptions{Dir: dir, FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	if migrated.Format().Epoch != dbformat.CurrentEpoch ||
		migrated.Format().RequiredFeatures&uint64(dbformat.FeatureBoundedRecordSizes) == 0 {
		t.Fatalf("migrated format = %+v", migrated.Format())
	}
	got, found, err := migrated.Get([]byte("key"))
	if err != nil || !found || string(got) != "value" {
		t.Fatalf("migrated value = (%q,%v,%v)", got, found, err)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenMigratesEpochTwoBeforeWritingAtomicBatch(t *testing.T) {
	fs := disk.NewMemFS()
	dir := "epoch-two"
	seed, err := OpenDB(DBOptions{Dir: dir, FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Put([]byte("existing"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := seed.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
	legacy := dbformat.Descriptor{
		Epoch:          2,
		MinReaderEpoch: 2,
		MinWriterEpoch: 2,
		RequiredFeatures: dbformat.FeatureManifestV4 |
			dbformat.FeaturePerLeafWALWatermarks |
			dbformat.FeatureDictionaryGroups |
			dbformat.FeatureExternalDictionaryCatalog |
			dbformat.FeatureBoundedRecordSizes,
		OptionalFeatures: dbformat.OptionalSchedulerModel,
	}
	encoded, err := dbformat.Encode(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.WriteFileAtomically(fs, dbformat.FileName(dir), encoded); err != nil {
		t.Fatal(err)
	}

	migrated, err := OpenDB(DBOptions{Dir: dir, FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	if migrated.Format().Epoch != dbformat.CurrentEpoch ||
		migrated.Format().RequiredFeatures&uint64(dbformat.FeatureAtomicBatches) == 0 {
		t.Fatalf("migrated format = %+v", migrated.Format())
	}
	if applied, err := migrated.ApplyOnce([]byte("event/1"), []Mutation{
		PutMutation([]byte("purchase/1"), []byte("paid")),
		IncMutation([]byte("tickets/sold"), 1),
	}); err != nil || !applied {
		t.Fatalf("atomic batch after migration = (%v, %v)", applied, err)
	}
}

func TestEpochMigrationDoesNotPublishBeforeWALPassesBounds(t *testing.T) {
	fs := disk.NewMemFS()
	dir := "epoch-one-oversized-wal"
	seed, err := OpenDB(DBOptions{Dir: dir, FS: fs, WALSyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
	legacy := epochOneDescriptor()
	encoded, err := dbformat.Encode(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.WriteFileAtomically(fs, dbformat.FileName(dir), encoded); err != nil {
		t.Fatal(err)
	}
	corruptWAL := wal.EmptyFile(1)
	corruptWAL = append(corruptWAL, byte(1))
	corruptWAL = binary.AppendUvarint(corruptWAL, 1)
	corruptWAL = binary.AppendUvarint(corruptWAL, limits.MaxKeyBytes+1)
	corruptWAL = binary.AppendUvarint(corruptWAL, 0)
	if err := disk.WriteFileAtomically(fs, filepath.Join(dir, "wal.log"), corruptWAL); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenDB(DBOptions{Dir: dir, FS: fs}); err == nil {
		t.Fatal("oversized WAL record unexpectedly opened")
	}
	persisted, err := dbformat.Load(fs, dir)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Epoch != 1 {
		t.Fatalf("FORMAT epoch = %d, migration marker published before WAL validation", persisted.Epoch)
	}
}

func epochOneDescriptor() dbformat.Descriptor {
	return dbformat.Descriptor{
		Epoch:          1,
		MinReaderEpoch: 1,
		MinWriterEpoch: 1,
		RequiredFeatures: dbformat.FeatureManifestV4 |
			dbformat.FeaturePerLeafWALWatermarks |
			dbformat.FeatureDictionaryGroups |
			dbformat.FeatureExternalDictionaryCatalog,
		OptionalFeatures: dbformat.OptionalSchedulerModel,
	}
}

func TestOpenRejectsIncompatibleFormatBeforeCreatingRequiredFiles(t *testing.T) {
	tests := []struct {
		name string
		edit func(*dbformat.Descriptor)
		want error
	}{
		{
			name: "future epoch",
			edit: func(d *dbformat.Descriptor) {
				d.Epoch++
				d.MinReaderEpoch = d.Epoch
				d.MinWriterEpoch = d.Epoch
			},
			want: ErrFormatTooNew,
		},
		{
			name: "unknown required feature",
			edit: func(d *dbformat.Descriptor) {
				d.RequiredFeatures |= 1 << 63
			},
			want: ErrUnknownRequiredFeature,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := disk.NewMemFS()
			dir := "incompatible"
			descriptor := dbformat.Current()
			test.edit(&descriptor)
			data, err := dbformat.Encode(descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if err := disk.WriteFileAtomically(fs, dbformat.FileName(dir), data); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenDB(DBOptions{Dir: dir, FS: fs}); !errors.Is(err, test.want) {
				t.Fatalf("open error = %v, want %v", err, test.want)
			}
			for _, name := range []string{manifest.DefaultFileName, compression.DefaultGroupCatalogFileName} {
				if _, err := fs.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("incompatible open created %s: %v", name, err)
				}
			}
		})
	}
}

func TestOpenRejectsCurrentFormatWithLegacyManifest(t *testing.T) {
	fs := disk.NewMemFS()
	dir := "format-manifest-mismatch"
	if err := disk.WriteFileAtomically(fs, manifest.FileName(dir), encodeLegacyEmptyManifest(3)); err != nil {
		t.Fatal(err)
	}
	if err := dbformat.SaveCurrent(fs, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDB(DBOptions{Dir: dir, FS: fs}); !errors.Is(err, ErrFormatMismatch) {
		t.Fatalf("open error = %v, want ErrFormatMismatch", err)
	}
	if _, err := fs.Stat(filepath.Join(dir, compression.DefaultGroupCatalogFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched open created catalog: %v", err)
	}
}

func TestVerifyAndBackupCoverFormatDescriptor(t *testing.T) {
	fs := disk.NewMemFS()
	db, err := OpenDB(DBOptions{
		Dir:        "format-verify",
		FS:         fs,
		DisableWAL: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 10 * time.Millisecond,
			QuietConfirm:      5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.CloseWithoutCheckpoint()

	report, err := db.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.FormatEpoch != dbformat.CurrentEpoch {
		t.Fatalf("verified format epoch = %d", report.FormatEpoch)
	}
	backupReport, err := db.Backup(context.Background(), "format-verify.fbak")
	if err != nil {
		t.Fatal(err)
	}
	if backupReport.Files < 4 {
		t.Fatalf("backup files = %d, expected FORMAT and required metadata", backupReport.Files)
	}
	archive, err := backup.Inspect(context.Background(), fs, "format-verify.fbak")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range archive.Entries {
		found = found || entry.Name == dbformat.DefaultFileName
	}
	if !found {
		t.Fatal("backup omitted FORMAT")
	}

	if err := disk.WriteFileAtomically(fs, dbformat.FileName("format-verify"), []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Verify(context.Background()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("verify error = %v, want ErrCorruption", err)
	}
}

func TestVerifyRereadsPersistedManifest(t *testing.T) {
	fs := disk.NewMemFS()
	db, err := OpenDB(DBOptions{
		Dir:        "manifest-verify",
		FS:         fs,
		DisableWAL: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 10 * time.Millisecond,
			QuietConfirm:      5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.CloseWithoutCheckpoint()
	if err := disk.WriteFileAtomically(fs, manifest.FileName("manifest-verify"), []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Verify(context.Background()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("verify error = %v, want ErrCorruption", err)
	}
}

type formatPublishFaultFS struct {
	disk.FS
	formatRenamed bool
}

func (f *formatPublishFaultFS) Rename(oldName, newName string) error {
	if err := f.FS.Rename(oldName, newName); err != nil {
		return err
	}
	if filepath.Base(newName) == dbformat.DefaultFileName {
		f.formatRenamed = true
	}
	return nil
}

func (f *formatPublishFaultFS) SyncDir(dir string) error {
	if f.formatRenamed {
		return errors.New("injected FORMAT directory sync failure")
	}
	return f.FS.SyncDir(dir)
}

func TestFormatMigrationCommitUncertainIsReopenable(t *testing.T) {
	base := disk.NewMemFS()
	fs := &formatPublishFaultFS{FS: base}
	if _, err := OpenDB(DBOptions{Dir: "format-uncertain", FS: fs, DisableWAL: true}); !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("open error = %v, want ErrCommitUncertain", err)
	}
	if _, err := dbformat.Load(base, "format-uncertain"); err != nil {
		t.Fatalf("visible FORMAT is not valid: %v", err)
	}
	reopened, err := OpenDB(DBOptions{Dir: "format-uncertain", FS: base, DisableWAL: true})
	if err != nil {
		t.Fatalf("reopen after uncertain publication: %v", err)
	}
	if err := reopened.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
}

type manifestMigrationFaultFS struct {
	disk.FS
	manifestRenamed bool
}

func (f *manifestMigrationFaultFS) Rename(oldName, newName string) error {
	if err := f.FS.Rename(oldName, newName); err != nil {
		return err
	}
	if filepath.Base(newName) == manifest.DefaultFileName {
		f.manifestRenamed = true
	}
	return nil
}

func (f *manifestMigrationFaultFS) SyncDir(dir string) error {
	if f.manifestRenamed {
		return errors.New("injected manifest migration directory sync failure")
	}
	return f.FS.SyncDir(dir)
}

func TestManifestMigrationCommitUncertainDoesNotPublishFormat(t *testing.T) {
	base := disk.NewMemFS()
	if err := disk.WriteFileAtomically(base, manifest.FileName("manifest-uncertain"), encodeLegacyEmptyManifest(3)); err != nil {
		t.Fatal(err)
	}
	fs := &manifestMigrationFaultFS{FS: base}
	if _, err := OpenDB(DBOptions{Dir: "manifest-uncertain", FS: fs, DisableWAL: true}); !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("open error = %v, want ErrCommitUncertain", err)
	}
	if _, err := dbformat.Load(base, "manifest-uncertain"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("FORMAT published after uncertain manifest migration: %v", err)
	}
	upgraded, err := manifest.Load(base, manifest.FileName("manifest-uncertain"))
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.SourceVersion() != manifest.CurrentFormatVersion {
		t.Fatalf("visible manifest version = %d", upgraded.SourceVersion())
	}
	reopened, err := OpenDB(DBOptions{Dir: "manifest-uncertain", FS: base, DisableWAL: true})
	if err != nil {
		t.Fatalf("retry migration: %v", err)
	}
	if err := reopened.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyBackupWithoutFormatMigratesOnFirstOpen(t *testing.T) {
	fs := disk.NewMemFS()
	if _, err := backup.Write(context.Background(), fs, "legacy.fbak", []backup.Source{
		{Name: manifest.DefaultFileName, Data: encodeLegacyEmptyManifest(3)},
		{Name: "wal.log", Data: wal.EmptyFile(1)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Restore(context.Background(), fs, "legacy.fbak", "legacy-restored"); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(DBOptions{
		Dir:           "legacy-restored",
		FS:            fs,
		WALSyncWrites: true,
		SchedulerConfig: scheduler.Config{
			PollInterval:      2 * time.Millisecond,
			ObservationWindow: 10 * time.Millisecond,
			QuietConfirm:      5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("open restored legacy backup: %v", err)
	}
	if db.Format().Epoch != dbformat.CurrentEpoch {
		t.Fatalf("restored format = %+v", db.Format())
	}
	if _, err := db.Verify(context.Background()); err != nil {
		t.Fatalf("verify migrated restore: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func encodeLegacyEmptyManifest(version uint32) []byte {
	recordSize := 36
	if version == 3 {
		recordSize = 44
	}
	const headerSize = 8
	const bodyHeaderSize = 20
	data := make([]byte, headerSize+bodyHeaderSize+recordSize+4)
	copy(data[:4], "FMAN")
	binary.LittleEndian.PutUint32(data[4:8], version)
	pos := headerSize
	binary.LittleEndian.PutUint64(data[pos:pos+8], 1)
	binary.LittleEndian.PutUint64(data[pos+8:pos+16], 0)
	binary.LittleEndian.PutUint32(data[pos+16:pos+20], 1)
	pos += bodyHeaderSize
	binary.LittleEndian.PutUint64(data[pos:pos+8], 1)
	// Segment id, version, key count, applied sequence (v3), and low-key
	// length remain zero for one empty leftmost leaf.
	checksumAt := len(data) - 4
	binary.LittleEndian.PutUint32(data[checksumAt:], crc32.ChecksumIEEE(data[headerSize:checksumAt]))
	return data
}
