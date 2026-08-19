package compression

import (
	"errors"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

func TestDictionaryGroupCatalogReservationPublicationAndReopen(t *testing.T) {
	fs := disk.NewMemFS()
	catalog, err := OpenDictionaryGroupCatalog(fs, "db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first, err := catalog.ReserveDictionaryID()
	if err != nil || first != 1 {
		t.Fatalf("first reservation = (%d,%v)", first, err)
	}
	second, err := catalog.ReserveDictionaryID()
	if err != nil || second != 2 {
		t.Fatalf("second reservation = (%d,%v)", second, err)
	}
	record, err := catalog.Publish(7, second)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if record.Generation != 1 || record.ActiveDictionaryID != second {
		t.Fatalf("published record = %+v", record)
	}

	reopened, err := OpenDictionaryGroupCatalog(fs, "db")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if active, ok := reopened.ActiveDictionaryID(7); !ok || active != second {
		t.Fatalf("active dictionary = (%d,%v), want (%d,true)", active, ok, second)
	}
	third, err := reopened.ReserveDictionaryID()
	if err != nil || third != 3 {
		t.Fatalf("reservation after reopen = (%d,%v)", third, err)
	}
	updated, err := reopened.Publish(7, third)
	if err != nil || updated.Generation != 2 {
		t.Fatalf("second publication = (%+v,%v)", updated, err)
	}
}

func TestDictionaryGroupCatalogImportsExternalIDWithoutReuse(t *testing.T) {
	fs := disk.NewMemFS()
	catalog, err := OpenDictionaryGroupCatalog(fs, "db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := catalog.EnsureNextAfter(91); err != nil {
		t.Fatalf("ensure external ID: %v", err)
	}
	id, err := catalog.ReserveDictionaryID()
	if err != nil || id != 92 {
		t.Fatalf("reservation = (%d,%v), want 92", id, err)
	}
	if _, err := catalog.Publish(1, 93); !errors.Is(err, ErrDictionaryIDNotReserved) {
		t.Fatalf("publish unreserved error = %v", err)
	}
}

func TestDictionaryGroupCatalogRejectsCorruption(t *testing.T) {
	valid, err := EncodeDictionaryGroupCatalog(DictionaryGroupSnapshot{
		NextDictionaryID: 3,
		Groups: []DictionaryGroupRecord{{
			GroupID:            1,
			ActiveDictionaryID: 2,
			Generation:         1,
		}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for i := range valid {
		corrupt := append([]byte(nil), valid...)
		corrupt[i] ^= 0xff
		if _, err := DecodeDictionaryGroupCatalog(corrupt); err == nil {
			t.Fatalf("corruption at byte %d was accepted", i)
		}
	}
	for n := range len(valid) {
		if _, err := DecodeDictionaryGroupCatalog(valid[:n]); err == nil {
			t.Fatalf("prefix %d was accepted", n)
		}
	}
}

type catalogSyncFaultFS struct {
	disk.FS
	fail bool
}

func (f *catalogSyncFaultFS) SyncDir(dir string) error {
	if f.fail {
		f.fail = false
		return errors.New("injected catalog sync failure")
	}
	return f.FS.SyncDir(dir)
}

func TestDictionaryGroupCatalogCommitUncertainRequiresReopen(t *testing.T) {
	base := disk.NewMemFS()
	faults := &catalogSyncFaultFS{FS: base}
	catalog, err := OpenDictionaryGroupCatalog(faults, "db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	faults.fail = true
	if _, err := catalog.ReserveDictionaryID(); !errors.Is(err, disk.ErrCommitUncertain) {
		t.Fatalf("reserve error = %v, want ErrCommitUncertain", err)
	}

	reopened, err := OpenDictionaryGroupCatalog(base, "db")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	// Rename completed before the injected directory-sync failure, so reopening
	// observes the advanced allocator and never reuses the uncertain ID.
	id, err := reopened.ReserveDictionaryID()
	if err != nil || id != 2 {
		t.Fatalf("post-uncertain reservation = (%d,%v), want 2", id, err)
	}
}

func FuzzDecodeDictionaryGroupCatalogNeverPanics(f *testing.F) {
	f.Add([]byte{})
	valid, err := EncodeDictionaryGroupCatalog(DictionaryGroupSnapshot{NextDictionaryID: 1})
	if err != nil {
		f.Fatalf("encode seed: %v", err)
	}
	f.Add(valid)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeDictionaryGroupCatalog(data)
	})
}
