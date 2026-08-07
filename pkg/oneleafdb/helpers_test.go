package oneleafdb

import "testing"

// seedClosedDB fills a directory with numKeys entries and closes the database.
//
// Closing runs a checkpoint, so what is left in the directory is exactly what a
// cold start reads: a manifest plus merged segments. Benchmarks that want a cold
// read reopen this directory instead of reaching into a live database, which is
// the only way to measure the real open path (manifest load and segment open).
func seedClosedDB(tb testing.TB, opts DBOptions, numKeys, valueSize int) {
	tb.Helper()

	db, err := OpenDB(opts)
	if err != nil {
		tb.Fatalf("open db for seeding: %v", err)
	}

	value := make([]byte, valueSize)
	for i := 0; i < numKeys; i++ {
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := db.Put(DBKey(i), value); err != nil {
			_ = db.Close()
			tb.Fatalf("put seed key %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		tb.Fatalf("close seeded db: %v", err)
	}
}

// reopenSeeded reopens an already seeded directory and registers its cleanup.
func reopenSeeded(tb testing.TB, opts DBOptions) *DB {
	tb.Helper()

	db, err := OpenDB(opts)
	if err != nil {
		tb.Fatalf("reopen db: %v", err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Errorf("close db: %v", err)
		}
	})
	return db
}
