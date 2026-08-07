package oneleafdbbench

import (
	"sync/atomic"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	onedb "github.com/uchebnick/fusedb/pkg/oneleafdb"
)

// countingFS counts every byte the engine writes through the filesystem.
//
// Summing the sizes of the segment files that survive would only report the
// live data size: superseded segments are deleted after a merge, which is
// exactly the rewriting that amplification is meant to capture. Counting at the
// write call instead accounts for every rewrite.
type countingFS struct {
	disk.FS
	written atomic.Int64
}

func newCountingFS(inner disk.FS) *countingFS {
	return &countingFS{FS: inner}
}

func (c *countingFS) BytesWritten() int64 {
	return c.written.Load()
}

func (c *countingFS) Create(name string) (disk.File, error) {
	f, err := c.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &countingFile{File: f, counter: &c.written}, nil
}

func (c *countingFS) OpenReadWrite(name string) (disk.File, error) {
	f, err := c.FS.OpenReadWrite(name)
	if err != nil {
		return nil, err
	}
	return &countingFile{File: f, counter: &c.written}, nil
}

type countingFile struct {
	disk.File
	counter *atomic.Int64
}

func (f *countingFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	f.counter.Add(int64(n))
	return n, err
}

func (f *countingFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.counter.Add(int64(n))
	return n, err
}

// amplificationRun writes keys in batches, checkpointing after each one, and
// reports how many bytes reached the filesystem.
//
// The explicit checkpoint per batch is what makes the measurement meaningful.
// Left to the background worker, a single-threaded writer outruns the merges
// and the run ends up doing roughly one merge in total, which measures nothing
// about repeated rewriting. One merge per batch models a database that is
// continuously kept up to date, which is when amplification is actually paid.
func amplificationRun(t *testing.T, keys, batch int, maxLeafBytes int64) (userBytes, written int64, leaves int) {
	t.Helper()

	const valueSize = 128

	dir := t.TempDir()
	counting := newCountingFS(disk.DefaultFS)

	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            dir,
		FS:             counting,
		ThresholdBytes: 1 << 20,
		MaxLeafBytes:   maxLeafBytes,
		DisableWAL:     true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	value := make([]byte, valueSize)
	for i := range keys {
		key := onedb.DBKey(i)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		userBytes += int64(len(key) + len(value))

		if (i+1)%batch == 0 {
			if err := db.Merge(); err != nil {
				t.Fatalf("merge at %d: %v", i, err)
			}
		}
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("final merge: %v", err)
	}
	leaves = db.LeafCount()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	return userBytes, counting.BytesWritten(), leaves
}

// TestWriteAmplification compares a keyspace held in one leaf against one that
// splits, as the dataset grows.
//
// This is the number the leaf tree exists to control. Merging one leaf that
// holds everything rewrites the entire database every time, so amplification
// grows with the data size. When leaves split at MaxLeafBytes, a merge rewrites
// only the leaf that was touched.
func TestWriteAmplification(t *testing.T) {
	if testing.Short() {
		t.Skip("write amplification measurement is slow")
	}

	const (
		batch = 4000
		mib   = float64(1 << 20)

		// Large enough that the merge never cuts, which reproduces the old
		// single-segment engine.
		noSplit = int64(1) << 40

		// Small enough that a few batches force several splits.
		splitAt = int64(1) << 20
	)

	sizes := []int{20_000, 40_000, 80_000, 160_000}

	t.Log("                 one leaf                        splitting leaves")
	t.Log("keys       user MB   written MB   amp      written MB   amp     leaves")

	var firstSplit, lastSplit float64
	for i, keys := range sizes {
		userBytes, flatWritten, _ := amplificationRun(t, keys, batch, noSplit)
		_, treeWritten, treeLeaves := amplificationRun(t, keys, batch, splitAt)

		flatAmp := float64(flatWritten) / float64(userBytes)
		treeAmp := float64(treeWritten) / float64(userBytes)

		t.Logf("%-10d %-9.2f %-12.2f %-8.2f %-12.2f %-7.2f %d",
			keys, float64(userBytes)/mib,
			float64(flatWritten)/mib, flatAmp,
			float64(treeWritten)/mib, treeAmp, treeLeaves)

		if i == 0 {
			firstSplit = treeAmp
		}
		lastSplit = treeAmp

		if treeLeaves < 2 {
			t.Errorf("%d keys: expected the keyspace to split, got %d leaf", keys, treeLeaves)
		}
		if treeAmp >= flatAmp {
			t.Errorf("%d keys: splitting amplification %.2fx is not better than single-leaf %.2fx",
				keys, treeAmp, flatAmp)
		}
	}

	// The point is not a particular constant but that amplification stops
	// tracking the dataset size once merges are local to a leaf.
	if lastSplit > firstSplit*2 {
		t.Errorf("amplification grew from %.2fx to %.2fx while the dataset grew 8x: "+
			"merges are not staying local to a leaf", firstSplit, lastSplit)
	}
}
