package oneleafdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/manifest"
	"github.com/uchebnick/fusedb/internal/segment"
)

// reconcileOrphanSegments removes segment outputs that cannot be live because
// the successfully decoded manifest does not reference them. Open holds the
// exclusive database lock and tree.Open has already opened every referenced
// segment before this runs, making the manifest an exact and validated root
// set. This is required after a crash or commit-uncertain segment directory
// sync: otherwise the next retry may collide with the same immutable id/version.
func reconcileOrphanSegments(fs disk.FS, dir string, snapshot *manifest.Manifest) error {
	if fs == nil || snapshot == nil {
		return errors.New("oneleafdb: cannot reconcile segments without filesystem and manifest")
	}
	live := make(map[string]struct{}, len(snapshot.Leaves))
	for _, leaf := range snapshot.Leaves {
		if leaf.SegmentID == 0 {
			continue
		}
		live[filepath.Base(segment.SegmentFileName(dir, leaf.SegmentID, leaf.SegmentVersion))] = struct{}{}
	}
	names, err := fs.List(dir)
	if err != nil {
		return err
	}
	removed := false
	for _, name := range names {
		base := filepath.Base(name)
		_, _, temporary, owned := segment.ParseSegmentFileName(base)
		if !owned {
			continue
		}
		if _, referenced := live[base]; referenced && !temporary {
			continue
		}
		path := filepath.Join(dir, base)
		if err := fs.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", base, err)
		}
		removed = true
	}
	if removed {
		if err := fs.SyncDir(dir); err != nil {
			return fmt.Errorf("sync reconciled segment directory: %w", err)
		}
	}
	return nil
}
