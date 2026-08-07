// Package manifest owns the persistent catalog of database state.
//
// The manifest records the segment id allocator, the WAL sequence number that
// is already durable in segments, and the ordered set of leaves that partition
// the keyspace. It is the file that lets an existing database be reopened
// instead of being rebuilt or refused.
//
// The manifest is rewritten in full on every update through
// disk.WriteFileAtomically. This is a deliberate choice: the manifest holds
// thousands of leaf records at most, so a complete rewrite costs far less than
// the complexity of an append-only edit log with periodic compaction, and it
// makes every on-disk state a self-contained, CRC-checked snapshot. If the
// leaf count ever grows past that scale, this is the decision to revisit.
//
// Leaf ranges are half-open and only the lower bound is stored: a leaf covers
// [LowKey, next leaf's LowKey), and the last leaf covers everything to the
// right. The leftmost leaf therefore has an empty LowKey so that the leaves
// together cover the whole keyspace with no hole.
package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/uchebnick/fusedb/internal/disk"
)

// DefaultFileName is the manifest file name inside a database directory.
const DefaultFileName = "MANIFEST"

var (
	// ErrNilFilesystem reports that no filesystem was supplied.
	ErrNilFilesystem = errors.New("manifest: nil filesystem")

	// ErrNilManifest reports that a nil manifest was passed where a value was
	// required.
	ErrNilManifest = errors.New("manifest: nil manifest")

	// ErrNotExist reports that no manifest file is present, which is the
	// normal state of a database directory on first open. It wraps
	// os.ErrNotExist, so callers may match either error.
	ErrNotExist = errors.New("manifest: manifest file does not exist")

	// The errors below reject a leaf set that would not describe a valid
	// partition of the keyspace. Each leaf owns [LowKey, next LowKey), so the
	// records must be sorted, unique, and start at the empty key; any of these
	// broken means some key range is either unreachable or owned twice.

	// ErrLeavesNotSorted reports leaf records that are not in low-key order.
	ErrLeavesNotSorted = errors.New("manifest: leaves are not sorted by low key")

	// ErrDuplicateLowKey reports two leaves claiming the same range start.
	ErrDuplicateLowKey = errors.New("manifest: duplicate leaf low key")

	// ErrDuplicateLeafID reports two leaves sharing one identifier.
	ErrDuplicateLeafID = errors.New("manifest: duplicate leaf id")

	// ErrEmptyLowKeyNotFirst reports an empty low key on a leaf other than the
	// leftmost one.
	ErrEmptyLowKeyNotFirst = errors.New("manifest: empty low key is only allowed for the first leaf")

	// ErrMissingLeftmostLeaf reports a leaf set that does not start at the
	// empty key and so leaves the lowest keys uncovered.
	ErrMissingLeftmostLeaf = errors.New("manifest: first leaf must have an empty low key")

	// ErrLeafNotFound reports an update naming a leaf the manifest does not
	// hold.
	ErrLeafNotFound = errors.New("manifest: leaf not found")

	// ErrSplitLowKeyChange reports a split whose first output does not keep the
	// low key of the leaf being split, which would move the range boundary.
	ErrSplitLowKeyChange = errors.New("manifest: split must preserve the low key of the split leaf")

	// ErrInvalidSplitKey reports a split point outside the range of the leaf
	// being split.
	ErrInvalidSplitKey = errors.New("manifest: split key is outside the range of the split leaf")
)

// LeafRecord describes one leaf of the key-ordered leaf tree.
//
// LowKey is the inclusive lower bound of the leaf range; the upper bound is
// implied by the next leaf and is unbounded for the last leaf. A SegmentID of
// zero means the leaf has no segment yet, which is the valid state of a freshly
// created empty leaf.
type LeafRecord struct {
	LeafID         uint64
	LowKey         []byte
	SegmentID      uint64
	SegmentVersion uint64

	// Keys is how many keys the segment holds.
	//
	// It is recorded so a reopened leaf can size the bloom filter of its next
	// merge for the merged result. Without it a leaf restored from disk would
	// size the filter from its buffer alone and land well above the target
	// false positive rate.
	Keys uint64
}

// Manifest is the in-memory form of the persistent catalog.
//
// LowKey slices held by a Manifest are treated as immutable; mutation helpers
// copy the keys they are given, so callers keep ownership of their input.
type Manifest struct {
	// NextSegmentID is the next segment id to hand out. Segment id zero is
	// reserved to mean "no segment".
	NextSegmentID uint64

	// AppliedSeq is the highest WAL sequence number whose data is durable in
	// segments referenced by this manifest.
	AppliedSeq uint64

	// Leaves are sorted strictly ascending by LowKey.
	Leaves []LeafRecord
}

// New returns an empty manifest ready for a fresh database.
func New() *Manifest {
	return &Manifest{NextSegmentID: 1}
}

// FileName returns the manifest path inside a database directory.
func FileName(dir string) string {
	return filepath.Join(dir, DefaultFileName)
}

// IsNotExist reports whether err means that no manifest file is present.
func IsNotExist(err error) bool {
	return errors.Is(err, ErrNotExist) || errors.Is(err, os.ErrNotExist)
}

// Len returns the number of leaves.
func (m *Manifest) Len() int {
	if m == nil {
		return 0
	}
	return len(m.Leaves)
}

// Clone returns a deep copy of the manifest, including leaf low keys.
func (m *Manifest) Clone() *Manifest {
	if m == nil {
		return nil
	}
	out := &Manifest{
		NextSegmentID: m.NextSegmentID,
		AppliedSeq:    m.AppliedSeq,
	}
	if m.Leaves == nil {
		return out
	}
	out.Leaves = make([]LeafRecord, len(m.Leaves))
	for i, leaf := range m.Leaves {
		leaf.LowKey = cloneKey(leaf.LowKey)
		out.Leaves[i] = leaf
	}
	return out
}

// AllocateSegmentID reserves and returns the next segment id.
func (m *Manifest) AllocateSegmentID() uint64 {
	if m.NextSegmentID == 0 {
		m.NextSegmentID = 1
	}
	id := m.NextSegmentID
	m.NextSegmentID++
	return id
}

// Validate checks the leaf ordering and coverage invariants.
//
// Leaves must be strictly ascending by LowKey with unique leaf ids, only the
// first leaf may carry an empty LowKey, and a non-empty leaf set must start at
// the empty LowKey so that no part of the keyspace is left uncovered.
func (m *Manifest) Validate() error {
	if m == nil {
		return ErrNilManifest
	}
	if len(m.Leaves) == 0 {
		return nil
	}
	if len(m.Leaves[0].LowKey) != 0 {
		return ErrMissingLeftmostLeaf
	}

	seen := make(map[uint64]struct{}, len(m.Leaves))
	for i := range m.Leaves {
		leaf := &m.Leaves[i]
		if i > 0 && len(leaf.LowKey) == 0 {
			return fmt.Errorf("%w: index %d", ErrEmptyLowKeyNotFirst, i)
		}
		if _, dup := seen[leaf.LeafID]; dup {
			return fmt.Errorf("%w: %d", ErrDuplicateLeafID, leaf.LeafID)
		}
		seen[leaf.LeafID] = struct{}{}

		if i == 0 {
			continue
		}
		switch bytes.Compare(m.Leaves[i-1].LowKey, leaf.LowKey) {
		case 0:
			return fmt.Errorf("%w: index %d", ErrDuplicateLowKey, i)
		case 1:
			return fmt.Errorf("%w: index %d", ErrLeavesNotSorted, i)
		}
	}
	return nil
}

// FindLeaf returns the leaf whose range covers key, along with its index.
//
// It is the last leaf with LowKey <= key. The boolean is false only when the
// manifest has no leaves, or when its leaves do not start at the empty LowKey
// and key sorts before the first of them. FindLeaf performs no allocations and
// is safe for concurrent readers as long as the manifest is not mutated.
func (m *Manifest) FindLeaf(key []byte) (LeafRecord, int, bool) {
	if m == nil || len(m.Leaves) == 0 {
		return LeafRecord{}, -1, false
	}

	// Binary search for the first leaf with LowKey > key.
	lo, hi := 0, len(m.Leaves)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(m.Leaves[mid].LowKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return LeafRecord{}, -1, false
	}
	return m.Leaves[lo-1], lo - 1, true
}

// IndexOfLeafID returns the index of the leaf with the given id, or -1.
func (m *Manifest) IndexOfLeafID(leafID uint64) int {
	if m == nil {
		return -1
	}
	for i := range m.Leaves {
		if m.Leaves[i].LeafID == leafID {
			return i
		}
	}
	return -1
}

// AddLeaf inserts a leaf at its sorted position.
//
// The low key is copied. The insert is rejected if it would break an invariant,
// leaving the manifest unchanged.
func (m *Manifest) AddLeaf(leaf LeafRecord) error {
	if m == nil {
		return ErrNilManifest
	}
	if len(m.Leaves) == 0 {
		if len(leaf.LowKey) != 0 {
			return ErrMissingLeftmostLeaf
		}
	} else if len(leaf.LowKey) == 0 {
		return ErrEmptyLowKeyNotFirst
	}
	if m.IndexOfLeafID(leaf.LeafID) >= 0 {
		return fmt.Errorf("%w: %d", ErrDuplicateLeafID, leaf.LeafID)
	}

	pos := m.searchLowKey(leaf.LowKey)
	if pos < len(m.Leaves) && bytes.Equal(m.Leaves[pos].LowKey, leaf.LowKey) {
		return fmt.Errorf("%w: %x", ErrDuplicateLowKey, leaf.LowKey)
	}

	leaf.LowKey = cloneKey(leaf.LowKey)
	m.Leaves = append(m.Leaves, LeafRecord{})
	copy(m.Leaves[pos+1:], m.Leaves[pos:])
	m.Leaves[pos] = leaf
	return nil
}

// SplitLeaf replaces the leaf with the given id by two leaves.
//
// The left replacement must keep the low key of the leaf it replaces so that
// keyspace coverage is preserved, and the right replacement must start strictly
// inside the original range. Low keys are copied. A rejected split leaves the
// manifest unchanged.
func (m *Manifest) SplitLeaf(leafID uint64, left, right LeafRecord) error {
	if m == nil {
		return ErrNilManifest
	}
	idx := m.IndexOfLeafID(leafID)
	if idx < 0 {
		return fmt.Errorf("%w: %d", ErrLeafNotFound, leafID)
	}
	if !bytes.Equal(left.LowKey, m.Leaves[idx].LowKey) {
		return ErrSplitLowKeyChange
	}
	if bytes.Compare(right.LowKey, left.LowKey) <= 0 {
		return fmt.Errorf("%w: %x", ErrInvalidSplitKey, right.LowKey)
	}
	if idx+1 < len(m.Leaves) && bytes.Compare(right.LowKey, m.Leaves[idx+1].LowKey) >= 0 {
		return fmt.Errorf("%w: %x", ErrInvalidSplitKey, right.LowKey)
	}
	if left.LeafID == right.LeafID {
		return fmt.Errorf("%w: %d", ErrDuplicateLeafID, left.LeafID)
	}
	for i := range m.Leaves {
		if i == idx {
			continue
		}
		if m.Leaves[i].LeafID == left.LeafID || m.Leaves[i].LeafID == right.LeafID {
			return fmt.Errorf("%w: %d", ErrDuplicateLeafID, m.Leaves[i].LeafID)
		}
	}

	left.LowKey = cloneKey(left.LowKey)
	right.LowKey = cloneKey(right.LowKey)
	m.Leaves = append(m.Leaves, LeafRecord{})
	copy(m.Leaves[idx+2:], m.Leaves[idx+1:])
	m.Leaves[idx] = left
	m.Leaves[idx+1] = right
	return nil
}

// SetLeafSegment points a leaf at a segment id and version.
//
// A segment id of zero detaches the leaf from any segment.
func (m *Manifest) SetLeafSegment(leafID, segmentID, segmentVersion uint64) error {
	if m == nil {
		return ErrNilManifest
	}
	idx := m.IndexOfLeafID(leafID)
	if idx < 0 {
		return fmt.Errorf("%w: %d", ErrLeafNotFound, leafID)
	}
	m.Leaves[idx].SegmentID = segmentID
	m.Leaves[idx].SegmentVersion = segmentVersion
	return nil
}

// searchLowKey returns the first index whose low key is >= key.
func (m *Manifest) searchLowKey(key []byte) int {
	lo, hi := 0, len(m.Leaves)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(m.Leaves[mid].LowKey, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Save writes the manifest atomically, replacing any previous contents.
func Save(fs disk.FS, path string, m *Manifest) error {
	if fs == nil {
		return ErrNilFilesystem
	}
	if m == nil {
		return ErrNilManifest
	}
	data, err := EncodeManifest(m)
	if err != nil {
		return err
	}
	return disk.WriteFileAtomically(fs, path, data)
}

// Load reads and verifies the manifest at path.
//
// A missing file yields an error matching ErrNotExist, which callers should
// treat as "fresh database" rather than as corruption.
func Load(fs disk.FS, path string) (*Manifest, error) {
	if fs == nil {
		return nil, ErrNilFilesystem
	}
	data, err := disk.ReadFile(fs, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s: %w", ErrNotExist, path, os.ErrNotExist)
		}
		return nil, err
	}
	return DecodeManifest(data)
}

func cloneKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	out := make([]byte, len(key))
	copy(out, key)
	return out
}
