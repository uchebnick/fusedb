package compression

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
)

const (
	DefaultGroupCatalogFileName = "DICTIONARY-CATALOG"
	groupCatalogMagic           = "FDGC"
	groupCatalogVersion         = 1
	groupCatalogHeaderSize      = 4 + 4 + 4 + 4
	groupCatalogRecordSize      = 8 + 4 + 8
	groupCatalogChecksumSize    = 4
)

var (
	ErrGroupCatalogCorrupt     = errors.New("compression: corrupt dictionary group catalog")
	ErrGroupCatalogVersion     = errors.New("compression: unsupported dictionary group catalog version")
	ErrDictionaryIDExhausted   = errors.New("compression: dictionary id space exhausted")
	ErrDictionaryIDNotReserved = errors.New("compression: dictionary id was not reserved")
	ErrZeroDictionaryGroupID   = errors.New("compression: dictionary group id must be non-zero")
)

// DictionaryGroupRecord is the durable active dictionary selection for one
// group. Generation increments on every atomic publication.
type DictionaryGroupRecord struct {
	GroupID            uint64
	ActiveDictionaryID uint32
	Generation         uint64
}

// DictionaryGroupSnapshot is a detached catalog view.
type DictionaryGroupSnapshot struct {
	NextDictionaryID uint32
	Groups           []DictionaryGroupRecord
}

// DictionaryGroupCatalog reserves never-reused dictionary IDs and atomically
// publishes one active immutable dictionary per group.
type DictionaryGroupCatalog struct {
	mu     sync.Mutex
	fs     disk.FS
	path   string
	nextID uint32
	groups map[uint64]DictionaryGroupRecord
}

// OpenDictionaryGroupCatalog opens or creates the catalog in dir.
func OpenDictionaryGroupCatalog(fs disk.FS, dir string) (*DictionaryGroupCatalog, error) {
	if fs == nil {
		return nil, ErrNilDictionaryFS
	}
	path := filepath.Join(dir, DefaultGroupCatalogFileName)
	snapshot, err := LoadDictionaryGroupSnapshot(fs, path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		catalog := &DictionaryGroupCatalog{fs: fs, path: path, nextID: 1, groups: make(map[uint64]DictionaryGroupRecord)}
		if err := catalog.persistLocked(catalog.snapshotLocked()); err != nil {
			return nil, err
		}
		return catalog, nil
	}
	catalog := &DictionaryGroupCatalog{
		fs:     fs,
		path:   path,
		nextID: snapshot.NextDictionaryID,
		groups: make(map[uint64]DictionaryGroupRecord, len(snapshot.Groups)),
	}
	for _, group := range snapshot.Groups {
		catalog.groups[group.GroupID] = group
	}
	return catalog, nil
}

// LoadDictionaryGroupSnapshot reads one catalog under its hard metadata
// budget, then validates its checksum and records.
func LoadDictionaryGroupSnapshot(fs disk.FS, path string) (DictionaryGroupSnapshot, error) {
	data, err := disk.ReadFileLimited(fs, path, limits.MaxGroupCatalogBytes)
	if err != nil {
		return DictionaryGroupSnapshot{}, err
	}
	return DecodeDictionaryGroupCatalog(data)
}

// ReserveDictionaryID durably advances the allocator before returning an ID.
// If later training or publication fails, the ID remains consumed.
func (c *DictionaryGroupCatalog) ReserveDictionaryID() (uint32, error) {
	if c == nil {
		return 0, ErrGroupCatalogCorrupt
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nextID == 0 || c.nextID == math.MaxUint32 {
		return 0, ErrDictionaryIDExhausted
	}
	id := c.nextID
	next := c.snapshotLocked()
	next.NextDictionaryID++
	if err := c.persistLocked(next); err != nil {
		return 0, err
	}
	c.nextID = next.NextDictionaryID
	return id, nil
}

// EnsureNextAfter imports an externally supplied dictionary ID without ever
// allowing the runtime allocator to reuse it.
func (c *DictionaryGroupCatalog) EnsureNextAfter(id uint32) error {
	if c == nil {
		return ErrGroupCatalogCorrupt
	}
	if id == math.MaxUint32 {
		return ErrDictionaryIDExhausted
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nextID > id {
		return nil
	}
	next := c.snapshotLocked()
	next.NextDictionaryID = id + 1
	if err := c.persistLocked(next); err != nil {
		return err
	}
	c.nextID = next.NextDictionaryID
	return nil
}

// Publish atomically selects a previously reserved dictionary for groupID.
func (c *DictionaryGroupCatalog) Publish(groupID uint64, dictionaryID uint32) (DictionaryGroupRecord, error) {
	if c == nil {
		return DictionaryGroupRecord{}, ErrGroupCatalogCorrupt
	}
	if groupID == 0 {
		return DictionaryGroupRecord{}, ErrZeroDictionaryGroupID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if dictionaryID == 0 || dictionaryID >= c.nextID {
		return DictionaryGroupRecord{}, fmt.Errorf("%w: %d", ErrDictionaryIDNotReserved, dictionaryID)
	}
	current := c.groups[groupID]
	if current.ActiveDictionaryID == dictionaryID {
		return current, nil
	}
	nextRecord := DictionaryGroupRecord{
		GroupID:            groupID,
		ActiveDictionaryID: dictionaryID,
		Generation:         current.Generation + 1,
	}
	next := c.snapshotLocked()
	replaced := false
	for i := range next.Groups {
		if next.Groups[i].GroupID == groupID {
			next.Groups[i] = nextRecord
			replaced = true
			break
		}
	}
	if !replaced {
		next.Groups = append(next.Groups, nextRecord)
		sort.Slice(next.Groups, func(i, j int) bool { return next.Groups[i].GroupID < next.Groups[j].GroupID })
	}
	if err := c.persistLocked(next); err != nil {
		return DictionaryGroupRecord{}, err
	}
	c.groups[groupID] = nextRecord
	return nextRecord, nil
}

// ActiveDictionaryID returns the current immutable selection for groupID.
func (c *DictionaryGroupCatalog) ActiveDictionaryID(groupID uint64) (uint32, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.groups[groupID]
	return record.ActiveDictionaryID, ok && record.ActiveDictionaryID != 0
}

// Snapshot returns a detached, sorted catalog view.
func (c *DictionaryGroupCatalog) Snapshot() DictionaryGroupSnapshot {
	if c == nil {
		return DictionaryGroupSnapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked()
}

func (c *DictionaryGroupCatalog) snapshotLocked() DictionaryGroupSnapshot {
	snapshot := DictionaryGroupSnapshot{NextDictionaryID: c.nextID, Groups: make([]DictionaryGroupRecord, 0, len(c.groups))}
	for _, group := range c.groups {
		snapshot.Groups = append(snapshot.Groups, group)
	}
	sort.Slice(snapshot.Groups, func(i, j int) bool { return snapshot.Groups[i].GroupID < snapshot.Groups[j].GroupID })
	return snapshot
}

func (c *DictionaryGroupCatalog) persistLocked(snapshot DictionaryGroupSnapshot) error {
	data, err := EncodeDictionaryGroupCatalog(snapshot)
	if err != nil {
		return err
	}
	return disk.WriteFileAtomically(c.fs, c.path, data)
}

// EncodeDictionaryGroupCatalog encodes a stable checksummed catalog image.
func EncodeDictionaryGroupCatalog(snapshot DictionaryGroupSnapshot) ([]byte, error) {
	if err := validateDictionaryGroupSnapshot(snapshot); err != nil {
		return nil, err
	}
	maxGroups := (limits.MaxGroupCatalogBytes - groupCatalogHeaderSize - groupCatalogChecksumSize) / groupCatalogRecordSize
	if len(snapshot.Groups) > maxGroups || len(snapshot.Groups) > math.MaxUint32 {
		return nil, ErrGroupCatalogCorrupt
	}
	groups := append([]DictionaryGroupRecord(nil), snapshot.Groups...)
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	buf := make([]byte, groupCatalogHeaderSize+len(groups)*groupCatalogRecordSize+groupCatalogChecksumSize)
	copy(buf[:4], groupCatalogMagic)
	binary.LittleEndian.PutUint32(buf[4:8], groupCatalogVersion)
	binary.LittleEndian.PutUint32(buf[8:12], snapshot.NextDictionaryID)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(groups)))
	offset := groupCatalogHeaderSize
	for _, group := range groups {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], group.GroupID)
		binary.LittleEndian.PutUint32(buf[offset+8:offset+12], group.ActiveDictionaryID)
		binary.LittleEndian.PutUint64(buf[offset+12:offset+20], group.Generation)
		offset += groupCatalogRecordSize
	}
	binary.LittleEndian.PutUint32(buf[offset:], crc32.ChecksumIEEE(buf[:offset]))
	return buf, nil
}

// DecodeDictionaryGroupCatalog validates and decodes one complete image.
func DecodeDictionaryGroupCatalog(data []byte) (DictionaryGroupSnapshot, error) {
	if len(data) < groupCatalogHeaderSize+groupCatalogChecksumSize || string(data[:4]) != groupCatalogMagic {
		return DictionaryGroupSnapshot{}, ErrGroupCatalogCorrupt
	}
	if version := binary.LittleEndian.Uint32(data[4:8]); version != groupCatalogVersion {
		return DictionaryGroupSnapshot{}, fmt.Errorf("%w: %d", ErrGroupCatalogVersion, version)
	}
	count := uint64(binary.LittleEndian.Uint32(data[12:16]))
	wantSize := uint64(groupCatalogHeaderSize+groupCatalogChecksumSize) + count*groupCatalogRecordSize
	if len(data) > limits.MaxGroupCatalogBytes || wantSize != uint64(len(data)) {
		return DictionaryGroupSnapshot{}, ErrGroupCatalogCorrupt
	}
	checksumOffset := len(data) - groupCatalogChecksumSize
	if crc32.ChecksumIEEE(data[:checksumOffset]) != binary.LittleEndian.Uint32(data[checksumOffset:]) {
		return DictionaryGroupSnapshot{}, ErrGroupCatalogCorrupt
	}
	snapshot := DictionaryGroupSnapshot{
		NextDictionaryID: binary.LittleEndian.Uint32(data[8:12]),
		Groups:           make([]DictionaryGroupRecord, int(count)),
	}
	offset := groupCatalogHeaderSize
	for i := range snapshot.Groups {
		snapshot.Groups[i] = DictionaryGroupRecord{
			GroupID:            binary.LittleEndian.Uint64(data[offset : offset+8]),
			ActiveDictionaryID: binary.LittleEndian.Uint32(data[offset+8 : offset+12]),
			Generation:         binary.LittleEndian.Uint64(data[offset+12 : offset+20]),
		}
		offset += groupCatalogRecordSize
	}
	if err := validateDictionaryGroupSnapshot(snapshot); err != nil {
		return DictionaryGroupSnapshot{}, err
	}
	return snapshot, nil
}

func validateDictionaryGroupSnapshot(snapshot DictionaryGroupSnapshot) error {
	if snapshot.NextDictionaryID == 0 {
		return ErrGroupCatalogCorrupt
	}
	seen := make(map[uint64]struct{}, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		if group.GroupID == 0 || group.ActiveDictionaryID == 0 || group.ActiveDictionaryID >= snapshot.NextDictionaryID || group.Generation == 0 {
			return ErrGroupCatalogCorrupt
		}
		if _, exists := seen[group.GroupID]; exists {
			return ErrGroupCatalogCorrupt
		}
		seen[group.GroupID] = struct{}{}
	}
	return nil
}
