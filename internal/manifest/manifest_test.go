package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

func sampleManifest() *Manifest {
	return &Manifest{
		NextSegmentID: 42,
		AppliedSeq:    1337,
		Leaves: []LeafRecord{
			{LeafID: 1, LowKey: nil, SegmentID: 7, SegmentVersion: 3},
			{LeafID: 2, LowKey: []byte{0x00}, SegmentID: 0, SegmentVersion: 0},
			{LeafID: 3, LowKey: []byte{0x00, 0xff, 0x00, 'k', 0x00}, SegmentID: 9, SegmentVersion: 1},
			{LeafID: 4, LowKey: []byte("user:0001"), SegmentID: 11, SegmentVersion: 2},
			{LeafID: 5, LowKey: bytes.Repeat([]byte{0xff}, 300), SegmentID: 12, SegmentVersion: 8},
		},
	}
}

func assertManifestEqual(t *testing.T, got, want *Manifest) {
	t.Helper()
	if got == nil || want == nil {
		t.Fatalf("nil manifest: got=%v want=%v", got, want)
	}
	if got.NextSegmentID != want.NextSegmentID {
		t.Errorf("NextSegmentID = %d, want %d", got.NextSegmentID, want.NextSegmentID)
	}
	if got.AppliedSeq != want.AppliedSeq {
		t.Errorf("AppliedSeq = %d, want %d", got.AppliedSeq, want.AppliedSeq)
	}
	if len(got.Leaves) != len(want.Leaves) {
		t.Fatalf("leaf count = %d, want %d", len(got.Leaves), len(want.Leaves))
	}
	for i := range want.Leaves {
		g, w := got.Leaves[i], want.Leaves[i]
		if g.LeafID != w.LeafID || g.SegmentID != w.SegmentID || g.SegmentVersion != w.SegmentVersion {
			t.Errorf("leaf %d = %+v, want %+v", i, g, w)
		}
		if !bytes.Equal(g.LowKey, w.LowKey) {
			t.Errorf("leaf %d low key = %x, want %x", i, g.LowKey, w.LowKey)
		}
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	fs := disk.NewMemFS()
	path := FileName("db")
	want := sampleManifest()

	if err := Save(fs, path, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(fs, path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertManifestEqual(t, got, want)

	if err := got.Validate(); err != nil {
		t.Fatalf("loaded manifest is invalid: %v", err)
	}
}

func TestSaveLoadEmptyManifest(t *testing.T) {
	fs := disk.NewMemFS()
	path := FileName("db")

	if err := Save(fs, path, New()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(fs, path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.NextSegmentID != 1 || got.AppliedSeq != 0 || len(got.Leaves) != 0 {
		t.Fatalf("unexpected manifest: %+v", got)
	}
}

func TestLoadMissingFile(t *testing.T) {
	fs := disk.NewMemFS()

	got, err := Load(fs, FileName("db"))
	if err == nil {
		t.Fatalf("Load of missing file returned manifest %+v, want error", got)
	}
	if !errors.Is(err, ErrNotExist) {
		t.Errorf("error %v does not match ErrNotExist", err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error %v does not match os.ErrNotExist", err)
	}
	if !IsNotExist(err) {
		t.Errorf("IsNotExist(%v) = false, want true", err)
	}
	if errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrCorruptManifestData) {
		t.Errorf("missing file must not look like corruption: %v", err)
	}
}

func TestDecodeCorruptedBodyByte(t *testing.T) {
	data, err := EncodeManifest(sampleManifest())
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}

	for i := manifestHeaderSize; i < len(data)-manifestChecksumSize; i++ {
		corrupt := append([]byte(nil), data...)
		corrupt[i] ^= 0x01

		if _, err := DecodeManifest(corrupt); !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("corrupting byte %d: err = %v, want ErrChecksumMismatch", i, err)
		}
	}
}

func TestDecodeCorruptedHeader(t *testing.T) {
	data, err := EncodeManifest(sampleManifest())
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}

	badMagic := append([]byte(nil), data...)
	badMagic[0] = 'X'
	if _, err := DecodeManifest(badMagic); !errors.Is(err, ErrManifestMagicMismatch) {
		t.Errorf("bad magic: err = %v, want ErrManifestMagicMismatch", err)
	}

	badVersion := append([]byte(nil), data...)
	badVersion[4] = manifestVersion + 1
	if _, err := DecodeManifest(badVersion); !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("bad version: err = %v, want ErrUnsupportedVersion", err)
	}
}

func TestDecodeTruncatedFile(t *testing.T) {
	data, err := EncodeManifest(sampleManifest())
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}

	for n := 0; n < len(data); n++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodeManifest panicked on %d byte prefix: %v", n, r)
				}
			}()
			m, err := DecodeManifest(data[:n])
			if err == nil {
				t.Fatalf("DecodeManifest(%d byte prefix) = %+v, want error", n, m)
			}
			if m != nil {
				t.Fatalf("DecodeManifest(%d byte prefix) returned manifest with error %v", n, err)
			}
		}()
	}

	short := data[:manifestHeaderSize+manifestBodyHeadSize+manifestChecksumSize-1]
	if _, err := DecodeManifest(short); !errors.Is(err, ErrShortManifestBuffer) {
		t.Errorf("short buffer: err = %v, want ErrShortManifestBuffer", err)
	}
}

func TestDecodeCorruptLeafCount(t *testing.T) {
	// A leaf count larger than the body can hold must be rejected before any
	// allocation is sized from it.
	m := &Manifest{NextSegmentID: 1}
	data, err := EncodeManifest(m)
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	body := data[manifestHeaderSize : len(data)-manifestChecksumSize]
	body[16] = 0xff
	body[17] = 0xff
	body[18] = 0xff
	body[19] = 0x7f
	rechecksum(data)

	if _, err := DecodeManifest(data); !errors.Is(err, ErrCorruptManifestData) {
		t.Errorf("err = %v, want ErrCorruptManifestData", err)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		leaves  []LeafRecord
		wantErr error
	}{
		{
			name:    "empty is valid",
			leaves:  nil,
			wantErr: nil,
		},
		{
			name: "sorted is valid",
			leaves: []LeafRecord{
				{LeafID: 1},
				{LeafID: 2, LowKey: []byte("b")},
				{LeafID: 3, LowKey: []byte("c")},
			},
			wantErr: nil,
		},
		{
			name: "unsorted leaves",
			leaves: []LeafRecord{
				{LeafID: 1},
				{LeafID: 2, LowKey: []byte("c")},
				{LeafID: 3, LowKey: []byte("b")},
			},
			wantErr: ErrLeavesNotSorted,
		},
		{
			name: "duplicate low key",
			leaves: []LeafRecord{
				{LeafID: 1},
				{LeafID: 2, LowKey: []byte("b")},
				{LeafID: 3, LowKey: []byte("b")},
			},
			wantErr: ErrDuplicateLowKey,
		},
		{
			name: "empty low key not first",
			leaves: []LeafRecord{
				{LeafID: 1},
				{LeafID: 2, LowKey: []byte{}},
			},
			wantErr: ErrEmptyLowKeyNotFirst,
		},
		{
			name: "duplicate leaf id",
			leaves: []LeafRecord{
				{LeafID: 1},
				{LeafID: 1, LowKey: []byte("b")},
			},
			wantErr: ErrDuplicateLeafID,
		},
		{
			name: "first leaf must have empty low key",
			leaves: []LeafRecord{
				{LeafID: 1, LowKey: []byte("a")},
				{LeafID: 2, LowKey: []byte("b")},
			},
			wantErr: ErrMissingLeftmostLeaf,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &Manifest{NextSegmentID: 1, Leaves: tc.leaves}
			err := m.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				return
			}
			// An invalid manifest must never reach disk.
			if err := Save(disk.NewMemFS(), FileName("db"), m); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Save = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestFindLeaf(t *testing.T) {
	m := &Manifest{
		NextSegmentID: 10,
		Leaves: []LeafRecord{
			{LeafID: 1, LowKey: nil},
			{LeafID: 2, LowKey: []byte("d")},
			{LeafID: 3, LowKey: []byte("m")},
			{LeafID: 4, LowKey: []byte{0xff, 0x00}},
		},
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	tests := []struct {
		name      string
		key       []byte
		wantIndex int
		wantOK    bool
	}{
		{name: "empty key hits leftmost leaf", key: nil, wantIndex: 0, wantOK: true},
		{name: "before first boundary", key: []byte("a"), wantIndex: 0, wantOK: true},
		{name: "exactly on boundary", key: []byte("d"), wantIndex: 1, wantOK: true},
		{name: "just before boundary", key: []byte("cz"), wantIndex: 0, wantOK: true},
		{name: "between boundaries", key: []byte("f"), wantIndex: 1, wantOK: true},
		{name: "exactly on last boundary", key: []byte{0xff, 0x00}, wantIndex: 3, wantOK: true},
		{name: "just before last boundary", key: []byte{0xff}, wantIndex: 2, wantOK: true},
		{name: "past last boundary", key: []byte{0xff, 0xff, 0xff}, wantIndex: 3, wantOK: true},
		{name: "key with nul bytes", key: []byte("d\x00\x00"), wantIndex: 1, wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			leaf, idx, ok := m.FindLeaf(tc.key)
			if ok != tc.wantOK || idx != tc.wantIndex {
				t.Fatalf("FindLeaf(%q) = (idx %d, ok %v), want (idx %d, ok %v)", tc.key, idx, ok, tc.wantIndex, tc.wantOK)
			}
			if ok && leaf.LeafID != m.Leaves[idx].LeafID {
				t.Fatalf("FindLeaf(%q) leaf = %+v, want %+v", tc.key, leaf, m.Leaves[idx])
			}
		})
	}
}

func TestFindLeafEdgeCases(t *testing.T) {
	empty := New()
	if leaf, idx, ok := empty.FindLeaf([]byte("k")); ok || idx != -1 {
		t.Errorf("FindLeaf on empty manifest = (%+v, %d, %v), want (zero, -1, false)", leaf, idx, ok)
	}

	var nilManifest *Manifest
	if _, idx, ok := nilManifest.FindLeaf([]byte("k")); ok || idx != -1 {
		t.Errorf("FindLeaf on nil manifest = (%d, %v), want (-1, false)", idx, ok)
	}

	// A leaf set that does not start at the empty low key leaves keys to the
	// left uncovered. Validate rejects it, but FindLeaf must not lie about it.
	partial := &Manifest{Leaves: []LeafRecord{{LeafID: 1, LowKey: []byte("m")}}}
	if _, idx, ok := partial.FindLeaf([]byte("a")); ok || idx != -1 {
		t.Errorf("FindLeaf left of coverage = (%d, %v), want (-1, false)", idx, ok)
	}
	if _, idx, ok := partial.FindLeaf([]byte("z")); !ok || idx != 0 {
		t.Errorf("FindLeaf right of coverage = (%d, %v), want (0, true)", idx, ok)
	}
}

func TestFindLeafDoesNotAllocate(t *testing.T) {
	m := New()
	if err := m.AddLeaf(LeafRecord{LeafID: 1}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	for i := byte('b'); i <= 'z'; i++ {
		if err := m.AddLeaf(LeafRecord{LeafID: uint64(i), LowKey: []byte{i}}); err != nil {
			t.Fatalf("AddLeaf: %v", err)
		}
	}

	key := []byte("q")
	var idx int
	allocs := testing.AllocsPerRun(100, func() {
		_, idx, _ = m.FindLeaf(key)
	})
	if allocs != 0 {
		t.Errorf("FindLeaf allocated %v objects per run, want 0 (last idx %d)", allocs, idx)
	}
}

func TestAddLeaf(t *testing.T) {
	m := New()

	if err := m.AddLeaf(LeafRecord{LeafID: 1, LowKey: []byte("a")}); !errors.Is(err, ErrMissingLeftmostLeaf) {
		t.Fatalf("first leaf with non-empty low key: err = %v, want ErrMissingLeftmostLeaf", err)
	}
	if err := m.AddLeaf(LeafRecord{LeafID: 1}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	if err := m.AddLeaf(LeafRecord{LeafID: 3, LowKey: []byte("m")}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	// Out-of-order insert must land at its sorted position.
	if err := m.AddLeaf(LeafRecord{LeafID: 2, LowKey: []byte("d")}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for i, want := range []uint64{1, 2, 3} {
		if m.Leaves[i].LeafID != want {
			t.Fatalf("leaf %d id = %d, want %d", i, m.Leaves[i].LeafID, want)
		}
	}

	if err := m.AddLeaf(LeafRecord{LeafID: 9, LowKey: []byte("d")}); !errors.Is(err, ErrDuplicateLowKey) {
		t.Errorf("duplicate low key: err = %v, want ErrDuplicateLowKey", err)
	}
	if err := m.AddLeaf(LeafRecord{LeafID: 2, LowKey: []byte("x")}); !errors.Is(err, ErrDuplicateLeafID) {
		t.Errorf("duplicate leaf id: err = %v, want ErrDuplicateLeafID", err)
	}
	if err := m.AddLeaf(LeafRecord{LeafID: 9, LowKey: nil}); !errors.Is(err, ErrEmptyLowKeyNotFirst) {
		t.Errorf("second empty low key: err = %v, want ErrEmptyLowKeyNotFirst", err)
	}
	if len(m.Leaves) != 3 {
		t.Errorf("rejected inserts changed the manifest: %d leaves", len(m.Leaves))
	}

	// The manifest must not alias the caller's key buffer.
	key := []byte("s")
	if err := m.AddLeaf(LeafRecord{LeafID: 4, LowKey: key}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	key[0] = 'a'
	if _, idx, _ := m.FindLeaf([]byte("s")); idx != 3 {
		t.Errorf("mutating the caller key changed the manifest: idx = %d, want 3", idx)
	}
}

func TestSplitLeaf(t *testing.T) {
	m := New()
	if err := m.AddLeaf(LeafRecord{LeafID: 1, SegmentID: 5, SegmentVersion: 1}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	if err := m.AddLeaf(LeafRecord{LeafID: 2, LowKey: []byte("m"), SegmentID: 6, SegmentVersion: 1}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}

	left := LeafRecord{LeafID: 1, LowKey: nil, SegmentID: 7, SegmentVersion: 1}
	right := LeafRecord{LeafID: 3, LowKey: []byte("f"), SegmentID: 8, SegmentVersion: 1}
	if err := m.SplitLeaf(1, left, right); err != nil {
		t.Fatalf("SplitLeaf: %v", err)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate after split: %v", err)
	}
	if len(m.Leaves) != 3 {
		t.Fatalf("leaf count = %d, want 3", len(m.Leaves))
	}

	for _, tc := range []struct {
		key        []byte
		wantLeafID uint64
	}{
		{key: nil, wantLeafID: 1},
		{key: []byte("a"), wantLeafID: 1},
		{key: []byte("ez"), wantLeafID: 1},
		{key: []byte("f"), wantLeafID: 3},
		{key: []byte("j"), wantLeafID: 3},
		{key: []byte("m"), wantLeafID: 2},
		{key: []byte("z"), wantLeafID: 2},
	} {
		leaf, _, ok := m.FindLeaf(tc.key)
		if !ok || leaf.LeafID != tc.wantLeafID {
			t.Errorf("FindLeaf(%q) = (leaf %d, ok %v), want leaf %d", tc.key, leaf.LeafID, ok, tc.wantLeafID)
		}
	}

	// Splits that break invariants are rejected and change nothing.
	before := len(m.Leaves)
	if err := m.SplitLeaf(99, left, right); !errors.Is(err, ErrLeafNotFound) {
		t.Errorf("unknown leaf: err = %v, want ErrLeafNotFound", err)
	}
	if err := m.SplitLeaf(3, LeafRecord{LeafID: 3, LowKey: []byte("g")}, LeafRecord{LeafID: 4, LowKey: []byte("h")}); !errors.Is(err, ErrSplitLowKeyChange) {
		t.Errorf("changed low key: err = %v, want ErrSplitLowKeyChange", err)
	}
	if err := m.SplitLeaf(3, LeafRecord{LeafID: 3, LowKey: []byte("f")}, LeafRecord{LeafID: 4, LowKey: []byte("f")}); !errors.Is(err, ErrInvalidSplitKey) {
		t.Errorf("split key equal to low key: err = %v, want ErrInvalidSplitKey", err)
	}
	if err := m.SplitLeaf(3, LeafRecord{LeafID: 3, LowKey: []byte("f")}, LeafRecord{LeafID: 4, LowKey: []byte("m")}); !errors.Is(err, ErrInvalidSplitKey) {
		t.Errorf("split key past the leaf range: err = %v, want ErrInvalidSplitKey", err)
	}
	if err := m.SplitLeaf(3, LeafRecord{LeafID: 3, LowKey: []byte("f")}, LeafRecord{LeafID: 1, LowKey: []byte("h")}); !errors.Is(err, ErrDuplicateLeafID) {
		t.Errorf("reused leaf id: err = %v, want ErrDuplicateLeafID", err)
	}
	if len(m.Leaves) != before {
		t.Errorf("rejected split changed the manifest: %d leaves, want %d", len(m.Leaves), before)
	}
}

func TestSplitLeafSurvivesRoundTrip(t *testing.T) {
	fs := disk.NewMemFS()
	path := FileName("db")

	m := New()
	if err := m.AddLeaf(LeafRecord{LeafID: 1, SegmentID: m.AllocateSegmentID(), SegmentVersion: 1}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	splitKey := []byte{0x00, 0x01}
	if err := m.SplitLeaf(1,
		LeafRecord{LeafID: 1, SegmentID: 1, SegmentVersion: 2},
		LeafRecord{LeafID: 2, LowKey: splitKey, SegmentID: m.AllocateSegmentID(), SegmentVersion: 1},
	); err != nil {
		t.Fatalf("SplitLeaf: %v", err)
	}
	if err := Save(fs, path, m); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(fs, path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertManifestEqual(t, got, m)

	if leaf, _, ok := got.FindLeaf([]byte{0x00, 0x00, 0xff}); !ok || leaf.LeafID != 1 {
		t.Errorf("FindLeaf left of split = (leaf %d, ok %v), want leaf 1", leaf.LeafID, ok)
	}
	if leaf, _, ok := got.FindLeaf([]byte{0x00, 0x01}); !ok || leaf.LeafID != 2 {
		t.Errorf("FindLeaf at split = (leaf %d, ok %v), want leaf 2", leaf.LeafID, ok)
	}
}

func TestSetLeafSegment(t *testing.T) {
	m := New()
	if err := m.AddLeaf(LeafRecord{LeafID: 1}); err != nil {
		t.Fatalf("AddLeaf: %v", err)
	}
	if m.Leaves[0].SegmentID != 0 {
		t.Fatalf("new leaf has segment %d, want 0", m.Leaves[0].SegmentID)
	}

	id := m.AllocateSegmentID()
	if err := m.SetLeafSegment(1, id, 4); err != nil {
		t.Fatalf("SetLeafSegment: %v", err)
	}
	if m.Leaves[0].SegmentID != id || m.Leaves[0].SegmentVersion != 4 {
		t.Errorf("leaf = %+v, want segment %d version 4", m.Leaves[0], id)
	}
	if err := m.SetLeafSegment(1, id, 5); err != nil {
		t.Fatalf("SetLeafSegment: %v", err)
	}
	if m.Leaves[0].SegmentVersion != 5 {
		t.Errorf("segment version = %d, want 5", m.Leaves[0].SegmentVersion)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := m.SetLeafSegment(99, 1, 1); !errors.Is(err, ErrLeafNotFound) {
		t.Errorf("unknown leaf: err = %v, want ErrLeafNotFound", err)
	}
}

func TestAllocateSegmentID(t *testing.T) {
	m := New()
	first, second := m.AllocateSegmentID(), m.AllocateSegmentID()
	if first != 1 || second != 2 || m.NextSegmentID != 3 {
		t.Fatalf("ids = (%d, %d), next = %d, want (1, 2) and 3", first, second, m.NextSegmentID)
	}

	// Segment id zero is reserved, so a zero-valued manifest must skip it.
	zero := &Manifest{}
	if id := zero.AllocateSegmentID(); id != 1 {
		t.Errorf("first id from zero-valued manifest = %d, want 1", id)
	}
}

func TestSaveOverwritesPreviousContents(t *testing.T) {
	fs := disk.NewMemFS()
	path := FileName("db")

	big := sampleManifest()
	if err := Save(fs, path, big); err != nil {
		t.Fatalf("Save: %v", err)
	}
	bigInfo, err := fs.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	small := &Manifest{
		NextSegmentID: 2,
		AppliedSeq:    99,
		Leaves:        []LeafRecord{{LeafID: 77, SegmentID: 1, SegmentVersion: 1}},
	}
	if err := Save(fs, path, small); err != nil {
		t.Fatalf("Save: %v", err)
	}
	smallInfo, err := fs.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if smallInfo.Size() >= bigInfo.Size() {
		t.Fatalf("file size after rewrite = %d, want less than %d", smallInfo.Size(), bigInfo.Size())
	}

	got, err := Load(fs, path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertManifestEqual(t, got, small)

	// No stale bytes from the previous manifest, and no leftover temp file.
	raw, err := disk.ReadFile(fs, path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.Contains(raw, []byte("user:0001")) {
		t.Errorf("rewritten manifest still contains old contents")
	}
	names, err := fs.List("db")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, name := range names {
		if name == path+".tmp" {
			t.Errorf("temp file %s was left behind", name)
		}
	}
}

func TestSaveLoadNilArguments(t *testing.T) {
	if err := Save(nil, "x", New()); !errors.Is(err, ErrNilFilesystem) {
		t.Errorf("Save with nil fs: err = %v, want ErrNilFilesystem", err)
	}
	if err := Save(disk.NewMemFS(), "x", nil); !errors.Is(err, ErrNilManifest) {
		t.Errorf("Save with nil manifest: err = %v, want ErrNilManifest", err)
	}
	if _, err := Load(nil, "x"); !errors.Is(err, ErrNilFilesystem) {
		t.Errorf("Load with nil fs: err = %v, want ErrNilFilesystem", err)
	}
}

func TestClone(t *testing.T) {
	orig := sampleManifest()
	clone := orig.Clone()
	assertManifestEqual(t, clone, orig)

	clone.Leaves[1].LowKey[0] = 0xaa
	clone.Leaves[0].SegmentVersion = 999
	clone.AppliedSeq = 0
	if orig.Leaves[1].LowKey[0] != 0x00 {
		t.Errorf("clone shares low key storage with the original")
	}
	if orig.Leaves[0].SegmentVersion == 999 || orig.AppliedSeq == 0 {
		t.Errorf("clone shares leaf storage with the original")
	}
}

// rechecksum recomputes the trailer so that deliberate body edits are seen as
// valid data rather than as checksum failures.
func rechecksum(data []byte) {
	body := data[manifestHeaderSize : len(data)-manifestChecksumSize]
	binary.LittleEndian.PutUint32(data[len(data)-manifestChecksumSize:], crc32.ChecksumIEEE(body))
}
