package segment

import "testing"

func TestParseSegmentFileNameAcceptsOnlyCanonicalNames(t *testing.T) {
	final := SegmentFileName("db", 12, 34)
	temporary := SegmentTempFileName("db", 12, 34)
	for _, test := range []struct {
		name          string
		wantOK        bool
		wantTemporary bool
	}{
		{name: final, wantOK: true},
		{name: temporary, wantOK: true, wantTemporary: true},
		{name: "segment-12-v34.seg"},
		{name: "segment-00000000000000000000-v00000000000000000034.seg"},
		{name: "segment-00000000000000000012-v00000000000000000000.seg"},
		{name: "segment-00000000000000000012-v00000000000000000034.seg.bak"},
		{name: "notes.txt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			id, version, temporary, ok := ParseSegmentFileName(test.name)
			if ok != test.wantOK || temporary != test.wantTemporary {
				t.Fatalf("parse = (%d,%d,temp=%v,ok=%v)", id, version, temporary, ok)
			}
			if ok && (id != 12 || version != 34) {
				t.Fatalf("identity = (%d,%d)", id, version)
			}
		})
	}
}
