package tree

import (
	"testing"

	"github.com/uchebnick/fusedb/internal/manifest"
)

func TestRebalanceDictionaryGroupsCreatesStableContiguousChunks(t *testing.T) {
	leaves := make([]manifest.LeafRecord, 20)
	for i := range leaves {
		leaves[i].LeafID = uint64(i + 1)
		if i > 0 {
			leaves[i].LowKey = []byte{byte(i)}
		}
	}
	if !rebalanceDictionaryGroups(leaves, 8) {
		t.Fatal("expected rebalance")
	}
	for i, want := range []uint64{1, 1, 1, 1, 1, 1, 1, 1, 9, 9, 9, 9, 9, 9, 9, 9, 17, 17, 17, 17} {
		if got := leaves[i].DictionaryGroup(); got != want {
			t.Fatalf("leaf %d group = %d, want %d", i+1, got, want)
		}
	}
	m := &manifest.Manifest{NextSegmentID: 1, Leaves: leaves}
	if err := m.Validate(); err != nil {
		t.Fatalf("rebalanced manifest: %v", err)
	}
	if rebalanceDictionaryGroups(leaves, 8) {
		t.Fatal("second rebalance was not stable")
	}
}
