package skiplist

import (
	"github.com/uchebnick/fusedb/internal/ops"
	"testing"
)

func TestMergeInc(t *testing.T) {
	got := ops.MergeInc(ops.NewInc(2), ops.NewInc(3))
	if delta := ops.DecodeInc(got); delta != 5 {
		t.Fatalf("merged inc = %d, want 5", delta)
	}
}

func TestRandomHeightFromHashIsDeterministic(t *testing.T) {
	list := NewSkipList(42)

	first := list.randomHeight([]byte("account:100500"))
	second := list.randomHeight([]byte("account:100500"))
	if first != second {
		t.Fatalf("height changed for same key: %d != %d", first, second)
	}
	if first < 1 || first > maxHeight {
		t.Fatalf("height = %d, want within [1,%d]", first, maxHeight)
	}
}
