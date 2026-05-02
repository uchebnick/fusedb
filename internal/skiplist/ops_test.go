package skiplist

import "testing"

func TestMergeInc(t *testing.T) {
	got := MergeInc(NewInc(2), NewInc(3))
	if delta := DecodeInc(got); delta != 5 {
		t.Fatalf("merged inc = %d, want 5", delta)
	}
}

func TestRandomHeightFromHashIsDeterministic(t *testing.T) {
	list := NewSkipList(42)

	first := list.randomHeight("account:100500")
	second := list.randomHeight("account:100500")
	if first != second {
		t.Fatalf("height changed for same key: %d != %d", first, second)
	}
	if first < 1 || first > maxHeight {
		t.Fatalf("height = %d, want within [1,%d]", first, maxHeight)
	}
}
