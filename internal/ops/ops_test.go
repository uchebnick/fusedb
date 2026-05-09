package ops

import (
	"bytes"
	"testing"
)

func TestNewPutOwnsData(t *testing.T) {
	value := []byte("stable")
	op := NewPut(value)
	value[0] = 'X'

	if op.Kind != OpPut {
		t.Fatalf("kind = %d, want put", op.Kind)
	}
	if !bytes.Equal(op.Data, []byte("stable")) {
		t.Fatalf("data = %q, want stable", op.Data)
	}
}

func TestMergeInc(t *testing.T) {
	got := MergeInc(NewInc(2), NewInc(3))
	if delta := DecodeInc(got); delta != 5 {
		t.Fatalf("merged inc = %d, want 5", delta)
	}
}

func TestOpCloneOwnsData(t *testing.T) {
	op := NewPut([]byte("stable"))
	clone := op.Clone()
	clone.Data[0] = 'X'

	if string(op.Data) != "stable" {
		t.Fatalf("clone mutated original: %q", op.Data)
	}
}
