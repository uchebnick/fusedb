package disk

import (
	"bytes"
	"testing"
)

func TestReadWriteFileAtomically(t *testing.T) {
	fs := NewMemFS()
	name := "state/dict.bin"
	want := []byte("hello world")

	if err := WriteFileAtomically(fs, name, want); err != nil {
		t.Fatalf("write file atomically: %v", err)
	}

	got, err := ReadFile(fs, name)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read file = %q, want %q", got, want)
	}
}
