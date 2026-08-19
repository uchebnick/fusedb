//go:build !cgo

package compression

import (
	"errors"
	"testing"
)

func TestDictionaryReportsCGORequirement(t *testing.T) {
	_, err := NewDictionary(1, []byte("dictionary"))
	if !errors.Is(err, ErrCGODisabled) {
		t.Fatalf("NewDictionary error = %v, want %v", err, ErrCGODisabled)
	}
}
