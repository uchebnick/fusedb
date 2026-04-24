package segment

import (
	"errors"
	"fmt"
	"math"
)

var ErrSectionOffsetOverflow = errors.New("segment: section offset overflow")

// Section describes one contiguous byte range in the segment file.
type Section struct {
	Offset uint64
	Length uint64
}

// Empty reports whether section is absent.
func (s Section) Empty() bool {
	return s.Offset == 0 && s.Length == 0
}

// EndOffset returns the first byte after section payload.
func (s Section) EndOffset() (uint64, error) {
	if s.Offset > math.MaxUint64-s.Length {
		return 0, ErrSectionOffsetOverflow
	}
	return s.Offset + s.Length, nil
}

// Footer is final segment metadata appended after all payload sections.
type Footer struct {
	BlockCount uint32
	Data       Section
	Bloom      Section
	Index      Section
}

// Validate checks footer invariants.
func (f Footer) Validate() error {
	if _, err := f.Data.EndOffset(); err != nil {
		return err
	}
	if _, err := f.Bloom.EndOffset(); err != nil {
		return err
	}
	if _, err := f.Index.EndOffset(); err != nil {
		return err
	}
	return nil
}

func (f Footer) String() string {
	return fmt.Sprintf("blocks=%d data=%+v bloom=%+v index=%+v", f.BlockCount, f.Data, f.Bloom, f.Index)
}
