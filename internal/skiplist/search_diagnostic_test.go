package skiplist

import (
	"bytes"
	"fmt"
	"fusedb/internal/ops"
	"testing"
)

func TestReadPathDiagnostics64K(t *testing.T) {
	if testing.Short() {
		t.Skip("diagnostic")
	}

	list := NewSkipList(42)
	for i := 0; i < 64*1024; i++ {
		list.Apply([]byte(fmt.Sprintf("key:%08d", i)), ops.NewPut([]byte("value")))
	}

	keys := []string{
		"key:00000000",
		"key:00000001",
		"key:00000128",
		"key:00001024",
		"key:00008192",
		"key:00016384",
		"key:00032768",
		"key:00049152",
		"key:00065535",
		"key:99999999",
	}

	for _, key := range keys {
		loads, moves, drops, found := list.searchStats([]byte(key))
		t.Logf("%s found=%v loads=%d moves=%d drops=%d", key, found, loads, moves, drops)
	}
}

func (s *SkipList) searchStats(key []byte) (loads, moves, drops int, found bool) {
	x := s.head

	for level := s.height.Load() - 1; level >= 0; level-- {
		drops++
		for {
			next := x.next[level].Load()
			loads++
			if next == nil || bytes.Compare(next.key, key) >= 0 {
				if level == 0 && next != nil && bytes.Equal(next.key, key) {
					found = true
				}
				break
			}
			x = next
			moves++
		}
	}

	return loads, moves, drops, found
}
