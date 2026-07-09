package skiplist

import (
	"fmt"
	"testing"

	"github.com/uchebnick/fusedb/internal/ops"
)

// BenchmarkUpdateNodeContention benchmarks concurrent Inc operations on a single
// hot key to measure CAS contention behavior with and without Gosched backoff.
func BenchmarkUpdateNodeContention(b *testing.B) {
	for _, goroutines := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			sl := NewSkipList(0)
			hotKey := []byte("hot-key")

			// Pre-insert the key so all goroutines only hit updateNode (no inserts).
			sl.Apply(hotKey, ops.NewInc(0))

			b.ResetTimer()
			b.SetParallelism(goroutines)
			b.RunParallel(func(pb *testing.PB) {
				op := ops.NewInc(1)
				for pb.Next() {
					sl.Apply(hotKey, op)
				}
			})
		})
	}
}
