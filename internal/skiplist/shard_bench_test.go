package skiplist

import (
	"testing"

	"github.com/cespare/xxhash/v2"
)

const benchShardCount = 64

var benchShardSink uint64

func BenchmarkShardLookup(b *testing.B) {
	key := []byte("tenant:42:account:100500")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchShardSink = xxhash.Sum64(key) & (benchShardCount - 1)
	}
}
