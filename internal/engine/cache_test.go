package engine

import (
	"fmt"
	"testing"
)

func TestValueCacheChargesMetadataForSmallEntries(t *testing.T) {
	const budget = int64(1024)
	cache := newValueCache(budget)
	for i := range 100 {
		cache.set([]byte(fmt.Sprintf("key-%04d", i)), nil, 1)
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if cache.used > budget {
		t.Fatalf("charged bytes = %d, budget = %d", cache.used, budget)
	}
	maximum := int(budget / (cacheEntryOverhead + 8))
	if len(cache.items) > maximum {
		t.Fatalf("tiny entries retained = %d, want at most %d under metadata-aware budget", len(cache.items), maximum)
	}
}
