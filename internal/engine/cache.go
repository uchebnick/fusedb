package engine

import (
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

// cacheEpochShards is the number of independent invalidation counters.
//
// A single global epoch, which is what this cache used to have, made every
// write invalidate every cached entry: under any mixed read/write load the hit
// rate collapsed to roughly zero. Sharding the counter by key hash confines a
// write to one shard, so a write to one key leaves the other shards' entries
// usable. It costs one array of counters and keeps the read path lock-free.
const (
	cacheEpochShards = 256
	// cacheEntryOverhead conservatively covers the retained string/slice
	// headers, cacheEntry fields, map bucket share, pointers, and allocator
	// rounding. Exact Go heap accounting is runtime-specific, but charging only
	// payload bytes lets millions of tiny entries exceed the configured budget
	// by an order of magnitude.
	cacheEntryOverhead int64 = 160
)

type valueCache struct {
	mu       sync.RWMutex
	maxBytes int64
	used     int64
	items    map[string]cacheEntry
}

type cacheEntry struct {
	value []byte
	epoch uint64
	size  int64
}

func newValueCache(maxBytes int64) *valueCache {
	if maxBytes <= 0 {
		return nil
	}
	return &valueCache{
		maxBytes: maxBytes,
		items:    make(map[string]cacheEntry),
	}
}

func (c *valueCache) get(key []byte, epoch uint64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.items[cacheKeyView(key)]
	if !ok || entry.epoch != epoch {
		return nil, false
	}
	// Return a copy to prevent caller from modifying cached data
	valueCopy := make([]byte, len(entry.value))
	copy(valueCopy, entry.value)
	return valueCopy, true
}

// set stores an owned copy of value under key.
//
// The cache always clones: callers pass views of skiplist or block memory that
// can be rewritten or released underneath it.
func (c *valueCache) set(key, value []byte, epoch uint64) {
	if c == nil {
		return
	}
	size := int64(len(key)+len(value)) + cacheEntryOverhead
	if size > c.maxBytes {
		c.delete(key)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	cacheKey := string(key)
	if old, ok := c.items[cacheKey]; ok {
		c.used -= old.size
	}
	for c.used+size > c.maxBytes {
		for evict := range c.items {
			c.used -= c.items[evict].size
			delete(c.items, evict)
			break
		}
	}
	owned := make([]byte, len(value))
	copy(owned, value)
	c.items[cacheKey] = cacheEntry{
		value: owned,
		epoch: epoch,
		size:  size,
	}
	c.used += size
}

func (c *valueCache) delete(key []byte) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cacheKey := cacheKeyView(key)
	if old, ok := c.items[cacheKey]; ok {
		c.used -= old.size
		delete(c.items, cacheKey)
	}
}

// cacheKeyView borrows key bytes as a map lookup string without allocating.
//
// Taking the address of key[0] panics on an empty key, so zero-length keys are
// handled explicitly rather than left to crash the caller.
func cacheKeyView(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return unsafe.String(&key[0], len(key))
}

// cacheShard picks the invalidation counter that owns key.
func cacheShard(key []byte) uint64 {
	return xxhash.Sum64(key) % cacheEpochShards
}
