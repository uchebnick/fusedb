package oneleafdb

import (
	"sync"
	"unsafe"
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
	entry, ok := c.items[unsafe.String(&key[0], len(key))]
	if !ok || entry.epoch != epoch {
		return nil, false
	}
	// Return a copy to prevent caller from modifying cached data
	valueCopy := make([]byte, len(entry.value))
	copy(valueCopy, entry.value)
	return valueCopy, true
}

func (c *valueCache) set(key, value []byte, epoch uint64) {
	if c == nil {
		return
	}
	size := int64(len(key) + len(value))
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
	c.items[cacheKey] = cacheEntry{
		value: value,
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
	cacheKey := unsafe.String(&key[0], len(key))
	if old, ok := c.items[cacheKey]; ok {
		c.used -= old.size
		delete(c.items, cacheKey)
	}
}

func (c *valueCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.items)
	c.used = 0
}
