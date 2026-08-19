package kvbench

import (
	"errors"

	"github.com/cockroachdb/pebble"
)

type pebbleFactory struct{}

func (pebbleFactory) Info() EngineInfo {
	return EngineInfo{
		Name:      "pebble",
		Version:   dependencyVersion("github.com/cockroachdb/pebble"),
		Available: true,
	}
}

func (pebbleFactory) Open(dir string, opts engineOptions) (store, error) {
	cache := pebble.NewCache(opts.CacheBytes)
	db, err := pebble.Open(dir, &pebble.Options{
		Cache:                       cache,
		MemTableSize:                uint64(opts.MemtableBytes),
		MemTableStopWritesThreshold: 4,
		Logger:                      quietPebbleLogger{},
	})
	if err != nil {
		cache.Unref()
		return nil, err
	}
	writeOptions := pebble.NoSync
	if opts.Durability == DurabilitySync {
		writeOptions = pebble.Sync
	}
	return &pebbleStore{db: db, cache: cache, writeOptions: writeOptions}, nil
}

type quietPebbleLogger struct{}

func (quietPebbleLogger) Infof(string, ...any)  {}
func (quietPebbleLogger) Fatalf(string, ...any) {}

type pebbleStore struct {
	db           *pebble.DB
	cache        *pebble.Cache
	writeOptions *pebble.WriteOptions
}

func (s *pebbleStore) Get(key []byte) ([]byte, bool, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	owned := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		return nil, false, err
	}
	return owned, true, nil
}

func (s *pebbleStore) Put(key, value []byte) error {
	return s.db.Set(key, value, s.writeOptions)
}

func (s *pebbleStore) Flush() error {
	return s.db.Flush()
}

func (s *pebbleStore) Close() error {
	err := s.db.Close()
	s.cache.Unref()
	return err
}
