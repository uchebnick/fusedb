package kvbench

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

type badgerFactory struct{}

func (badgerFactory) Info() EngineInfo {
	return EngineInfo{
		Name:      "badger",
		Version:   dependencyVersion("github.com/dgraph-io/badger/v4"),
		Available: true,
	}
}

func (badgerFactory) Open(dir string, opts engineOptions) (store, error) {
	indexCache := opts.CacheBytes / 4
	blockCache := opts.CacheBytes - indexCache
	db, err := badger.Open(badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(opts.Durability == DurabilitySync).
		WithMemTableSize(opts.MemtableBytes).
		WithBlockCacheSize(blockCache).
		WithIndexCacheSize(indexCache).
		WithValueThreshold(1 << 10))
	if err != nil {
		return nil, err
	}
	return &badgerStore{db: db}, nil
}

type badgerStore struct {
	db *badger.DB
}

func (s *badgerStore) Get(key []byte) ([]byte, bool, error) {
	var owned []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		owned, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return owned, true, nil
}

func (s *badgerStore) Put(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *badgerStore) Flush() error {
	return s.db.Sync()
}

func (s *badgerStore) Close() error {
	return s.db.Close()
}
