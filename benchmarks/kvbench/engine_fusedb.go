package kvbench

import "github.com/uchebnick/fusedb/pkg/fusedb"

type fuseDBFactory struct{}

func (fuseDBFactory) Info() EngineInfo {
	return EngineInfo{
		Name:      "fusedb",
		Version:   dependencyVersion("github.com/uchebnick/fusedb"),
		Available: true,
	}
}

func (fuseDBFactory) Open(dir string, opts engineOptions) (store, error) {
	db, err := fusedb.Open(fusedb.Options{
		Dir:           dir,
		CacheSize:     opts.CacheBytes,
		MergeSize:     opts.MemtableBytes,
		MaxLeafSize:   64 << 20,
		WALSyncWrites: opts.Durability == DurabilitySync,
		DictionaryTraining: fusedb.DictionaryTrainingOptions{
			Disabled: true,
		},
	})
	if err != nil {
		return nil, err
	}
	return &fuseDBStore{db: db}, nil
}

type fuseDBStore struct {
	db *fusedb.DB
}

func (s *fuseDBStore) Get(key []byte) ([]byte, bool, error) {
	return s.db.Get(key)
}

func (s *fuseDBStore) Put(key, value []byte) error {
	return s.db.Put(key, value)
}

func (s *fuseDBStore) Flush() error {
	return s.db.Merge()
}

func (s *fuseDBStore) Close() error {
	return s.db.Close()
}
