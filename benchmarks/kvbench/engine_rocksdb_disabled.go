//go:build !rocksdb

package kvbench

type unavailableRocksDBFactory struct{}

func rocksDBFactory() engineFactory {
	return unavailableRocksDBFactory{}
}

func (unavailableRocksDBFactory) Info() EngineInfo {
	return EngineInfo{
		Name:        "rocksdb",
		Version:     "native",
		Available:   false,
		Unavailable: "rebuild the harness with -tags rocksdb and install librocksdb",
	}
}

func (unavailableRocksDBFactory) Open(string, engineOptions) (store, error) {
	panic("unavailable RocksDB adapter was selected")
}
