//go:build rocksdb

package kvbench

/*
#cgo pkg-config: rocksdb
#include <stdlib.h>
#include <rocksdb/c.h>
*/
import "C"

import (
	"fmt"
	"os/exec"
	"strings"
	"unsafe"
)

type enabledRocksDBFactory struct{}

func rocksDBFactory() engineFactory {
	return enabledRocksDBFactory{}
}

func (enabledRocksDBFactory) Info() EngineInfo {
	return EngineInfo{
		Name:      "rocksdb",
		Version:   rocksDBVersion(),
		Available: true,
	}
}

func (enabledRocksDBFactory) Open(dir string, opts engineOptions) (store, error) {
	options := C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(options, 1)
	C.rocksdb_options_set_write_buffer_size(options, C.size_t(opts.MemtableBytes))

	cache := C.rocksdb_cache_create_lru(C.size_t(opts.CacheBytes))
	blockOptions := C.rocksdb_block_based_options_create()
	C.rocksdb_block_based_options_set_block_cache(blockOptions, cache)
	C.rocksdb_options_set_block_based_table_factory(options, blockOptions)

	path := C.CString(dir)
	defer C.free(unsafe.Pointer(path))
	var errorMessage *C.char
	db := C.rocksdb_open(options, path, &errorMessage)
	if err := consumeRocksDBError(errorMessage); err != nil {
		C.rocksdb_block_based_options_destroy(blockOptions)
		C.rocksdb_cache_destroy(cache)
		C.rocksdb_options_destroy(options)
		return nil, err
	}

	readOptions := C.rocksdb_readoptions_create()
	writeOptions := C.rocksdb_writeoptions_create()
	if opts.Durability == DurabilitySync {
		C.rocksdb_writeoptions_set_sync(writeOptions, 1)
	}
	flushOptions := C.rocksdb_flushoptions_create()
	C.rocksdb_flushoptions_set_wait(flushOptions, 1)
	return &rocksDBStore{
		db:           db,
		options:      options,
		blockOptions: blockOptions,
		cache:        cache,
		readOptions:  readOptions,
		writeOptions: writeOptions,
		flushOptions: flushOptions,
	}, nil
}

type rocksDBStore struct {
	db           *C.rocksdb_t
	options      *C.rocksdb_options_t
	blockOptions *C.rocksdb_block_based_table_options_t
	cache        *C.rocksdb_cache_t
	readOptions  *C.rocksdb_readoptions_t
	writeOptions *C.rocksdb_writeoptions_t
	flushOptions *C.rocksdb_flushoptions_t
}

func (s *rocksDBStore) Get(key []byte) ([]byte, bool, error) {
	var valueLength C.size_t
	var errorMessage *C.char
	value := C.rocksdb_get(
		s.db,
		s.readOptions,
		bytesPointer(key),
		C.size_t(len(key)),
		&valueLength,
		&errorMessage,
	)
	if err := consumeRocksDBError(errorMessage); err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}
	defer C.rocksdb_free(unsafe.Pointer(value))
	return C.GoBytes(unsafe.Pointer(value), C.int(valueLength)), true, nil
}

func (s *rocksDBStore) Put(key, value []byte) error {
	var errorMessage *C.char
	C.rocksdb_put(
		s.db,
		s.writeOptions,
		bytesPointer(key),
		C.size_t(len(key)),
		bytesPointer(value),
		C.size_t(len(value)),
		&errorMessage,
	)
	return consumeRocksDBError(errorMessage)
}

func (s *rocksDBStore) Flush() error {
	var errorMessage *C.char
	C.rocksdb_flush(s.db, s.flushOptions, &errorMessage)
	return consumeRocksDBError(errorMessage)
}

func (s *rocksDBStore) Close() error {
	C.rocksdb_close(s.db)
	C.rocksdb_flushoptions_destroy(s.flushOptions)
	C.rocksdb_writeoptions_destroy(s.writeOptions)
	C.rocksdb_readoptions_destroy(s.readOptions)
	C.rocksdb_block_based_options_destroy(s.blockOptions)
	C.rocksdb_cache_destroy(s.cache)
	C.rocksdb_options_destroy(s.options)
	return nil
}

func bytesPointer(value []byte) *C.char {
	if len(value) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(unsafe.SliceData(value)))
}

func consumeRocksDBError(message *C.char) error {
	if message == nil {
		return nil
	}
	defer C.rocksdb_free(unsafe.Pointer(message))
	return fmt.Errorf("rocksdb: %s", C.GoString(message))
}

func rocksDBVersion() string {
	output, err := exec.Command("pkg-config", "--modversion", "rocksdb").Output()
	if err != nil {
		return "native"
	}
	return strings.TrimSpace(string(output))
}
