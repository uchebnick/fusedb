# FuseDB

[![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat)](./LICENSE)

Embedded key-value database for hot mutable state.

[Русский](./README.ru.md)

## Performance

Comparison with Pebble (NoSync mode):

- Reads: 1.0µs vs 4.1µs
- Writes: 1.1µs vs 1.1µs
- Startup: 224ms vs 26s (open + first read)
- Atomic counters: 257ns Inc operation

## Install

```bash
go get github.com/uchebnick/fusedb
```

## Usage

```go
db, _ := oneleafdb.OpenDB(oneleafdb.DBOptions{
    Dir:            "/tmp/db",
    CacheBytes:     5 << 20,
    ThresholdBytes: 5 << 20,
})
defer db.Close()

db.Put([]byte("key"), []byte("value"))
db.Inc([]byte("counter"), 1)
value, found, _ := db.Get([]byte("key"))
```

## Architecture

- Lock-free skiplist buffer
- Immutable segments with LZ4 compression
- Async WAL with group commit

Details: [ARCHITECTURE.md](./ARCHITECTURE.md)

## Status

Experimental. Not production-ready.
