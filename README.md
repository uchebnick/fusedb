<h1 align="center">
  <img src="./assets/fusedb-logo-metal.png" width="520" alt="FuseDB">
</h1>

<p align="center">
  <strong>An embedded Go key-value engine built for predictable latency under load.</strong>
</p>

<p align="center">
  <a href="https://github.com/uchebnick/fusedb/actions/workflows/ci.yml"><img src="https://github.com/uchebnick/fusedb/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pkg.go.dev/github.com/uchebnick/fusedb/pkg/fusedb"><img src="https://pkg.go.dev/badge/github.com/uchebnick/fusedb/pkg/fusedb.svg" alt="Go Reference"></a>
  <img src="https://img.shields.io/badge/Go-1.25.13-00ADD8?logo=go&logoColor=white" alt="Go 1.25.13">
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-22c55e.svg" alt="MIT license"></a>
</p>

<p align="center">
  <a href="./README.ru.md">Русский</a> ·
  <a href="./docs/README.md">Documentation</a> ·
  <a href="./LIBRARY.md">API</a> ·
  <a href="./ARCHITECTURE.md">Architecture</a> ·
  <a href="./benchmarks/README.md">Benchmarks</a>
</p>

## What is FuseDB?

FuseDB is a concurrent embedded key-value engine for Go. It splits the keyspace
into independently maintained leaves, keeping merges local instead of turning
background maintenance into a database-wide latency event.

- **Durable primitives:** `Get`, `Put`, `Delete`, atomic `Inc`, conditional
  batches, idempotent events, checksummed WAL, and crash recovery.
- **Adaptive maintenance:** p95/p99 latency and CPU, disk, and RAM budgets
  control local merge admission and cooperatively interrupt LZ4 training.
- **Operations included:** verification, backup/restore, Prometheus metrics,
  Grafana dashboards, and target-hardware qualification.

## Quick start

Requires Go 1.25.13+, a C toolchain, and native LZ4 (`liblz4-dev` on Linux or
`brew install lz4` on macOS).

```bash
go get github.com/uchebnick/fusedb/pkg/fusedb@latest
```

```go
package main

import (
    "fmt"
    "log"

    "github.com/uchebnick/fusedb/pkg/fusedb"
)

func main() {
    db, err := fusedb.Open(fusedb.DurablePilotOptions("./data"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    if err := db.Put([]byte("user:42"), []byte("Ada")); err != nil {
        log.Fatal(err)
    }

    value, found, err := db.Get([]byte("user:42"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("found=%t value=%s\n", found, value)
}
```

`DB` is safe for concurrent use; reads return owned copies. See the
[library guide](./LIBRARY.md) for durability, batching, counters, configuration,
and error contracts.
