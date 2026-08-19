<h1 align="center">
  <img src="./assets/fusedb-logo-metal.png" width="520" alt="FuseDB">
</h1>

<p align="center">
  <strong>Встраиваемая key-value база для Go со стабильной задержкой под нагрузкой.</strong>
</p>

<p align="center">
  <a href="https://github.com/uchebnick/fusedb/actions/workflows/ci.yml"><img src="https://github.com/uchebnick/fusedb/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/uchebnick/fusedb/releases/latest"><img src="https://img.shields.io/github/v/release/uchebnick/fusedb?display_name=tag&sort=semver" alt="Release"></a>
  <a href="https://pkg.go.dev/github.com/uchebnick/fusedb/pkg/fusedb"><img src="https://pkg.go.dev/badge/github.com/uchebnick/fusedb/pkg/fusedb.svg" alt="Go Reference"></a>
  <img src="https://img.shields.io/badge/Go-1.25.13-00ADD8?logo=go&logoColor=white" alt="Go 1.25.13">
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-22c55e.svg" alt="MIT license"></a>
</p>

<p align="center">
  <a href="./README.md">English</a> ·
  <a href="./docs/README.md">Документация</a> ·
  <a href="./LIBRARY.md">API</a> ·
  <a href="./ARCHITECTURE.ru.md">Архитектура</a> ·
  <a href="./benchmarks/README.md">Бенчмарки</a>
</p>

## Что такое FuseDB?

FuseDB — конкурентная встраиваемая key-value база для Go. Keyspace разделён
между независимо обслуживаемыми листьями, поэтому merge остаются локальными и
не превращают фоновую работу в скачок задержки всей базы.

- **Надёжные примитивы:** `Get`, `Put`, `Delete`, атомарный `Inc`, условные
  batches, идемпотентные события, WAL с checksum и crash recovery.
- **Адаптивное обслуживание:** p95/p99, CPU, диск и память управляют запуском
  локальных merge и позволяют кооперативно прерывать обучение LZ4-словарей.
- **Эксплуатация:** verify, backup/restore, Prometheus, Grafana и
  qualification на целевом железе.

## Быстрый старт

Нужны Go 1.25.13+, C toolchain и нативная LZ4 (`liblz4-dev` в Linux или
`brew install lz4` в macOS).

```bash
go get github.com/uchebnick/fusedb/pkg/fusedb@v0.1.0
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

`DB` безопасна для конкурентного использования; чтения возвращают независимые
копии. Режимы durability, batches, counters, конфигурация и ошибки описаны в
[library guide](./LIBRARY.md).
