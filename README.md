# FuseDB

<div align="center">

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat)](./LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen?style=flat)](https://github.com/uchebnick/fusedb)
[![Go Report](https://img.shields.io/badge/go%20report-A+-brightgreen?style=flat)](https://goreportcard.com/report/github.com/uchebnick/fusedb)
[![Documentation](https://img.shields.io/badge/docs-latest-blue?style=flat)](./ARCHITECTURE.md)

**[English](#english)** | **[Русский](#russian)**

</div>

---

<a name="english"></a>

## 🚀 FuseDB - High-Performance Embedded Key-Value Engine

FuseDB is an experimental embedded key-value database optimized for **small hot mutable state** - rate limiters, counters, sessions, quotas, and metadata. Built around **leaf-local mutation buffers** and **immutable segments** instead of global LSM compaction.

### ⚡ Performance Highlights

**vs Pebble (NoSync mode):**
- **3x faster reads**: 1.3µs p50 vs 4µs
- **1.4x faster writes**: 292ns p50 vs 416ns  
- **117x faster startup**: 224ms vs 26s (open + first read)
- **Native atomic counters**: 257ns Inc operation

**Memory efficiency:**
- 97-126 MB RSS under load (6-8x improvement after optimization)
- 334 B/op for Get operations (10.5x reduction)
- LZ4 dictionary compression: 62.6% space savings, 4 GB/s decompression

### 🎯 Key Features

- **Lock-free skiplist** mutation buffer with Put, Delete, and Inc (atomic counters)
- **LZ4 dictionary compression** with persistent registry and 4KB dictionaries
- **WAL with async group commit** (200µs batching interval)
- **5MB LRU value cache** with epoch-based invalidation
- **Crash-safe segment finalization** with atomic rename
- **Zero-copy reads** with pooled buffer management

### 📦 Installation

```bash
go get github.com/uchebnick/fusedb
```

### 🔧 Quick Start

```go
package main

import (
    "fmt"
    "github.com/uchebnick/fusedb/internal/oneleafdb"
)

func main() {
    // Open database
    db, err := oneleafdb.OpenDB(oneleafdb.DBOptions{
        Dir:            "/tmp/mydb",
        CacheBytes:     5 << 20, // 5MB cache
        ThresholdBytes: 5 << 20, // Merge at 5MB
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Write operations
    db.Put([]byte("user:123"), []byte("alice"))
    db.Inc([]byte("counter:requests"), 1)
    db.Delete([]byte("old:key"))

    // Read operations
    value, found, _ := db.Get([]byte("user:123"))
    if found {
        fmt.Printf("User: %s\n", value)
    }

    // Force merge
    db.Merge()
}
```

### 🏗️ Architecture

FuseDB routes keys into **leaves**. Each leaf owns:
- **In-memory mutation buffer** (lock-free skiplist)
- **Immutable segment on disk** (compressed blocks + bloom filter + index)

**Write path:**
1. Operation enters skiplist buffer (may coalesce with existing ops)
2. WAL append (async group commit)
3. When buffer reaches threshold → merge to new segment

**Read path:**
1. Check skiplist buffer first
2. If not found → check segment (bloom filter → index → block read → decompress)
3. Cache decompressed values

See [ARCHITECTURE.md](./ARCHITECTURE.md) for detailed internals.

### 📊 Benchmarks

Run benchmarks:
```bash
# Throughput
go test ./benchmarks/oneleafdb -bench . -benchmem -benchtime=2s

# Latency probes
FUSEDB_LATENCY_PROBE=1 go test ./benchmarks/oneleafdb -run Latency -v

# YCSB workloads
FUSEDB_REAL_YCSB=1 go test ./benchmarks/oneleafdb -run GoYCSB -v
```

See [benchmarks/RESULTS.md](./benchmarks/RESULTS.md) for full results.

### 🗂️ Project Structure

```
fusedb/
├── internal/
│   ├── oneleafdb/      # Main DB implementation
│   ├── skiplist/       # Lock-free ordered mutation index
│   ├── segment/        # Immutable segment format (blocks, index, bloom)
│   ├── compression/    # LZ4 dictionary compression
│   ├── leaf/           # Leaf buffer and WAL
│   ├── value/          # Value encoding (bytes, int64, tombstones)
│   ├── ops/            # Operation types (Put, Delete, Inc)
│   └── disk/           # Filesystem abstraction
├── benchmarks/         # Performance benchmarks vs Pebble
└── cmd/                # Benchmark servers and tools
```

### 🛠️ Development Status

**Experimental / Research Phase**

Core functionality complete and heavily benchmarked, but not production-ready. Current limitations:
- Single-leaf only (no routing tree)
- No range scans exposed at DB level
- No replication or distributed features

**Roadmap:**
- [ ] Multi-leaf routing tree
- [ ] Persistent routing metadata
- [ ] Range scan API
- [ ] Scheduler policies for merge prioritization
- [ ] Production hardening

### 📄 License

MIT License - see [LICENSE](./LICENSE) for details.

### 🤝 Contributing

This is a research project. Issues and PRs welcome for bug fixes and performance improvements.

---

<a name="russian"></a>

## 🚀 FuseDB - Высокопроизводительная встраиваемая key-value база

FuseDB — экспериментальная встраиваемая key-value база данных, оптимизированная для **небольшого горячего изменяемого состояния** — rate limiter'ов, счётчиков, сессий, квот и метаданных. Построена на **локальных буферах мутаций** и **иммутабельных сегментах** вместо глобальной LSM-компакции.

### ⚡ Производительность

**vs Pebble (режим NoSync):**
- **В 3 раза быстрее чтение**: 1.3µs p50 vs 4µs
- **В 1.4 раза быстрее запись**: 292ns p50 vs 416ns
- **В 117 раз быстрее старт**: 224ms vs 26s (открытие + первое чтение)
- **Нативные атомарные счётчики**: 257ns операция Inc

**Эффективность памяти:**
- 97-126 МБ RSS под нагрузкой (улучшение в 6-8 раз после оптимизации)
- 334 байта/операцию для Get (снижение в 10.5 раз)
- LZ4 словарная компрессия: экономия 62.6% места, декомпрессия 4 ГБ/с

### 🎯 Основные возможности

- **Lock-free skiplist** буфер мутаций с операциями Put, Delete и Inc (атомарные счётчики)
- **LZ4 словарная компрессия** с персистентным реестром и словарями 4KB
- **WAL с асинхронным групповым коммитом** (батчинг каждые 200µs)
- **5MB LRU кэш значений** с инвалидацией по эпохам
- **Crash-safe финализация сегментов** с атомарным переименованием
- **Zero-copy чтение** с пулом буферов

### 📦 Установка

```bash
go get github.com/uchebnick/fusedb
```

### 🔧 Быстрый старт

```go
package main

import (
    "fmt"
    "github.com/uchebnick/fusedb/internal/oneleafdb"
)

func main() {
    // Открыть базу
    db, err := oneleafdb.OpenDB(oneleafdb.DBOptions{
        Dir:            "/tmp/mydb",
        CacheBytes:     5 << 20, // 5MB кэш
        ThresholdBytes: 5 << 20, // Мердж при 5MB
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Операции записи
    db.Put([]byte("user:123"), []byte("alice"))
    db.Inc([]byte("counter:requests"), 1)
    db.Delete([]byte("old:key"))

    // Операции чтения
    value, found, _ := db.Get([]byte("user:123"))
    if found {
        fmt.Printf("User: %s\n", value)
    }

    // Принудительный мердж
    db.Merge()
}
```

### 🏗️ Архитектура

FuseDB маршрутизирует ключи в **листья** (leaves). Каждый лист владеет:
- **In-memory буфером мутаций** (lock-free skiplist)
- **Иммутабельным сегментом на диске** (сжатые блоки + bloom filter + индекс)

**Путь записи:**
1. Операция попадает в skiplist буфер (может слиться с существующими операциями)
2. Запись в WAL (асинхронный групповой коммит)
3. Когда буфер достигает порога → мердж в новый сегмент

**Путь чтения:**
1. Проверка skiplist буфера
2. Если не найдено → проверка сегмента (bloom filter → индекс → чтение блока → декомпрессия)
3. Кэширование декомпрессированных значений

Подробности в [ARCHITECTURE.md](./ARCHITECTURE.md).

### 📊 Бенчмарки

Запуск бенчмарков:
```bash
# Throughput
go test ./benchmarks/oneleafdb -bench . -benchmem -benchtime=2s

# Latency пробы
FUSEDB_LATENCY_PROBE=1 go test ./benchmarks/oneleafdb -run Latency -v

# YCSB workloads
FUSEDB_REAL_YCSB=1 go test ./benchmarks/oneleafdb -run GoYCSB -v
```

Полные результаты в [benchmarks/RESULTS.md](./benchmarks/RESULTS.md).

### 🗂️ Структура проекта

```
fusedb/
├── internal/
│   ├── oneleafdb/      # Основная реализация БД
│   ├── skiplist/       # Lock-free упорядоченный индекс мутаций
│   ├── segment/        # Формат иммутабельного сегмента (блоки, индекс, bloom)
│   ├── compression/    # LZ4 словарная компрессия
│   ├── leaf/           # Буфер листа и WAL
│   ├── value/          # Кодирование значений (bytes, int64, tombstones)
│   ├── ops/            # Типы операций (Put, Delete, Inc)
│   └── disk/           # Абстракция файловой системы
├── benchmarks/         # Бенчмарки производительности vs Pebble
└── cmd/                # Бенчмарк-серверы и утилиты
```

### 🛠️ Статус разработки

**Экспериментальная / Исследовательская фаза**

Основной функционал реализован и протестирован, но не готов для production. Текущие ограничения:
- Только один лист (нет дерева маршрутизации)
- Нет range scan на уровне DB API
- Нет репликации и распределённых возможностей

**Roadmap:**
- [ ] Дерево маршрутизации для нескольких листьев
- [ ] Персистентные метаданные маршрутизации
- [ ] API для range scan
- [ ] Политики планировщика для приоритизации мерджа
- [ ] Production hardening

### 📄 Лицензия

MIT License - см. [LICENSE](./LICENSE).

### 🤝 Участие в разработке

Это исследовательский проект. Issues и PR приветствуются для исправления багов и улучшения производительности.

---

<div align="center">

**Made with ❤️ for high-performance embedded databases**

</div>
