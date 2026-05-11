# FuseDB

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat)](./LICENSE)

**[English](#english)** | **[Русский](#russian)**

---

<a name="english"></a>

## English

Embedded key-value database optimized for small hot mutable state: rate limiters, counters, sessions, quotas. Built around leaf-local mutation buffers and immutable segments instead of global LSM compaction.

### Performance vs Pebble

- Reads: 1.3µs p50 (3x faster)
- Writes: 292ns p50 (1.4x faster)
- Startup: 224ms (117x faster)
- Atomic counters: 257ns Inc operation

### Installation

```bash
go get github.com/uchebnick/fusedb
```

### Usage

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

### Architecture

Each leaf owns:
- In-memory skiplist buffer (lock-free)
- Immutable segment on disk (LZ4 compressed blocks + bloom filter + index)
- Write-ahead log with async group commit

See [ARCHITECTURE.md](./ARCHITECTURE.md) for details.

### Status

Experimental. Core functionality complete and benchmarked. Not production-ready.

---

<a name="russian"></a>

## Русский

Встраиваемая key-value база данных, оптимизированная для небольшого горячего изменяемого состояния: rate limiter'ов, счётчиков, сессий, квот. Построена на локальных буферах мутаций и иммутабельных сегментах вместо глобальной LSM-компакции.

### Производительность vs Pebble

- Чтение: 1.3µs p50 (в 3 раза быстрее)
- Запись: 292ns p50 (в 1.4 раза быстрее)
- Старт: 224ms (в 117 раз быстрее)
- Атомарные счётчики: 257ns операция Inc

### Установка

```bash
go get github.com/uchebnick/fusedb
```

### Использование

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

### Архитектура

Каждый лист владеет:
- In-memory skiplist буфером (lock-free)
- Иммутабельным сегментом на диске (LZ4 сжатые блоки + bloom filter + индекс)
- Write-ahead log с асинхронным групповым коммитом

Подробности в [ARCHITECTURE.md](./ARCHITECTURE.md).

### Статус

Экспериментальная. Основной функционал реализован и протестирован. Не готова для production.
