# FuseDB

[![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat)](./LICENSE)

Встраиваемая key-value база данных для горячего изменяемого состояния.

[English](./README.md)

## Производительность

Сравнение с Pebble (режим NoSync):

- Чтение: 1.0µs vs 4.1µs
- Запись: 1.1µs vs 1.1µs
- Старт: 224ms vs 26s (открытие + первое чтение)
- Атомарные счётчики: 257ns операция Inc

## Установка

```bash
go get github.com/uchebnick/fusedb
```

## Использование

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

## Архитектура

- Lock-free skiplist буфер
- Иммутабельные сегменты с LZ4 компрессией
- Асинхронный WAL с групповым коммитом

Подробности: [ARCHITECTURE.md](./ARCHITECTURE.md)

## Статус

Экспериментальная. Не готова для production.
