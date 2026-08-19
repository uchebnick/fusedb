# FuseDB

[![CI](https://github.com/uchebnick/fusedb/actions/workflows/ci.yml/badge.svg)](https://github.com/uchebnick/fusedb/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/uchebnick/fusedb/pkg/fusedb.svg)](https://pkg.go.dev/github.com/uchebnick/fusedb/pkg/fusedb)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)

Экспериментальный встраиваемый key-value движок для часто изменяемых данных с
преобладанием чтений. FuseDB делит пространство ключей на независимо
мерджущиеся листья, поэтому объём данных, переписываемый одним merge, остаётся
ограниченным по мере роста базы.

> [!WARNING]
> FuseDB — исследовательский проект. API и дисковый формат пока нестабильны;
> хранить production-данные в нём рано.

[English](./README.md) · [Использование как библиотеки](./LIBRARY.md) ·
[Архитектура](./ARCHITECTURE.ru.md) · [Бенчмарки](./benchmarks/RESULTS.md) ·
[Планировщик](./docs/scheduler.md) · [Эксплуатация](./docs/operations.md) ·
[Qualification](./docs/qualification.md) · [Мониторинг](./monitoring/prometheus/README.md) ·
[Дисковый формат](./docs/format.md) · [Релизы](./docs/release.md) ·
[Пилот лайков/билетов](./docs/pilot-likes-tickets.ru.md) ·
[Detailed English wiki](./docs/pilot-likes-tickets.md) ·
[Участие в разработке](./CONTRIBUTING.md)

## Зачем FuseDB?

Движок с одним сегментом со временем вынужден переписывать весь набор данных на
каждом merge. FuseDB направляет каждый ключ в лист со своим изменяемым буфером и
иммутабельным сегментом. Листья мерджатся и делятся независимо.

- Point-read без глобальной блокировки чтения
- Локальные merge с настраиваемым ограничением размера листа
- Атомарный `int64`-инкремент с точным однократным replay после рестарта
- Условные multi-key write batches и идемпотентная обработка событий одним
  WAL-record для атомарного обновления membership + materialized counter
- WAL recovery, контрольные суммы и атомарная замена manifest
- Bounded decoders и лимиты записи: повреждённые размеры отвергаются до
  аллокации, а порядок одного ключа совпадает в WAL и live state
- Terminal fencing ошибок WAL: failed/uncertain persistence блокирует handle до
  reopen и не допускает небезопасный checkpoint
- Checksummed `FORMAT`-gate с ранним отказом для новых/неизвестных обязательных
  features и атомарной миграцией manifest v2/v3
- Эксклюзивная crash-safe блокировка каталога базы между процессами
- Startup-reconciliation завершённых и временных segment-файлов без ссылок из manifest
- Subprocess crash-матрица с убийством процесса на границах commit WAL,
  segment, manifest и WAL rotation и проверкой точного восстановления счётчика
- Полная `Verify`-проверка manifest, WAL, CRC блоков, диапазонов ключей, bloom
  и ссылок на LZ4-словари
- Онлайн point-in-time backup/restore с потоковыми контрольными суммами и
  полной проверкой восстановленной базы
- Адаптивный допуск merge с защитой foreground p95/p99
- Checksummed UTC-модель недельной нагрузки, переживающая restart и backup
- Адаптивное LZ4-обучение по группам листьев на реальных merged-блоках:
  held-out evaluation, кооперативная отмена и durable publication
- Reference-safe GC старых словарей, ограниченный и прерываемый тем же
  планировщиком, что merge и backup
- Автоопределение доступных CPU/RAM и обучение эффективной пропускной
  способности диска с возможностью явно задать каждый бюджет
- Опциональный низкокардинальный Prometheus collector, readiness-контракт,
  recording rules и примеры production-алертов
- Bounded phased qualification runner с p95/p99 gates, измерением maintenance
  debt, полным Verify, close/reopen и точной проверкой counters
- Иммутабельные сжатые сегменты с bloom filter и блочным индексом
- Безопасное владение байтовыми слайсами: запись не удерживает память
  вызывающего, чтение возвращает копию

Текущий scope намеренно узкий: точечные операции и атомарные write batches над
одной локальной базой. Общих read/write-транзакций, transactional read snapshots,
range scans, репликации и долгосрочной гарантии совместимости форматов пока нет.

## Установка

Нужны Go 1.25.13+, C toolchain и нативная development-библиотека LZ4. Для
Debian/Ubuntu: `apt install liblz4-dev`, для macOS: `brew install lz4`. Сборка
с `CGO_ENABLED=0` компилируется, но dictionary codec и адаптивное обучение
возвращают `fusedb.ErrCGODisabled`.

```bash
go get github.com/uchebnick/fusedb/pkg/fusedb@latest
```

## Быстрый старт

```go
package main

import (
    "log"

    "github.com/uchebnick/fusedb/pkg/fusedb"
)

func main() {
    db, err := fusedb.Open(fusedb.Options{Dir: "./data"})
    if err != nil {
        log.Fatal(err)
    }

    if err := db.Put([]byte("user:42"), []byte("Ada")); err != nil {
        log.Fatal(err)
    }

    value, found, err := db.Get([]byte("user:42"))
    if err != nil {
        log.Fatal(err)
    }
    if found {
        log.Printf("user:42 = %s", value)
    }

    if err := db.Close(); err != nil {
        log.Fatal(err)
    }
}
```

`DB` безопасно использовать конкурентно. `Put`, `Delete` и `Inc` отвергают
пустые ключи; для `Get` пустой ключ считается отсутствующим. Настройки и
гарантии durability описаны в [руководстве по библиотеке](./LIBRARY.md).

## Как это работает

```text
Put / Delete / Inc
        │
        ├── запись в WAL
        ▼
 маршрутизация в лист ───► изменяемый skiplist-буфер
                                  │
                                  │ merge по порогу
                                  ▼
Get ─► лист ─► буфер ─────► иммутабельный сегмент
                              bloom → index → block
```

Упорядоченный набор листьев публикуется по принципу copy-on-write. Лист владеет
полуоткрытым диапазоном ключей, in-memory буфером мутаций и не более чем одним
иммутабельным сегментом. Когда лист перерастает `MaxLeafSize`, следующий merge
создаёт несколько листьев и публикует их атомарно. Инварианты конкурентности и
восстановления разобраны в [описании архитектуры](./ARCHITECTURE.ru.md).

## Режимы durability

| Настройка | Когда завершается запись | Окно потери при сбое |
|---|---|---|
| `WALSyncWrites: false` (по умолчанию) | После append в WAL и применения в памяти | До одного интервала group commit (по умолчанию 200 мкс) |
| `WALSyncWrites: true` | После coalesced group sync WAL и применения в памяти | Подтверждённые WAL-записи намеренно не остаются несинхронизированными |

`Close` и `Merge` переносят буферизованные операции в сегменты. У каждого листа
есть собственный точный replay watermark: локальный merge не может повторно
применить `Inc`. Глобальный `AppliedSeq` остаётся минимумом по листьям и задаёт
безопасную границу truncation WAL.

## Мониторинг

`DB.Health`, `DB.Metrics`, `DB.Stats` и `DB.Format` возвращают иммутабельные
in-process snapshots. Опциональный пакет `pkg/fusedb/prometheus` преобразует их
в custom collector без глобальной регистрации и persistent I/O во время
scrape. Готовый пример `/metrics` и `/readyz`, recording rules и alerts находятся
в [Prometheus operations bundle](./monitoring/prometheus/README.md).

## Снимок производительности

Apple M4, `darwin/arm64`, значения по 128 байт, 64K заранее созданных ключей,
кэш 5 MiB. Pebble работает с `NoSync`, поэтому это сравнение задержки, а не
эквивалентной per-write durability. Указан диапазон трёх прогонов.

| Операция | FuseDB | Pebble (`NoSync`) |
|---|---:|---:|
| Точечное чтение | `1.06–1.13 мкс` | `4.25–4.39 мкс` |
| Запись | `1.03 мкс` | `0.83–0.88 мкс` |
| Открытие + первое чтение | `7.4–7.8 мс` | `26.3–26.6 мс` |
| Атомарный инкремент | `407–482 нс` | — |

Это результат одной машины и одного workload, а не универсальный рейтинг.
Команды, данные и результаты write amplification находятся в
[benchmarks/RESULTS.md](./benchmarks/RESULTS.md).

## Структура репозитория

| Путь | Ответственность |
|---|---|
| `pkg/fusedb` | Поддерживаемый публичный API |
| `pkg/fusedb/prometheus` | Опциональный низкокардинальный Prometheus collector |
| `internal/tree` | Маршрутизация, набор листьев, split и обновление manifest |
| `internal/leaf` | Буфер, reader, merge и split одного листа |
| `internal/skiplist` | Упорядоченный in-memory индекс мутаций |
| `internal/segment` | Формат и чтение/запись иммутабельных сегментов |
| `internal/wal` | WAL, replay, group commit и truncation |
| `internal/manifest` | Каталог листьев, группы словарей и replay watermarks |
| `internal/dbformat` | Epoch дискового формата, feature negotiation и compatibility gate |
| `internal/compression` | LZ4, адаптивное обучение, registry и каталог групп |
| `internal/metrics` | Переиспользуемые метрики latency, фоновых задач, CPU, disk и RAM |
| `internal/scheduler` | Адаптивный admission и прерывание фоновых задач |
| `internal/backup` | Потоковый формат backup и безопасное восстановление |
| `internal/disk` | Абстракция файловой системы и атомарные файловые операции |
| `monitoring` | Prometheus rules, Grafana dashboard, alerts и руководство интеграции |
| `cmd/fusedb-qualify` | Phased qualification целевого железа и JSON evidence |
| `benchmarks` | Отдельный Go module для воспроизводимых сравнений и результатов |

## Состояние проекта

Сейчас FuseDB подходит для исследований storage engine и экспериментов. До
production-релиза нужны долгосрочная политика поддержки форматов, более широкие
power-loss и target-filesystem fault-кампании, регулярные backup/restore drills
и проверка на production-железе. Текущая готовность честно описана в
[руководстве по эксплуатации](./docs/operations.md). Правила разработки — в
[CONTRIBUTING.md](./CONTRIBUTING.md). Поддерживаемые runtime-платформы — Linux
и macOS; точный контракт описан в [release guide](./docs/release.md).
