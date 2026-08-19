# FuseDB documentation

This directory is the maintained documentation index. Start with the path that
matches what you are trying to do.

## Use FuseDB

| Document | Audience | Contents |
|---|---|---|
| [Library guide](../LIBRARY.md) | Application developers | Public API, options, errors, ownership and durability |
| [Likes and ticket counters](./pilot-likes-tickets.md) | Pilot integrators | Idempotent event flow, key schema, SQL/outbox boundary, rebuild and rollback |
| [Краткий runbook](./pilot-likes-tickets.ru.md) | Русскоязычные интеграторы | Сжатая инструкция для пилота |
| [Prometheus integration](../monitoring/prometheus/README.md) | Operators | Collector, readiness endpoint, alerts and dashboard |

## Understand the engine

| Document | Contents |
|---|---|
| [Architecture](../ARCHITECTURE.md) | Implemented storage model and correctness invariants |
| [Архитектура](../ARCHITECTURE.ru.md) | Русская версия основной архитектуры |
| [Scheduler](./scheduler.md) | Load classification, resource budgets, admission and preemption |
| [Compression](./compression.md) | LZ4 dictionary groups, training, evaluation, publication and GC |
| [Disk format](./format.md) | Directory epoch, required features and migration protocol |

## Operate and validate

| Document | Contents |
|---|---|
| [Operations](./operations.md) | Readiness contract, metrics, backup, restore and incidents |
| [Qualification](./qualification.md) | Quiet/steady/spike/recovery testing on deployment hardware |
| [Comparative benchmarks](../benchmarks/README.md) | Fair FuseDB/Pebble/Badger/RocksDB methodology |
| [Recorded results](../benchmarks/RESULTS.md) | Environment-specific benchmark evidence |
| [Release engineering](./release.md) | Compatibility gates, CI artifacts and release checklist |
| [Security policy](../SECURITY.md) | Supported versions and private reporting |

## Contribute

- [Contributing guide](../CONTRIBUTING.md) — prerequisites, focused checks and
  pull-request expectations.
- [Agent invariants](../AGENTS.md) — storage-engine contracts that automated
  contributors must preserve.
- [Repository README](../README.md) — project overview, status and quick start.

Documentation should distinguish implemented behavior from planned behavior.
Performance claims must link to a reproducible configuration and raw report.
