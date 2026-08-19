# Changelog

All notable changes to FuseDB are documented here. The project follows
[Semantic Versioning](https://semver.org/) while public APIs and the on-disk
format remain pre-1.0.

## [0.1.0] - 2026-08-20

First versioned release.

### Added

- Concurrent embedded key-value API with `Get`, `Put`, `Delete`, atomic
  counters, conditional batches, and idempotent event application.
- Checksummed WAL, exact per-leaf replay watermarks, crash recovery, atomic
  manifests, point-in-time backup/restore, and full integrity verification.
- Independently merged and split key ranges with adaptive, resource-aware
  scheduling designed to preserve foreground p95/p99 latency.
- Per-group LZ4 dictionary sampling, cooperative training, evaluation,
  immutable registry, publication, and lifecycle garbage collection.
- Prometheus metrics, Grafana dashboards, target-hardware qualification, and
  Pebble, BadgerDB, and RocksDB benchmark adapters.

### Reliability

- Deterministic short-write, ENOSPC, fsync, rename, commit-uncertain, WAL
  rotation, manifest, segment, and process-crash test matrices.
- Race-tested reader retirement, split handover, merge retry, close admission,
  caller-buffer ownership, and WAL group commit.

### Compatibility

- Go 1.25.13 or a newer patched Go 1.25 toolchain.
- Linux and macOS on amd64 and arm64.
- Native LZ4 and CGO are required for dictionary compression; raw storage
  remains available without CGO.
- On-disk upgrades before 1.0 must be validated as data migrations on a restored
  copy. FuseDB should not yet be the only copy of business-critical data.

[0.1.0]: https://github.com/uchebnick/fusedb/releases/tag/v0.1.0
