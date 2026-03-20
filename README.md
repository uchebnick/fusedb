# FuseDB
[![Language: Go](https://img.shields.io/badge/language-Go-00ADD8)](#)
[![Status: Research](https://img.shields.io/badge/status-research-orange)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](./LICENSE)
## What it is

FuseDB is an embedded KV engine for small hot mutable state, optimized for predictable tail latency on update-heavy workloads.

It is designed for workloads such as rate limiting, counters, sessions, quotas, and other small metadata/state-serving use cases where point reads and point updates dominate.

> [!IMPORTANT]
> FuseDB is currently in the research stage. The architecture, APIs, and on-disk format are expected to change.

## Why this exists

### Motivation

Existing storage engine architectures make different trade-offs:

#### LSM trees
- often provide strong write throughput
- can suffer from latency instability under compaction pressure
- may rewrite data multiple times across levels
- can accumulate compaction debt that affects tail latency

#### B+ trees
- provide strong ordered access and efficient range traversal
- may suffer from checkpoint and dirty-page pressure under heavy update workloads
- can experience cache churn and contention on hot paths

FuseDB explores a different design: leaf-local immutable segments with local merge, aiming to reduce global write amplification and improve p99 predictability for small hot-state workloads.

## Target workloads

### Optimized for
- small keys and values
- point reads
- point updates
- overwrite-heavy workloads
- counters and increments
- hot keys and hot ranges
- bounded or secondary range scans

### Not optimized for
- large blobs
- analytical scans
- general-purpose OLTP replacement
- distributed workloads

> [!WARNING]
> FuseDB is not intended to be a general-purpose replacement for RocksDB, B+ tree engines, or full OLTP databases.

## Core ideas

- tree-based key-range routing
- mutable in-memory buffer per leaf
- immutable leaf-local segments
- local merge within a leaf instead of global LSM-style compaction
- leaf split on overflow or hot-range pressure
- update/merge semantics in the in-memory buffer
- per-leaf seqno for visibility and merge ordering

## Current status

- research stage
- architecture and workload model are being refined
- not production-ready

## Roadmap

- [ ] Core engine
  - [ ] in-memory buffer with update merge
  - [ ] immutable leaf segment
  - [ ] local merge between buffer and segment
  - [ ] basic scheduler
- [ ] Leaf split
- [ ] WAL and recovery
- [ ] First benchmarks
- [ ] Tuning, merge policy, and scheduler optimization
