# FuseDB Architecture

## 1. Purpose

This document describes the high-level architecture of FuseDB.

FuseDB is an embedded KV engine for small hot mutable state, optimized for predictable tail latency on update-heavy workloads. The document defines the engine's target workload, leaf model, buffer organization, materialization path, split model, durability model, recovery model, and main correctness invariants.

> [!IMPORTANT]
> This document describes the current architectural model of FuseDB. It is a design document, not a stable storage-format or API specification.

---

## 2. Goals

FuseDB is designed with the following goals:

- provide an embedded single-node KV engine
- optimize for small hot mutable state
- provide predictable tail latency on update-heavy workloads
- keep foreground writes cheap
- keep point reads fast for hot keys
- localize maintenance work at the leaf level
- reduce global write amplification compared to LSM-style compaction
- preserve ordered key-space routing through a tree-based structure
- support durability and crash recovery through WAL-based persistence
- make trade-offs explicit rather than pretending to be universally optimal

---

## 3. Non-goals

FuseDB is explicitly not designed to be:

- a distributed database
- a full SQL engine
- a general-purpose OLTP replacement
- a large-value/blob store
- an analytics-oriented scan engine
- universally better than B+ tree or LSM-based engines on all workloads
- a fully transactional multi-key storage engine in its initial versions

---
## 4. Workload Model

FuseDB targets workloads with the following characteristics:

- small keys and values
- frequent point reads
- frequent point updates
- overwrite-heavy mutation patterns
- counters, increments, quotas, sessions, rate limits, and similar serving-state use cases
- hot keys and hot ranges
- bounded or secondary range scans

FuseDB prioritizes predictable p99 behavior on these workloads over universal workload coverage.

FuseDB is not optimized for:

- long analytical scans
- large blobs
- scan-dominated workloads
- large-value storage
- general-purpose SQL-style query workloads

---

## 5. Architectural Overview

FuseDB should be thought of as a tree-routed engine with leaf-local mutable history and leaf-local immutable storage, rather than as a classic B+ tree or a classic LSM tree.

At a high level, FuseDB consists of:

- a tree-based key-range index that routes reads and writes to leaves
- a leaf as the primary unit of mutation, maintenance, and split
- each leaf consisting of a mutable in-memory buffer and an immutable leaf-local segment state
- in-buffer update merge for mergeable mutations
- buffer-to-segment merge for leaf-local materialization
- a WAL for durability
- a scheduler responsible for materialization, merge, and split decisions
- a recovery path that reconstructs in-memory state from persisted metadata and WAL

The routing tree maps keys to leaves and supports ordered leaf traversal for range scans.

---
## 6. Data Model

FuseDB stores opaque byte keys and opaque byte values.

The primary supported operations are:

- `Get(key)`
- `Put(key, value)`
- `Delete(key)`
- optional merge/update-style operations for counters or accumulator-like state

Keys are globally ordered. Key ordering is used by the routing tree and by range scans.

Each mutation accepted by a leaf is assigned a per-leaf seqno.

Deletes are represented as tombstones.

Mergeable update types may be represented in buffered form as coalesced per-key state rather than as a raw list of individual in-memory records.

---

## 7. Leaf Model
