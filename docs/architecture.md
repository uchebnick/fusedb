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
A leaf is the primary unit of mutation, storage, maintenance, and split in FuseDB.
Each leaf owns:

- a mutable in-memory buffer
- at most one immutable leaf-local segment on disk
- a per-leaf seqno counter
> [!NOTE]
> The buffer and segment serve different roles.
> 
> The buffer is an operation log — it records mutations as typed operations (puts, deletes, increments, accumulators) rather than as materialized values.
> 
> The segment is a materialized state snapshot — it stores the resolved value for each key as of the last buffer-to-segment merge.

---

### 7.1 Buffer
The buffer is a sorted map from key to the latest known operation for that key. Sorting preserves key order within the leaf and supports bounded range scans.
Each accepted mutation increments the per-leaf seqno. For mergeable operation types (counters, quotas, accumulators), the buffer coalesces multiple updates into a single per-key entry. Deletes are represented as tombstones.

---

### 7.2 Segment
A segment is an immutable, leaf-local materialized snapshot. Each leaf holds at most one active segment at any time. Segment contents are ordered by key and represent fully resolved values — no pending operations remain in the segment.

---

### 7.3 Read Path
A point read on a leaf resolves the key by consulting the buffer first, then the segment.
The buffer stores mutations as operations, not as materialized values. The segment stores materialized state. Read resolution follows the operation type found in the buffer:

Tombstone (Delete) - return not found immediately, without consulting the segment.
Put — return the value from the buffer entry directly.
Mergeable operation (e.g. increment, accumulator) — the buffer entry cannot be resolved alone. Fall through to the segment to read the base value, then apply the buffered operation on top to produce the final result.
No buffer entry — fall through to the segment and return the segment value, or not found if absent.

---

### 7.4 Write Path
A write is accepted by the leaf as a typed operation: Put, Delete, or a mergeable operation type.
Before the buffer is updated, the operation is appended to the WAL. Only after the WAL write is confirmed is the buffer entry updated. This ordering ensures the mutation is durable before it becomes visible within the leaf.

The buffer entry for the key is replaced with the incoming operation. For mergeable operation types, the incoming operation is coalesced with the existing buffer entry if one is present, producing a single updated entry rather than accumulating a list of individual records.
The per-leaf seqno is incremented on each accepted mutation.

---

### 7.5 Merge path
Buffer-to-segment merge is triggered by the scheduler. The merge reads the current buffer state and produces a new immutable segment. The new segment replaces the previous segment atomically. After a successful merge, the buffer is cleared and WAL records up to this merge point may be truncated.
---

### 7.6 Split-merge path
When the scheduler determines that a merge would produce a segment exceeding a configured size threshold, it performs a split-merge instead of a regular merge.
Split-merge proceeds as follows:

1. Two new leaves are created, each assigned a sub-range of the current leaf's key range.
2. The current leaf becomes immutable — it stops accepting new mutations. Incoming writes for its key range are routed to the new leaves by the routing tree.
3. The current leaf's buffer and segment are merged together and divided by key range into two new segments, one per new leaf.
4. The routing tree is updated atomically to replace the old leaf with the two new leaves.
5. The old leaf is deleted.

This means the buffer is never split directly — it is first merged with the segment, and the resulting materialized state is then divided by key range into the new segments.

--- 
## 8. Scheduler
The scheduler is responsible for triggering merge and split-merge operations across leaves. Its primary goal is to keep foreground write latency predictable by avoiding coordinated IO spikes.

---

### 8.1 Merge triggering
Each leaf tracks its buffer size. When the buffer size exceeds a configured threshold, the leaf becomes a candidate for merge.
The scheduler does not use a fixed global threshold applied uniformly to all leaves. A fixed threshold would cause multiple leaves to become merge candidates simultaneously, creating coordinated IO pressure. Instead, the scheduler introduces per-leaf threshold variance at startup, spreading merge triggers across time.

---

### 8.2 IO-aware scheduling
The scheduler monitors current IO load. When IO pressure is low, pending merges are dispatched promptly. When IO pressure is high, the scheduler may defer merge dispatch and allow candidate buffers to grow beyond their nominal threshold before triggering.
This means the effective merge threshold is not fixed — it adapts to available IO capacity. A leaf under high IO pressure may accumulate a larger buffer before its merge is scheduled.

---

### 8.3 Split decision
The split decision is made at merge time. After the buffer and segment are merged, the scheduler evaluates the size of the resulting materialized state. If it exceeds the split threshold and IO capacity is available, a split-merge is performed instead of a regular merge. If IO pressure is too high, the merge completes without a split and the leaf is allowed to grow until conditions improve.
