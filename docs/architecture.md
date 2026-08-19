# FuseDB Architecture

> [!NOTE]
> This is an early design note retained for historical context. The maintained
> description of the implemented engine is [`ARCHITECTURE.md`](../ARCHITECTURE.md).

## Status

FuseDB is an embedded KV engine in active development for small, hot mutable
state. The public API and storage format are still allowed to evolve while the
core engine is being wired together.

## Goals

- Keep point writes cheap.
- Keep point reads predictable.
- Localize merge and split work to leaves.
- Avoid global LSM-style compaction debt.
- Preserve ordered key routing for bounded range reads.
- Make mutation, segment, compression, and disk responsibilities explicit.

## Non-Goals

- Distributed storage.
- SQL or OLTP replacement.
- Large blob storage.
- Scan-heavy analytics.
- Stable public API at this stage.

## High-Level Model

FuseDB is tree-routed. The routing tree maps key ranges to leaves. A leaf is the unit of mutation, materialization, merge, and future split.

Each leaf is expected to own:

- an in-memory mutation buffer;
- an immutable on-disk segment;
- leaf-local metadata such as seqno and merge state;
- scheduler-visible thresholds for merge/split decisions.

## Buffer Model

The buffer stores typed operations, not fully materialized values.

Supported operation kinds:

- `Put`;
- `Delete` as a tombstone;
- `Inc` for counters.

The current in-memory ordered structure is `internal/skiplist`.

Important contracts:

- `Op.Data` is immutable after the op is handed to the skiplist.
- Callers should use constructors such as `NewPut` and `NewInc` when they need owned data.
- Deletes are stored as operations and are not physical removals from the active skiplist.
- Existing-key updates publish a new `Op` through `atomic.Pointer[Op]`.
- New nodes are linked into the skiplist with CAS on `next` pointers.
- Node height is deterministic from `xxhash(key) ^ seed`, avoiding shared RNG contention.

Coalescing is local to the buffer. For example, two `Inc` operations for the same key may be merged into one buffered `Inc`.

## Segment Model

A segment is an immutable, leaf-local materialized snapshot.

Segments store resolved key/value entries, not pending buffer operations. The segment module owns:

- block encoding;
- block index;
- segment header/footer;
- bloom filter;
- reader/writer behavior;
- compression metadata at the segment level.

The disk module owns filesystem primitives and atomic file operations. Segment code may use disk primitives, but compression code should not own file layout.

## Read Path

Point read order:

1. Check the leaf buffer.
2. If buffer has `Delete`, return not found.
3. If buffer has `Put`, return that value.
4. If buffer has `Inc`, read the segment base value and apply the increment.
5. If buffer has no key, read from the segment.

This keeps recent mutations visible while allowing the segment to remain immutable.

## Write Path

Write path target:

1. Convert the request into an `Op`.
2. Append to WAL before visibility once WAL exists.
3. Apply the op to the leaf buffer.
4. Coalesce with an existing buffered op when safe.

Current skiplist code is internal and assumes operation ownership rules are respected.

## Merge Path

The scheduler eventually merges a leaf buffer with the leaf segment:

1. Read ordered buffered operations.
2. Read ordered segment entries.
3. Resolve operations into materialized values.
4. Build a new immutable segment.
5. Atomically replace the old segment.
6. Clear merged buffer/WAL state.

Deletes remain meaningful until merge/compaction proves they can be dropped.

## Split Direction

Leaf split should happen at materialization time:

1. Merge buffer and segment into ordered materialized state.
2. Split the resulting key range into two leaves.
3. Write one segment per new leaf.
4. Publish routing-tree changes atomically.

The buffer itself should not be split as raw mutable state.

## Scheduler Direction

The scheduler should avoid coordinated IO spikes:

- use per-leaf threshold variance;
- defer merges under IO pressure;
- allow leaves to temporarily exceed nominal thresholds;
- decide split at merge time based on materialized size and IO budget.

## Correctness Invariants

- Published skiplist node links are changed only through atomic pointer operations.
- Active skiplist nodes are not physically removed.
- `Op` values published through atomic pointers are immutable.
- Segment files are immutable after `Freeze`.
- Segment replacement must be atomic from the perspective of recovery metadata.
- Compression dictionaries are versioned and immutable once used by a segment.
