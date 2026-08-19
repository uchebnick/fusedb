# FuseDB Agent Notes

## Project State

FuseDB is a research-stage embedded KV engine. APIs, storage formats, and module boundaries may change while the core design is being built.

The engine partitions the keyspace into leaves. Each leaf owns a mutable buffer and one immutable segment, and merges independently, so the volume a merge rewrites is bounded by the leaf size rather than by the size of the database.

## Working Rules

- Prefer small, focused changes.
- Do not rewrite unrelated modules while working on one component.
- Keep correctness ahead of micro-optimizations.
- Add tests for behavior before relying on benchmarks.
- Do not mutate caller-owned byte slices after publishing them into shared structures unless the API explicitly transfers ownership.
- Never return internal buffer or block memory to an external caller. `DB.Get` copies before returning; a caller writing into a returned slice must not be able to corrupt the database.
- All filesystem access goes through `internal/disk.FS`. Reaching for `os` directly breaks the in-memory filesystem that tests run on.

## Current Design Contracts

- `internal/skiplist` is an in-memory ordered mutation buffer.
- `Op.Data` is treated as immutable after an `Op` is handed to the skiplist.
- Constructors such as `NewPut` and `NewInc` create owned operation data. `NewPutOwned` is the opposite: it takes ownership and must only receive a freshly allocated buffer.
- Delete is stored as an operation/tombstone, not as physical removal from active structures.
- Skiplist nodes are not physically deleted from the active skiplist.
- Node links use CAS through `atomic.Pointer`.
- Existing-node operation updates use `atomic.Pointer[Op]`.
- New skiplist node height is derived from `xxhash(key) ^ seed`, not from shared mutable RNG state.
- A leaf range is half-open and only its lower bound is stored: a leaf covers `[LowKey, next leaf's LowKey)`, and the leftmost leaf has an empty `LowKey` so the leaves cover the keyspace with no hole.
- The leaf set is copy-on-write under an `atomic.Pointer`. Reads and ordinary writes take no lock; only structural changes serialize.
- A write rejected by a leaf (`TryPut` and friends returning false) means the leaf was detached by a split and the caller must retry against the refreshed tree. Retry is safe precisely because a rejected operation was never applied.
- During a split, pending operations must be routed into the new leaves *before* those leaves become visible, both under the leaf write lock. Publishing first would let a fresh write be overwritten by an older replayed operation.
- `AppliedSeq` in the manifest is exact, not conservative: every log record at or below it is in a segment and nothing above it is. An inexact watermark replays an increment that a segment already contains and silently doubles it.
- Segment ids are allocated from the manifest and never reused while a file for that id and version exists.

## Module Boundaries

- `internal/skiplist` owns ordered in-memory operation indexing.
- `internal/leaf` owns one leaf: its buffer, its segment reader, and the merge that rewrites them.
- `internal/tree` owns the ordered set of leaves, key routing, splits, and manifest updates.
- `internal/manifest` owns the persistent catalog: leaf ranges, segment ids, and the log watermark.
- `internal/wal` owns the write-ahead log format, group commit, replay, and truncation.
- `internal/segment` owns immutable block/segment format and segment readers/writers.
- `internal/compression` owns compression, dictionary training, and dictionary registries.
- `internal/disk` owns filesystem abstractions and atomic file operations.
- `pkg/fusedb` is the public API. `internal/engine` is the engine behind it and is not the supported entry point.

## Testing Notes

- For skiplist changes, run `go test ./internal/skiplist`.
- For concurrency-sensitive changes anywhere in the write path, run `go test -race ./internal/skiplist ./internal/tree ./pkg/fusedb`.
- For segment changes, run `go test ./internal/segment`.
- Durability behaviour lives in `pkg/fusedb/durability_test.go`: reopening, crash replay, splits, and counter exactness across restarts. Changes to the merge, manifest, or log paths should be checked there.
- Full `go test ./...` may fail while unfinished modules are under active development.
