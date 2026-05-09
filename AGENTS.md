# FuseDB Agent Notes

## Project State

FuseDB is a research-stage embedded KV engine. APIs, storage formats, and module boundaries may change while the core design is being built.

## Working Rules

- Prefer small, focused changes.
- Do not rewrite unrelated modules while working on one component.
- Keep correctness ahead of micro-optimizations.
- Add tests for behavior before relying on benchmarks.
- Do not mutate caller-owned byte slices after publishing them into shared structures unless the API explicitly transfers ownership.

## Current Design Contracts

- `internal/skiplist` is an in-memory ordered mutation buffer.
- `Op.Data` is treated as immutable after an `Op` is handed to the skiplist.
- Constructors such as `NewPut` and `NewInc` create owned operation data.
- Delete is stored as an operation/tombstone, not as physical removal from active structures.
- Skiplist nodes are not physically deleted from the active skiplist.
- Node links use CAS through `atomic.Pointer`.
- Existing-node operation updates use `atomic.Pointer[Op]`.
- New skiplist node height is derived from `xxhash(key) ^ seed`, not from shared mutable RNG state.

## Module Boundaries

- `internal/skiplist` owns ordered in-memory operation indexing.
- `internal/buffer` should own higher-level buffering/sharding decisions.
- `internal/segment` owns immutable block/segment format and segment readers/writers.
- `internal/compression` owns compression, dictionary training, and dictionary registries.
- `internal/disk` owns filesystem abstractions and atomic file operations.

## Testing Notes

- For skiplist changes, run `go test ./internal/skiplist`.
- For concurrency-sensitive skiplist changes, also run `go test -race ./internal/skiplist`.
- For segment changes, run `go test ./internal/segment`.
- Full `go test ./...` may fail while unfinished modules are under active development.
