# Skiplist Module

`internal/skiplist` is FuseDB's in-memory ordered mutation buffer.

It is not a general-purpose ordered map. The module is specialized for database
memtable-style usage:

- keys are `string`
- values are typed mutation operations (`Op`)
- deletes are tombstones
- active nodes are not physically removed
- existing-key updates publish a new immutable `Op`
- new nodes are linked with atomic pointer CAS

## Role In FuseDB

The skiplist stores recent mutations before they are merged into an immutable
segment.

Higher layers should use it as an ordered operation index:

1. writes become `Put`, `Delete`, or `Inc` operations
2. operations are published into the skiplist
3. point reads check the skiplist before segment lookup
4. merge code iterates the skiplist in key order and resolves operations with
   segment entries

The skiplist module does not own WAL, leaf routing, segment IO, or recovery.

## Public API

Convenience API:

```go
list.Put(key, value)
list.Delete(key)
list.Inc(key, delta)
value, ok := list.Get(key)
```

Operation-aware API:

```go
list.Apply(key, op)
op, ok := list.Read(key)
op, ok := list.SafeRead(key)
for key, op := range list.Iter() {}
for key, op := range list.SafeIter() {}
```

Use `Put`, `Delete`, and `Inc` when the caller does not need to construct `Op`
directly.

Use `Apply`, `Read`, and `Iter` for lower-level internal code such as buffer and
merge logic.

## Operation Model

`Op` is the internal mutation currency.

Supported operations:

- `OpPut`: replaces the materialized value for a key
- `OpDelete`: marks the key as deleted
- `OpInc`: adds a signed integer delta to a counter

`Inc + Inc` for the same key is coalesced into one increment operation.

`Put` and `Delete` overwrite older buffered operations for the key.

## Ownership Contract

`Op.Data` is immutable after publication.

Rules:

- after `Apply(key, op)`, the caller must not mutate `op.Data`
- `NewPut` copies the input value
- `NewInc` creates an owned varint buffer
- `Read` and `Iter` return zero-copy views
- callers must not mutate `Op.Data` returned by `Read` or `Iter`
- `SafeRead`, `SafeIter`, and `Get` return owned copies

This split keeps hot internal paths fast while preserving safe APIs for callers
that need ownership.

## Concurrency Model

The skiplist is designed for concurrent internal use under the module contract.

Important invariants:

- node links are `atomic.Pointer[Node]`
- existing-key values are `atomic.Pointer[Op]`
- level 0 publishes node existence
- upper levels are acceleration links
- nodes are not physically removed
- delete is a tombstone operation
- `Len` counts unique published nodes, not live materialized values

Insert flow:

1. find predecessor/successor splice
2. if the key exists, publish a coalesced `Op`
3. otherwise create a node
4. link level 0 with CAS
5. link upper levels with CAS retries
6. grow global height with CAS

Because nodes are not removed, readers and iterators can safely follow published
links without dealing with physical deletion.

## Height Selection

Node height is deterministic:

```text
height = f(xxhash(key) ^ seed)
```

The probability model is `p = 1/4`: every two zero low bits raise the node by
one level.

This avoids shared random-number state and keeps inserts free from RNG locks or
atomic counters.

## Iteration

`Iter` walks level 0 and yields all published nodes in key order.

This is the intended path for future buffer-to-segment merge work. It is much
faster than reading many keys individually because it avoids repeated skiplist
searches.

`SafeIter` performs the same ordered scan but copies every yielded operation.

## Benchmarks

Benchmarks were run on Apple M4 with:

```text
GOCACHE=/private/tmp/fusedb-gocache go test ./internal/skiplist \
  -run '^$' \
  -bench 'Benchmark(ReadHit|ReadMiss|ReadBatch|Iter|SafeIter|Apply)' \
  -benchmem \
  -count=3
```

Representative results:

| Benchmark | Time | Allocations |
| --- | ---: | ---: |
| `ReadHit1K` | `45-47 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadMiss1K` | `27 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHit64K` | `93-96 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadMiss64K` | `101-107 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitRotating64K` | `149-150 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00000000` | `19 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00000128` | `32-33 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00001024` | `41 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00008192` | `34-35 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00016384` | `90 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00032768` | `95-96 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00049152` | `70-71 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadHitPositions64K/key:00065535` | `110-112 ns/op` | `0 B/op, 0 allocs/op` |
| `ReadBatch10Keys64K` | `1.28-1.30 us/op` | `0 B/op, 0 allocs/op` |
| `ReadBatch100Keys64K` | `15.6-15.9 us/op` | `0 B/op, 0 allocs/op` |
| `IterFull1K` | `1.79-1.80 us/op` | `0 B/op, 0 allocs/op` |
| `IterFull64K` | `156-160 us/op` | `0 B/op, 0 allocs/op` |
| `IterFirst100Of64K` | `177-178 ns/op` | `0 B/op, 0 allocs/op` |
| `SafeIterFull1K` | `7.9 us/op` | `5461 B/op, 1024 allocs/op` |
| `SafeIterFull64K` | `554-556 us/op` | `349527 B/op, 65536 allocs/op` |
| `SafeIterFirst100Of64K` | `813-821 ns/op` | `533 B/op, 100 allocs/op` |
| `ApplyInsertSequential` | `236-248 ns/op` | `122 B/op, 5 allocs/op` |
| `ApplyPutExisting` | `27-28 ns/op` | `40 B/op, 2 allocs/op` |
| `ApplyIncExisting` | `43-45 ns/op` | `39 B/op, 3 allocs/op` |
| `ApplyIncExistingParallel` | `231-234 ns/op` | `238-242 B/op, 14 allocs/op` |

Notes:

- `Read` and `Iter` are zero-copy view APIs, so they do not allocate.
- `SafeIter` allocates once per yielded entry because it copies `Op.Data`.
- Full iteration over 64K entries is roughly `2.4 ns/entry`.
- point-read hit cost depends on target-key position and tower distribution.
- `ReadHitRotating64K` rotates across 1024 keys and is a better average-hit
  estimate than one fixed key.
- Merge/range paths should prefer `Iter` over repeated point `Read` calls.

## Testing

Run:

```text
go test ./internal/skiplist
go test -race ./internal/skiplist
```

The race tests cover:

- concurrent inserts on different keys
- mixed `Put`, `Inc`, `Delete`, and `SafeRead`
- concurrent reads while incrementing one hot key
- iteration while writers publish new nodes
- safe API ownership behavior
