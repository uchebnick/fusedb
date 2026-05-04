// Package skiplist implements FuseDB's in-memory ordered mutation buffer.
//
// This package is intentionally not a general-purpose ordered map. It is tuned
// for memtable-like storage of string keys and typed mutation operations.
//
// Operation ownership contract:
//   - Op values published into a SkipList are immutable.
//   - Op.Data must not be mutated after the Op is passed to the SkipList.
//   - Constructors such as NewPut and NewInc create owned Op.Data buffers.
//
// Read contract:
//   - Read and Iter are zero-copy view APIs.
//   - Callers of Read and Iter must not mutate returned Op.Data.
//   - SafeRead and SafeIter copy Op.Data before returning it.
//
// Concurrency contract:
//   - Active nodes are not physically removed.
//   - Deletes are represented as tombstone operations.
//   - Node links are published with atomic pointer CAS.
//   - Existing-key updates publish a new Op through atomic.Pointer[Op].
package skiplist
