// Package skiplist implements FuseDB's in-memory ordered mutation buffer.
//
// This package is intentionally not a general-purpose ordered map. It is tuned
// for memtable-like storage of byte keys and typed mutation operations.
//
// Operation ownership contract:
//   - Op values published into a SkipList are immutable.
//   - Op.Data must not be mutated after the Op is passed to the SkipList.
//   - Constructors such as NewPut and NewInc create owned Op.Data buffers.
//
// Read contract:
//   - Read is a zero-copy view API for Op.Data.
//   - Iter returns detached key copies and zero-copy Op.Data views.
//   - Callers of Read and Iter must not mutate returned Op.Data.
//   - SafeRead and SafeIter copy Op.Data before returning it.
//
// Concurrency contract:
//   - Active nodes are not physically removed.
//   - Deletes are represented as tombstone operations.
//   - Node links are published with atomic pointer CAS.
//   - Existing-key updates publish a new Op through atomic.Pointer[Op].
package skiplist
