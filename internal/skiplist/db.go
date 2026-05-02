package skiplist

import "iter"

// Apply publishes op for key.
//
// The caller must treat op.Data as immutable after this call. Prefer Put,
// Delete, and Inc when the caller does not need to construct Op directly.
func (s *SkipList) Apply(key string, op Op) {
	s.apply(key, op)
}

// Put publishes a value replacement for key.
//
// Put copies value before publishing it into the skiplist.
func (s *SkipList) Put(key string, value []byte) {
	s.apply(key, NewPut(value))
}

// Delete publishes a delete tombstone for key.
func (s *SkipList) Delete(key string) {
	s.apply(key, NewDelete())
}

// Inc publishes a counter increment for key.
func (s *SkipList) Inc(key string, delta int64) {
	s.apply(key, NewInc(delta))
}

// Read returns the current Op for key without copying Op.Data.
//
// The returned Op is a view of immutable skiplist-owned data. Callers must not
// mutate returned Op.Data. Use SafeRead when the caller needs an owned copy.
func (s *SkipList) Read(key string) (Op, bool) {
	return s.read(key)
}

// SafeRead returns the current Op for key with an owned copy of Op.Data.
func (s *SkipList) SafeRead(key string) (Op, bool) {
	return s.safeRead(key)
}

// Get returns the current put value for key with an owned copy.
//
// Get returns false for missing keys, delete tombstones, and increment
// operations. Higher layers that need operation-aware reads should use Read or
// SafeRead.
func (s *SkipList) Get(key string) ([]byte, bool) {
	op, ok := s.safeRead(key)
	if !ok || op.Kind != OpPut {
		return nil, false
	}

	return op.Data, true
}

// Iter returns a zero-copy ordered iterator over all live skiplist nodes.
//
// Deletes are yielded as OpDelete tombstones. Returned Op.Data shares storage
// with the skiplist and must not be mutated by the caller. Use SafeIter when
// the caller needs owned Op.Data buffers.
func (s *SkipList) Iter() iter.Seq2[string, Op] {
	return s.iter()
}

// SafeIter returns an ordered iterator that copies Op.Data for every yielded Op.
func (s *SkipList) SafeIter() iter.Seq2[string, Op] {
	return s.safeIter()
}
