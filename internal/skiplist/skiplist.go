package skiplist

import (
	"iter"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

const maxHeight = 20

// SkipList is an ordered in-memory mutation index for string keys.
//
// It stores Op values, keeps deletes as tombstones, and never physically
// removes active nodes. The structure is intended for internal buffer/memtable
// use, not as a general-purpose ordered map.
type SkipList struct {
	head      *node
	height    atomic.Int32
	nodeCount atomic.Int64
	dataBytes atomic.Int64
	seed      uint64
}

// node is one published skiplist node.
type node struct {
	key  string
	op   atomic.Pointer[Op]
	next []atomic.Pointer[node]
}

// NewSkipList creates an empty SkipList.
//
// The seed is mixed with each key hash to choose deterministic node heights.
// Passing zero uses the package default seed.
func NewSkipList(seed uint64) *SkipList {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}

	skiplist := &SkipList{
		head: &node{
			next: make([]atomic.Pointer[node], maxHeight),
		},
		seed: seed,
	}
	skiplist.height.Store(1)

	return skiplist
}

// Len returns the number of unique keys currently represented by nodes.
//
// Deletes are tombstones and do not decrease Len.
func (s *SkipList) Len() int64 {
	return s.nodeCount.Load()
}

// DataBytes returns the total byte length of Op.Data payloads currently stored.
//
// Deletes contribute zero data bytes. Updates adjust the counter after the new
// operation is successfully published.
func (s *SkipList) DataBytes() int64 {
	return s.dataBytes.Load()
}

// Apply publishes op for key.
//
// The caller must treat op.Data as immutable after this call. If key already
// exists, Apply publishes a new coalesced Op without relinking the node. If key
// does not exist, Apply inserts a new node and links level 0 before publishing
// upper levels.
func (s *SkipList) Apply(key string, op Op) {
	var prevList, nextList [maxHeight]*node

	for {
		s.findSplice(key, &prevList, &nextList)

		if next := nextList[0]; next != nil && next.key == key {
			s.updateNode(next, op)
			return
		}

		nodeHeight := s.randomHeight(key)
		oldHeight := s.height.Load()

		s.prepareNewLevels(oldHeight, nodeHeight, &prevList, &nextList)

		newNode := newNode(key, op, nodeHeight)
		if !s.publishBaseLevel(newNode, &prevList, &nextList) {
			continue
		}

		s.publishUpperLevels(key, newNode, nodeHeight, &prevList, &nextList)
		s.growHeight(oldHeight, nodeHeight)
		return
	}
}

// Read returns the current Op for key without copying Op.Data.
//
// The returned Op is a view of immutable skiplist-owned data. Callers must not
// mutate returned Op.Data. Use SafeRead when the caller needs an owned copy.
func (s *SkipList) Read(key string) (Op, bool) {
	_, next := s.findSpliceAtLevel(key, 0)

	if next == nil || next.key != key {
		return Op{}, false
	}

	op := next.op.Load()
	if op == nil {
		return Op{}, false
	}

	return *op, true
}

// SafeRead returns the current Op for key with an owned copy of Op.Data.
func (s *SkipList) SafeRead(key string) (Op, bool) {
	op, ok := s.Read(key)
	if !ok {
		return Op{}, false
	}

	return *op.copy(), true
}

// Iter returns a zero-copy ordered iterator over all live skiplist nodes.
//
// Deletes are yielded as OpDelete tombstones. Returned Op.Data shares storage
// with the skiplist and must not be mutated by the caller. Use SafeIter when
// the caller needs owned Op.Data buffers.
func (s *SkipList) Iter() iter.Seq2[string, Op] {
	return func(yield func(string, Op) bool) {
		for x := s.head.next[0].Load(); x != nil; x = x.next[0].Load() {
			op := x.op.Load()
			if op == nil {
				continue
			}

			if !yield(x.key, *op) {
				return
			}
		}
	}
}

// SafeIter returns an ordered iterator that copies Op.Data for every yielded Op.
func (s *SkipList) SafeIter() iter.Seq2[string, Op] {
	return func(yield func(string, Op) bool) {
		for key, op := range s.Iter() {
			if !yield(key, *op.copy()) {
				return
			}
		}
	}
}

func newNode(key string, op Op, height int32) *node {
	n := &node{
		key:  key,
		next: make([]atomic.Pointer[node], height),
	}
	n.op.Store(&op)

	return n
}

func (s *SkipList) randomHeight(key string) int32 {
	var h int32 = 1
	hash := xxhash.Sum64String(key) ^ s.seed

	for h < maxHeight && (hash&3) == 0 {
		h++
		hash >>= 2
	}

	return h
}

func (s *SkipList) findSplice(key string, prevList, nextList *[maxHeight]*node) {
	x := s.head

	for level := s.height.Load() - 1; level >= 0; level-- {
		for {
			next := x.next[level].Load()
			if next == nil || next.key >= key {
				prevList[level] = x
				nextList[level] = next
				break
			}
			x = next
		}
	}
}

func (s *SkipList) findSpliceAtLevel(key string, targetLevel int32) (*node, *node) {
	x := s.head

	for level := s.height.Load() - 1; level >= targetLevel; level-- {
		for {
			next := x.next[level].Load()
			if next == nil || next.key >= key {
				if level == targetLevel {
					return x, next
				}
				break
			}
			x = next
		}
	}

	return s.head, s.head.next[targetLevel].Load()
}

func (s *SkipList) updateNode(n *node, op Op) {
	for {
		oldPtr := n.op.Load()

		merged := coalesceToNew(*oldPtr, op)
		if n.op.CompareAndSwap(oldPtr, &merged) {
			s.dataBytes.Add(int64(len(merged.Data) - len(oldPtr.Data)))
			return
		}
	}
}

func (s *SkipList) prepareNewLevels(
	oldHeight,
	nodeHeight int32,
	prevList,
	nextList *[maxHeight]*node,
) {
	if nodeHeight <= oldHeight {
		return
	}

	for level := oldHeight; level < nodeHeight; level++ {
		prevList[level] = s.head
		nextList[level] = nil
	}
}

func (s *SkipList) publishBaseLevel(
	node *node,
	prevList,
	nextList *[maxHeight]*node,
) bool {
	node.next[0].Store(nextList[0])

	if !prevList[0].next[0].CompareAndSwap(nextList[0], node) {
		return false
	}

	s.nodeCount.Add(1)
	s.dataBytes.Add(int64(len(node.op.Load().Data)))
	return true
}

func (s *SkipList) publishUpperLevels(
	key string,
	node *node,
	nodeHeight int32,
	prevList,
	nextList *[maxHeight]*node,
) {
	for level := int32(1); level < nodeHeight; level++ {
		for {
			node.next[level].Store(nextList[level])

			if prevList[level].next[level].CompareAndSwap(nextList[level], node) {
				break
			}

			prevList[level], nextList[level] = s.findSpliceAtLevel(key, level)
		}
	}
}

func (s *SkipList) growHeight(oldHeight, nodeHeight int32) {
	for nodeHeight > oldHeight {
		if s.height.CompareAndSwap(oldHeight, nodeHeight) {
			return
		}
		oldHeight = s.height.Load()
	}
}

func coalesceToNew(old, next Op) Op {
	switch next.Kind {
	case OpPut, OpDelete:
		return next
	case OpInc:
		if old.Kind == OpInc {
			sum := DecodeInc(old) + DecodeInc(next)
			return NewInc(sum)
		}
		return next
	default:
		return next
	}
}
