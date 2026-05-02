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
	head      *Node
	height    atomic.Int32
	nodeCount atomic.Int64
	seed      uint64
}

// Node is one published skiplist node.
//
// Node is exported only because higher internal layers may need to talk about
// skiplist internals during debugging and tests. Callers should not mutate Node
// fields directly.
type Node struct {
	key  string
	op   atomic.Pointer[Op]
	next []atomic.Pointer[Node]
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
		head: &Node{
			next: make([]atomic.Pointer[Node], maxHeight),
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

func (s *SkipList) apply(key string, op Op) {
	var prevList, nextList [maxHeight]*Node

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

func (s *SkipList) read(key string) (Op, bool) {
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

func (s *SkipList) safeRead(key string) (Op, bool) {
	op, ok := s.read(key)
	if !ok {
		return Op{}, false
	}

	return *op.copy(), true
}

func (s *SkipList) iter() iter.Seq2[string, Op] {
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

func (s *SkipList) safeIter() iter.Seq2[string, Op] {
	return func(yield func(string, Op) bool) {
		for key, op := range s.iter() {
			if !yield(key, *op.copy()) {
				return
			}
		}
	}
}

func newNode(key string, op Op, height int32) *Node {
	node := &Node{
		key:  key,
		next: make([]atomic.Pointer[Node], height),
	}
	node.op.Store(&op)

	return node
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

func (s *SkipList) findSplice(key string, prevList, nextList *[maxHeight]*Node) {
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

func (s *SkipList) findSpliceAtLevel(key string, targetLevel int32) (*Node, *Node) {
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

func (s *SkipList) updateNode(node *Node, op Op) {
	for {
		oldPtr := node.op.Load()

		merged := CoalesceToNew(*oldPtr, op)
		if node.op.CompareAndSwap(oldPtr, &merged) {
			return
		}
	}
}

func (s *SkipList) prepareNewLevels(
	oldHeight,
	nodeHeight int32,
	prevList,
	nextList *[maxHeight]*Node,
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
	node *Node,
	prevList,
	nextList *[maxHeight]*Node,
) bool {
	node.next[0].Store(nextList[0])

	if !prevList[0].next[0].CompareAndSwap(nextList[0], node) {
		return false
	}

	s.nodeCount.Add(1)
	return true
}

func (s *SkipList) publishUpperLevels(
	key string,
	node *Node,
	nodeHeight int32,
	prevList,
	nextList *[maxHeight]*Node,
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

// CoalesceToNew merges next into old when the operation pair is mergeable.
//
// The returned Op is safe to publish as a new immutable value. The input Op
// values are not mutated.
func CoalesceToNew(old, next Op) Op {
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
