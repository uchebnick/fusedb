package skiplist

import (
	"bytes"
	"encoding/binary"
)

// OpKind identifies the kind of buffered mutation.
type OpKind uint8

const (
	// OpPut replaces the materialized value for a key.
	OpPut OpKind = iota
	// OpDelete marks a key as deleted without physically removing its node.
	OpDelete
	// OpInc adds a signed integer delta to a counter value.
	OpInc
)

// Op is one immutable buffered mutation.
//
// Once an Op is passed to SkipList, callers must not mutate Data. Use NewPut,
// NewDelete, and NewInc to create operations with the expected ownership rules.
type Op struct {
	Kind OpKind
	Data []byte
}

// NewPut returns a put operation with an owned copy of value.
func NewPut(value []byte) Op {
	return Op{
		Kind: OpPut,
		Data: bytes.Clone(value),
	}
}

// NewDelete returns a delete tombstone operation.
func NewDelete() Op {
	return Op{
		Kind: OpDelete,
	}
}

// NewInc returns an increment operation encoded as a signed varint delta.
func NewInc(delta int64) Op {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], delta)

	data := make([]byte, n)
	copy(data, buf[:n])

	return Op{
		Kind: OpInc,
		Data: data,
	}
}

// DecodeInc decodes an OpInc delta.
//
// It panics if op is not an increment or if the varint payload is invalid.
func DecodeInc(op Op) int64 {
	if op.Kind != OpInc {
		panic("not inc op")
	}

	delta, n := binary.Varint(op.Data)
	if n <= 0 {
		panic("bad inc encoding")
	}

	return delta
}

// MergeIncInto merges next into dst.
//
// Both operations must be OpInc. This helper mutates dst and is intended for
// local scratch values only, not for Op values already published into a
// SkipList.
func MergeIncInto(dst *Op, next Op) {
	if dst == nil {
		panic("nil inc dst")
	}
	sum := DecodeInc(*dst) + DecodeInc(next)

	need := varintLen64(sum)
	if cap(dst.Data) < need {
		dst.Data = make([]byte, need)
	}

	n := binary.PutVarint(dst.Data[:need], sum)
	dst.Kind = OpInc
	dst.Data = dst.Data[:n]
}

// MergeInc returns a new increment operation containing op1 + op2.
func MergeInc(op1, op2 Op) Op {
	MergeIncInto(&op1, op2)
	return op1
}

func varintLen64(v int64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutVarint(buf[:], v)
}

func (o *Op) copy() *Op {
	op := &Op{
		Kind: o.Kind,
		Data: make([]byte, len(o.Data)),
	}

	copy(op.Data, o.Data)
	return op
}
