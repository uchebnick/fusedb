package skiplist

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
)

var benchOpSink Op

func BenchmarkMergeIncReuse(b *testing.B) {
	op := NewInc(0)
	inc := NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op = MergeInc(op, inc)
	}

	benchOpSink = op
}

func BenchmarkMergeIncReuseMaxCap(b *testing.B) {
	op := NewInc(0)
	inc := NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op = mergeIncMaxCap(op, inc)
	}

	benchOpSink = op
}

func BenchmarkMergeIncAllocate(b *testing.B) {
	op := NewInc(0)
	inc := NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op = mergeIncAllocate(op, inc)
	}

	benchOpSink = op
}

func BenchmarkMergeIncGrowExactCap(b *testing.B) {
	inc := NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := NewInc(int64(i))
		op.Data = op.Data[:len(op.Data):len(op.Data)]
		benchOpSink = MergeInc(op, inc)
	}
}

func BenchmarkMergeIncGrowMaxCap(b *testing.B) {
	inc := NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := NewInc(int64(i))
		op.Data = op.Data[:len(op.Data):len(op.Data)]
		benchOpSink = mergeIncMaxCap(op, inc)
	}
}

func mergeIncAllocate(op1, op2 Op) Op {
	return NewInc(DecodeInc(op1) + DecodeInc(op2))
}

func mergeIncMaxCap(op1, op2 Op) Op {
	sum := DecodeInc(op1) + DecodeInc(op2)
	if cap(op1.Data) < binary.MaxVarintLen64 {
		op1.Data = make([]byte, binary.MaxVarintLen64)
	}
	n := binary.PutVarint(op1.Data[:binary.MaxVarintLen64], sum)
	op1.Kind = OpInc
	op1.Data = op1.Data[:n]
	return op1
}

func BenchmarkCoalesceCopyOldPtr(b *testing.B) {
	var ptr atomic.Pointer[Op]
	initial := NewInc(0)
	ptr.Store(&initial)
	next := NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oldPtr := ptr.Load()
		merged := CoalesceToNew(*oldPtr, next)
		if !ptr.CompareAndSwap(oldPtr, &merged) {
			b.Fatal("unexpected CAS failure")
		}
	}

	benchOpSink = *ptr.Load()
}

func BenchmarkCoalesceReuseNextScratch(b *testing.B) {
	var ptr atomic.Pointer[Op]
	initial := NewInc(0)
	ptr.Store(&initial)
	next := NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oldPtr := ptr.Load()
		merged := coalesceReuseNextScratch(*oldPtr, next)
		if !ptr.CompareAndSwap(oldPtr, &merged) {
			b.Fatal("unexpected CAS failure")
		}
	}

	benchOpSink = *ptr.Load()
}

func coalesceReuseNextScratch(old Op, next Op) Op {
	switch next.Kind {
	case OpPut, OpDelete:
		return next
	case OpInc:
		if old.Kind != OpInc {
			return next
		}
		sum := DecodeInc(old) + DecodeInc(next)
		n := putIncInto(&next, sum)
		next.Data = next.Data[:n]
		return next
	default:
		return next
	}
}

func putIncInto(dst *Op, delta int64) int {
	if cap(dst.Data) < binary.MaxVarintLen64 {
		dst.Data = make([]byte, binary.MaxVarintLen64)
	}
	dst.Kind = OpInc
	return binary.PutVarint(dst.Data[:binary.MaxVarintLen64], delta)
}
