package skiplist

import (
	"encoding/binary"
	"github.com/uchebnick/fusedb/internal/ops"
	"sync/atomic"
	"testing"
)

var benchOpSink ops.Op

func BenchmarkMergeIncReuse(b *testing.B) {
	op := ops.NewInc(0)
	inc := ops.NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op = ops.MergeInc(op, inc)
	}

	benchOpSink = op
}

func BenchmarkMergeIncReuseMaxCap(b *testing.B) {
	op := ops.NewInc(0)
	inc := ops.NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op = mergeIncMaxCap(op, inc)
	}

	benchOpSink = op
}

func BenchmarkMergeIncAllocate(b *testing.B) {
	op := ops.NewInc(0)
	inc := ops.NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op = mergeIncAllocate(op, inc)
	}

	benchOpSink = op
}

func BenchmarkMergeIncGrowExactCap(b *testing.B) {
	inc := ops.NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := ops.NewInc(int64(i))
		op.Data = op.Data[:len(op.Data):len(op.Data)]
		benchOpSink = ops.MergeInc(op, inc)
	}
}

func BenchmarkMergeIncGrowMaxCap(b *testing.B) {
	inc := ops.NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := ops.NewInc(int64(i))
		op.Data = op.Data[:len(op.Data):len(op.Data)]
		benchOpSink = mergeIncMaxCap(op, inc)
	}
}

func mergeIncAllocate(op1, op2 ops.Op) ops.Op {
	return ops.NewInc(ops.DecodeInc(op1) + ops.DecodeInc(op2))
}

func mergeIncMaxCap(op1, op2 ops.Op) ops.Op {
	sum := ops.DecodeInc(op1) + ops.DecodeInc(op2)
	if cap(op1.Data) < binary.MaxVarintLen64 {
		op1.Data = make([]byte, binary.MaxVarintLen64)
	}
	n := binary.PutVarint(op1.Data[:binary.MaxVarintLen64], sum)
	op1.Kind = ops.OpInc
	op1.Data = op1.Data[:n]
	return op1
}

func BenchmarkCoalesceCopyOldPtr(b *testing.B) {
	var ptr atomic.Pointer[ops.Op]
	initial := ops.NewInc(0)
	ptr.Store(&initial)
	next := ops.NewInc(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oldPtr := ptr.Load()
		merged := coalesceToNew(*oldPtr, next)
		if !ptr.CompareAndSwap(oldPtr, &merged) {
			b.Fatal("unexpected CAS failure")
		}
	}

	benchOpSink = *ptr.Load()
}

func BenchmarkCoalesceReuseNextScratch(b *testing.B) {
	var ptr atomic.Pointer[ops.Op]
	initial := ops.NewInc(0)
	ptr.Store(&initial)
	next := ops.NewInc(1)

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

func coalesceReuseNextScratch(old ops.Op, next ops.Op) ops.Op {
	switch next.Kind {
	case ops.OpPut, ops.OpDelete:
		return next
	case ops.OpInc:
		if old.Kind != ops.OpInc {
			return next
		}
		sum := ops.DecodeInc(old) + ops.DecodeInc(next)
		n := putIncInto(&next, sum)
		next.Data = next.Data[:n]
		return next
	default:
		return next
	}
}

func putIncInto(dst *ops.Op, delta int64) int {
	if cap(dst.Data) < binary.MaxVarintLen64 {
		dst.Data = make([]byte, binary.MaxVarintLen64)
	}
	dst.Kind = ops.OpInc
	return binary.PutVarint(dst.Data[:binary.MaxVarintLen64], delta)
}
