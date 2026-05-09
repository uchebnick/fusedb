package skiplist

import (
	"bytes"
	"fmt"
	"fusedb/internal/ops"
	"sync"
	"testing"
)

func TestReadPut(t *testing.T) {
	list := NewSkipList(42)

	list.Apply([]byte("beta"), ops.NewPut([]byte("2")))
	list.Apply([]byte("alpha"), ops.NewPut([]byte("1")))
	list.Apply([]byte("gamma"), ops.NewPut([]byte("3")))

	got, ok := list.Read([]byte("alpha"))
	if !ok {
		t.Fatal("read alpha: not found")
	}
	if got.Kind != ops.OpPut || !bytes.Equal(got.Data, []byte("1")) {
		t.Fatalf("read alpha = %#v, want put(1)", got)
	}
	if list.Len() != 3 {
		t.Fatalf("len = %d, want 3", list.Len())
	}
}

func TestOperationAPI(t *testing.T) {
	list := NewSkipList(42)

	list.Apply([]byte("alpha"), ops.NewPut([]byte("1")))
	op, ok := list.SafeRead([]byte("alpha"))
	if !ok || op.Kind != ops.OpPut || !bytes.Equal(op.Data, []byte("1")) {
		t.Fatalf("safe read alpha = %#v, %v; want put(1), true", op, ok)
	}

	op.Data[0] = 'X'
	again, ok := list.SafeRead([]byte("alpha"))
	if !ok || again.Kind != ops.OpPut || string(again.Data) != "1" {
		t.Fatalf("safe read alpha after mutation = %#v, %v; want put(1), true", again, ok)
	}

	list.Apply([]byte("alpha"), ops.NewDelete())
	op, ok = list.Read([]byte("alpha"))
	if !ok || op.Kind != ops.OpDelete {
		t.Fatalf("read deleted alpha = %#v, %v; want delete, true", op, ok)
	}

	list.Apply([]byte("counter"), ops.NewInc(2))
	list.Apply([]byte("counter"), ops.NewInc(3))
	op, ok = list.Read([]byte("counter"))
	if !ok {
		t.Fatal("read counter: not found")
	}
	if delta := ops.DecodeInc(op); delta != 5 {
		t.Fatalf("counter delta = %d, want 5", delta)
	}
}

func TestDataBytesTracksMutations(t *testing.T) {
	list := NewSkipList(42)

	list.Apply([]byte("alpha"), ops.NewPut([]byte("12345")))
	if got, want := list.DataBytes(), int64(5); got != want {
		t.Fatalf("data bytes after insert = %d, want %d", got, want)
	}

	list.Apply([]byte("alpha"), ops.NewPut([]byte("123456789")))
	if got, want := list.DataBytes(), int64(9); got != want {
		t.Fatalf("data bytes after update = %d, want %d", got, want)
	}

	list.Apply([]byte("beta"), ops.NewPut([]byte("12")))
	if got, want := list.DataBytes(), int64(11); got != want {
		t.Fatalf("data bytes after second insert = %d, want %d", got, want)
	}

	list.Apply([]byte("alpha"), ops.NewDelete())
	if got, want := list.DataBytes(), int64(2); got != want {
		t.Fatalf("data bytes after delete = %d, want %d", got, want)
	}

	list.Apply([]byte("counter"), ops.NewInc(1))
	if got, want := list.DataBytes(), int64(3); got != want {
		t.Fatalf("data bytes after inc insert = %d, want %d", got, want)
	}

	list.Apply([]byte("counter"), ops.NewInc(127))
	if got, want := list.DataBytes(), int64(4); got != want {
		t.Fatalf("data bytes after inc coalesce = %d, want %d", got, want)
	}
}

func TestReadMissing(t *testing.T) {
	list := NewSkipList(42)

	list.Apply([]byte("alpha"), ops.NewPut([]byte("1")))
	list.Apply([]byte("gamma"), ops.NewPut([]byte("3")))

	if got, ok := list.Read([]byte("beta")); ok {
		t.Fatalf("read beta = %#v, want not found", got)
	}
}

func TestReadReturnsCopy(t *testing.T) {
	list := NewSkipList(42)

	list.Apply([]byte("alpha"), ops.NewPut([]byte("stable")))

	got, ok := list.SafeRead([]byte("alpha"))
	if !ok {
		t.Fatal("read alpha: not found")
	}
	got.Data[0] = 'X'

	again, ok := list.SafeRead([]byte("alpha"))
	if !ok {
		t.Fatal("read alpha again: not found")
	}
	if string(again.Data) != "stable" {
		t.Fatalf("read returned shared data: got %q", again.Data)
	}
}

func TestReadDeleteTombstone(t *testing.T) {
	list := NewSkipList(42)

	list.Apply([]byte("alpha"), ops.NewPut([]byte("1")))
	list.Apply([]byte("alpha"), ops.NewDelete())

	got, ok := list.Read([]byte("alpha"))
	if !ok {
		t.Fatal("read alpha tombstone: not found")
	}
	if got.Kind != ops.OpDelete {
		t.Fatalf("read alpha kind = %d, want delete", got.Kind)
	}
	if list.Len() != 1 {
		t.Fatalf("len = %d, want 1", list.Len())
	}
}

func TestReadCoalescedInc(t *testing.T) {
	list := NewSkipList(42)

	list.Apply([]byte("counter"), ops.NewInc(2))
	list.Apply([]byte("counter"), ops.NewInc(3))

	got, ok := list.Read([]byte("counter"))
	if !ok {
		t.Fatal("read counter: not found")
	}
	if got.Kind != ops.OpInc {
		t.Fatalf("read counter kind = %d, want inc", got.Kind)
	}
	if delta := ops.DecodeInc(got); delta != 5 {
		t.Fatalf("read counter delta = %d, want 5", delta)
	}
	if list.Len() != 1 {
		t.Fatalf("len = %d, want 1", list.Len())
	}
}

func TestConcurrentIncCoalesces(t *testing.T) {
	list := NewSkipList(42)
	const goroutines = 8
	const perGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				list.Apply([]byte("counter"), ops.NewInc(1))
			}
		}()
	}
	wg.Wait()

	got, ok := list.Read([]byte("counter"))
	if !ok {
		t.Fatal("read counter: not found")
	}
	want := int64(goroutines * perGoroutine)
	if delta := ops.DecodeInc(got); delta != want {
		t.Fatalf("read counter delta = %d, want %d", delta, want)
	}
	if list.Len() != 1 {
		t.Fatalf("len = %d, want 1", list.Len())
	}
}

func makeBenchmarkSkipList(b *testing.B, n int) *SkipList {
	b.Helper()

	list := NewSkipList(42)
	for i := 0; i < n; i++ {
		list.Apply([]byte(fmt.Sprintf("key:%08d", i)), ops.NewPut([]byte("value")))
	}
	if list.Len() != int64(n) {
		b.Fatalf("len = %d, want %d", list.Len(), n)
	}
	return list
}

func BenchmarkReadHit1K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 1024)
	key := "key:00000512"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op, ok := list.Read([]byte(key))
		if !ok || op.Kind != ops.OpPut {
			b.Fatal("read hit failed")
		}
		benchOpSink = op
	}
}

func BenchmarkReadMiss1K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 1024)
	key := "key:99999999"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op, ok := list.Read([]byte(key))
		if ok {
			b.Fatal("read miss found key")
		}
		benchOpSink = op
	}
}

func BenchmarkReadHit64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)
	key := "key:00032768"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op, ok := list.Read([]byte(key))
		if !ok || op.Kind != ops.OpPut {
			b.Fatal("read hit failed")
		}
		benchOpSink = op
	}
}

func BenchmarkReadHitPositions64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)
	keys := []string{
		"key:00000000",
		"key:00000001",
		"key:00000128",
		"key:00001024",
		"key:00008192",
		"key:00016384",
		"key:00032768",
		"key:00049152",
		"key:00065535",
	}

	for _, key := range keys {
		b.Run(key, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				op, ok := list.Read([]byte(key))
				if !ok || op.Kind != ops.OpPut {
					b.Fatal("read hit failed")
				}
				benchOpSink = op
			}
		})
	}
}

func BenchmarkReadHitRotating64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%08d", i*64)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i&(len(keys)-1)]
		op, ok := list.Read([]byte(key))
		if !ok || op.Kind != ops.OpPut {
			b.Fatal("read hit failed")
		}
		benchOpSink = op
	}
}

func BenchmarkReadMiss64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)
	key := "key:99999999"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op, ok := list.Read([]byte(key))
		if ok {
			b.Fatal("read miss found key")
		}
		benchOpSink = op
	}
}

func BenchmarkReadBatch10Keys64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)
	keys := []string{
		"key:00001024",
		"key:00002048",
		"key:00004096",
		"key:00008192",
		"key:00012288",
		"key:00016384",
		"key:00024576",
		"key:00032768",
		"key:00049152",
		"key:00065535",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			op, ok := list.Read([]byte(key))
			if !ok || op.Kind != ops.OpPut {
				b.Fatal("read hit failed")
			}
			benchOpSink = op
		}
	}
}

func BenchmarkReadBatch100Keys64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%08d", i*512)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			op, ok := list.Read([]byte(key))
			if !ok || op.Kind != ops.OpPut {
				b.Fatal("read hit failed")
			}
			benchOpSink = op
		}
	}
}

func BenchmarkIterFull1K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for key, op := range list.Iter() {
			if len(key) == 0 || op.Kind != ops.OpPut {
				b.Fatal("bad iterator entry")
			}
			count++
			benchOpSink = op
		}
		if count != 1024 {
			b.Fatalf("iter count = %d, want 1024", count)
		}
	}
}

func BenchmarkSafeIterFull1K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for key, op := range list.SafeIter() {
			if len(key) == 0 || op.Kind != ops.OpPut {
				b.Fatal("bad iterator entry")
			}
			count++
			benchOpSink = op
		}
		if count != 1024 {
			b.Fatalf("iter count = %d, want 1024", count)
		}
	}
}

func BenchmarkIterFull64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for key, op := range list.Iter() {
			if len(key) == 0 || op.Kind != ops.OpPut {
				b.Fatal("bad iterator entry")
			}
			count++
			benchOpSink = op
		}
		if count != 64*1024 {
			b.Fatalf("iter count = %d, want %d", count, 64*1024)
		}
	}
}

func BenchmarkSafeIterFull64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for key, op := range list.SafeIter() {
			if len(key) == 0 || op.Kind != ops.OpPut {
				b.Fatal("bad iterator entry")
			}
			count++
			benchOpSink = op
		}
		if count != 64*1024 {
			b.Fatalf("iter count = %d, want %d", count, 64*1024)
		}
	}
}

func BenchmarkIterFirst100Of64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for key, op := range list.Iter() {
			if len(key) == 0 || op.Kind != ops.OpPut {
				b.Fatal("bad iterator entry")
			}
			count++
			benchOpSink = op
			if count == 100 {
				break
			}
		}
		if count != 100 {
			b.Fatalf("iter count = %d, want 100", count)
		}
	}
}

func BenchmarkSafeIterFirst100Of64K(b *testing.B) {
	list := makeBenchmarkSkipList(b, 64*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for key, op := range list.SafeIter() {
			if len(key) == 0 || op.Kind != ops.OpPut {
				b.Fatal("bad iterator entry")
			}
			count++
			benchOpSink = op
			if count == 100 {
				break
			}
		}
		if count != 100 {
			b.Fatalf("iter count = %d, want 100", count)
		}
	}
}

func BenchmarkApplyInsertSequential(b *testing.B) {
	list := NewSkipList(42)
	value := []byte("value")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.Apply([]byte(fmt.Sprintf("key:%08d", i)), ops.NewPut(value))
	}
}

func BenchmarkApplyPutExisting(b *testing.B) {
	list := NewSkipList(42)
	value := []byte("value")
	list.Apply([]byte("key:00000001"), ops.NewPut(value))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.Apply([]byte("key:00000001"), ops.NewPut(value))
	}
}

func BenchmarkApplyIncExisting(b *testing.B) {
	list := NewSkipList(42)
	list.Apply([]byte("counter"), ops.NewInc(0))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.Apply([]byte("counter"), ops.NewInc(1))
	}
}

func BenchmarkApplyIncExistingParallel(b *testing.B) {
	list := NewSkipList(42)
	list.Apply([]byte("counter"), ops.NewInc(0))

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			list.Apply([]byte("counter"), ops.NewInc(1))
		}
	})
}
