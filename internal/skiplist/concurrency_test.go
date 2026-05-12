package skiplist

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/uchebnick/fusedb/internal/ops"
)

func TestConcurrentPutReadDifferentKeys(t *testing.T) {
	list := NewSkipList(42)

	const writers = 8
	const perWriter = 1000

	var wg sync.WaitGroup
	wg.Add(writers)
	for worker := 0; worker < writers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("worker:%02d:key:%04d", worker, i)
				list.Apply([]byte(key), ops.NewPut([]byte("value")))
				if _, ok := list.Read([]byte(key)); !ok {
					t.Errorf("read %s: not found", key)
					return
				}
			}
		}()
	}
	wg.Wait()

	want := int64(writers * perWriter)
	if got := list.Len(); got != want {
		t.Fatalf("len = %d, want %d", got, want)
	}
	if got := list.DataBytes(); got != want*int64(len("value")) {
		t.Fatalf("data bytes = %d, want %d", got, want*int64(len("value")))
	}
}

func TestConcurrentMixedOperationsRace(t *testing.T) {
	list := NewSkipList(42)

	const goroutines = 8
	const iterations = 2000
	const keyCount = 64

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for worker := 0; worker < goroutines; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("key:%02d", (worker+i)%keyCount)
				switch i % 4 {
				case 0:
					list.Apply([]byte(key), ops.NewPut([]byte("value")))
				case 1:
					list.Apply([]byte(key), ops.NewInc(1))
				case 2:
					list.Apply([]byte(key), ops.NewDelete())
				default:
					_, _ = list.SafeRead([]byte(key))
				}
			}
		}()
	}
	wg.Wait()

	if got := list.Len(); got > keyCount {
		t.Fatalf("len = %d, want <= %d", got, keyCount)
	}
}

func TestConcurrentReadWhileInc(t *testing.T) {
	list := NewSkipList(42)
	list.Apply([]byte("counter"), ops.NewInc(0))

	const writers = 4
	const readers = 4
	const perWriter = 2000

	var stop atomic.Bool

	var readerWG sync.WaitGroup
	readerWG.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer readerWG.Done()
			for !stop.Load() {
				op, ok := list.Read([]byte("counter"))
				if !ok {
					t.Errorf("read counter: not found")
					return
				}
				if op.Kind != ops.OpInc {
					t.Errorf("read counter kind = %d, want inc", op.Kind)
					return
				}
				_ = ops.DecodeInc(op)
			}
		}()
	}

	var writerWG sync.WaitGroup
	writerWG.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer writerWG.Done()
			for j := 0; j < perWriter; j++ {
				list.Apply([]byte("counter"), ops.NewInc(1))
			}
		}()
	}
	writerWG.Wait()
	stop.Store(true)
	readerWG.Wait()

	op, ok := list.Read([]byte("counter"))
	if !ok {
		t.Fatal("read counter: not found")
	}
	want := int64(writers * perWriter)
	if delta := ops.DecodeInc(op); delta != want {
		t.Fatalf("counter delta = %d, want %d", delta, want)
	}
	if got := list.DataBytes(); got != int64(len(op.Data)) {
		t.Fatalf("data bytes = %d, want %d", got, len(op.Data))
	}
}

func TestConcurrentIterWhileWriting(t *testing.T) {
	list := NewSkipList(42)
	for i := 0; i < 128; i++ {
		list.Apply([]byte(fmt.Sprintf("seed:%03d", i)), ops.NewPut([]byte("value")))
	}

	const writers = 4
	const readers = 4
	const iterations = 1000

	var stop atomic.Bool
	var readerWG sync.WaitGroup
	readerWG.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer readerWG.Done()
			for !stop.Load() {
				var last []byte
				for key, op := range list.Iter() {
					if last != nil && bytes.Compare(key, last) < 0 {
						t.Errorf("iterator order: %q before %q", key, last)
						return
					}
					if op.Kind > ops.OpInc {
						t.Errorf("bad op kind: %d", op.Kind)
						return
					}
					last = key
				}
			}
		}()
	}

	var writerWG sync.WaitGroup
	writerWG.Add(writers)
	for worker := 0; worker < writers; worker++ {
		worker := worker
		go func() {
			defer writerWG.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("writer:%02d:key:%04d", worker, i)
				list.Apply([]byte(key), ops.NewPut([]byte("value")))
			}
		}()
	}
	writerWG.Wait()
	stop.Store(true)
	readerWG.Wait()
}

func TestSafeAPIsReturnOwnedData(t *testing.T) {
	list := NewSkipList(42)
	list.Apply([]byte("alpha"), ops.NewPut([]byte("stable")))

	valueOp, ok := list.SafeRead([]byte("alpha"))
	if !ok {
		t.Fatal("safe read alpha: not found")
	}
	valueOp.Data[0] = 'X'

	op, ok := list.SafeRead([]byte("alpha"))
	if !ok {
		t.Fatal("safe read alpha: not found")
	}
	op.Data[0] = 'Y'

	for _, safeOp := range list.SafeIter() {
		if safeOp.Kind == ops.OpPut {
			safeOp.Data[0] = 'Z'
		}
	}

	again, ok := list.SafeRead([]byte("alpha"))
	if !ok {
		t.Fatal("safe read alpha again: not found")
	}
	if string(again.Data) != "stable" {
		t.Fatalf("safe API exposed shared data: got %q", again.Data)
	}
}
