package skiplist

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
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
				list.Put(key, []byte("value"))
				if _, ok := list.Read(key); !ok {
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
					list.Put(key, []byte("value"))
				case 1:
					list.Inc(key, 1)
				case 2:
					list.Delete(key)
				default:
					_, _ = list.SafeRead(key)
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
	list.Inc("counter", 0)

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
				op, ok := list.Read("counter")
				if !ok {
					t.Errorf("read counter: not found")
					return
				}
				if op.Kind != OpInc {
					t.Errorf("read counter kind = %d, want inc", op.Kind)
					return
				}
				_ = DecodeInc(op)
			}
		}()
	}

	var writerWG sync.WaitGroup
	writerWG.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer writerWG.Done()
			for j := 0; j < perWriter; j++ {
				list.Inc("counter", 1)
			}
		}()
	}
	writerWG.Wait()
	stop.Store(true)
	readerWG.Wait()

	op, ok := list.Read("counter")
	if !ok {
		t.Fatal("read counter: not found")
	}
	want := int64(writers * perWriter)
	if delta := DecodeInc(op); delta != want {
		t.Fatalf("counter delta = %d, want %d", delta, want)
	}
}

func TestConcurrentIterWhileWriting(t *testing.T) {
	list := NewSkipList(42)
	for i := 0; i < 128; i++ {
		list.Put(fmt.Sprintf("seed:%03d", i), []byte("value"))
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
				last := ""
				for key, op := range list.Iter() {
					if last != "" && key < last {
						t.Errorf("iterator order: %q before %q", key, last)
						return
					}
					if op.Kind > OpInc {
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
				list.Put(key, []byte("value"))
			}
		}()
	}
	writerWG.Wait()
	stop.Store(true)
	readerWG.Wait()
}

func TestSafeAPIsReturnOwnedData(t *testing.T) {
	list := NewSkipList(42)
	list.Put("alpha", []byte("stable"))

	value, ok := list.Get("alpha")
	if !ok {
		t.Fatal("get alpha: not found")
	}
	value[0] = 'X'

	op, ok := list.SafeRead("alpha")
	if !ok {
		t.Fatal("safe read alpha: not found")
	}
	op.Data[0] = 'Y'

	for _, safeOp := range list.SafeIter() {
		if safeOp.Kind == OpPut {
			safeOp.Data[0] = 'Z'
		}
	}

	again, ok := list.Get("alpha")
	if !ok {
		t.Fatal("get alpha again: not found")
	}
	if string(again) != "stable" {
		t.Fatalf("safe API exposed shared data: got %q", again)
	}
}
