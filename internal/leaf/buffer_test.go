package leaf

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/uchebnick/fusedb/internal/ops"
)

func TestBufferPutReadOp(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("alpha"), []byte("1"))

	op, ok := buffer.ReadOp([]byte("alpha"))
	if !ok {
		t.Fatal("read alpha: not found")
	}
	if op.Kind != ops.OpPut || !bytes.Equal(op.Data, []byte("1")) {
		t.Fatalf("read alpha = %#v, want put(1)", op)
	}
	if buffer.Len() != 1 {
		t.Fatalf("len = %d, want 1", buffer.Len())
	}
	if got, want := buffer.EstimatedBytes(), int64(len("1")); got != want {
		t.Fatalf("estimated bytes = %d, want %d", got, want)
	}
}

func TestBufferDeleteTombstone(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("alpha"), []byte("1"))
	buffer.Delete([]byte("alpha"))

	op, ok := buffer.ReadOp([]byte("alpha"))
	if !ok {
		t.Fatal("read alpha: not found")
	}
	if op.Kind != ops.OpDelete {
		t.Fatalf("read alpha kind = %d, want delete", op.Kind)
	}
	if buffer.Len() != 1 {
		t.Fatalf("len = %d, want 1", buffer.Len())
	}
	if got := buffer.DataBytes(); got != 0 {
		t.Fatalf("data bytes = %d, want 0 after delete", got)
	}
}

func TestBufferIncCoalesces(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Inc([]byte("counter"), 2)
	buffer.Inc([]byte("counter"), 3)

	op, ok := buffer.ReadOp([]byte("counter"))
	if !ok {
		t.Fatal("read counter: not found")
	}
	if op.Kind != ops.OpInc {
		t.Fatalf("read counter kind = %d, want inc", op.Kind)
	}
	if delta := ops.DecodeInc(op); delta != 5 {
		t.Fatalf("counter delta = %d, want 5", delta)
	}
}

func TestBufferEstimatedBytesTracksUniformWrites(t *testing.T) {
	buffer := NewBuffer(42)

	for i := 0; i < 100; i++ {
		buffer.Put([]byte(fmt.Sprintf("key:%03d", i)), []byte("value"))
	}

	got := buffer.EstimatedBytes()
	want := actualBufferDataBytes(buffer)
	if got != want {
		t.Fatalf("estimated bytes = %d, want exact %d", got, want)
	}
}

func TestBufferRetainedBytesChargesKeysAndTombstones(t *testing.T) {
	buffer := NewBuffer(42)
	buffer.Delete([]byte("small-key"))
	if got := buffer.RetainedBytes(); got <= int64(len("small-key")) {
		t.Fatalf("retained bytes for tombstone = %d, want key plus node overhead", got)
	}
}

func TestBufferDataBytesScenarios(t *testing.T) {
	tests := []struct {
		name string
		fill func(*Buffer)
	}{
		{
			name: "uniform values",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					buffer.Put([]byte(fmt.Sprintf("key:%03d", i)), []byte("value"))
				}
			},
		},
		{
			name: "mixed value sizes",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					value := bytes.Repeat([]byte{'x'}, 1+i%32)
					buffer.Put([]byte(fmt.Sprintf("key:%03d", i)), value)
				}
			},
		},
		{
			name: "delete heavy",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					key := fmt.Sprintf("key:%03d", i)
					buffer.Put([]byte(key), []byte("value"))
					if i%2 == 0 {
						buffer.Delete([]byte(key))
					}
				}
			},
		},
		{
			name: "hot key updates",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					buffer.Put([]byte("hot"), bytes.Repeat([]byte{'x'}, 1+i%64))
				}
			},
		},
		{
			name: "counter increments",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					buffer.Inc([]byte(fmt.Sprintf("counter:%03d", i%64)), 1)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := NewBuffer(42)
			tt.fill(buffer)

			estimated := buffer.EstimatedBytes()
			actual := actualBufferDataBytes(buffer)
			t.Logf("estimated=%d actual=%d", estimated, actual)
			if estimated != actual {
				t.Fatalf("estimated bytes = %d, want exact %d", estimated, actual)
			}
		})
	}
}

func TestBufferDataBytesTracksMutations(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("alpha"), bytes.Repeat([]byte{'x'}, 100))
	afterPut := buffer.EstimatedBytes()

	buffer.Put([]byte("alpha"), bytes.Repeat([]byte{'y'}, 100))
	afterUpdate := buffer.EstimatedBytes()

	buffer.Put([]byte("alpha"), bytes.Repeat([]byte{'z'}, 40))
	afterShortUpdate := buffer.EstimatedBytes()

	buffer.Delete([]byte("alpha"))
	afterDelete := buffer.EstimatedBytes()

	if afterPut != 100 {
		t.Fatalf("put estimate = %d, want 100", afterPut)
	}
	if afterUpdate != 100 {
		t.Fatalf("same-size update estimate = %d, want 100", afterUpdate)
	}
	if afterShortUpdate != 40 {
		t.Fatalf("short update estimate = %d, want 40", afterShortUpdate)
	}
	if afterDelete != 0 {
		t.Fatalf("delete estimate = %d, want 0", afterDelete)
	}
}

func TestBufferDataBytesLargeScenarios(t *testing.T) {
	tests := []struct {
		name string
		fill func(*Buffer)
	}{
		{
			name: "uniform 2 MiB",
			fill: func(buffer *Buffer) {
				value := bytes.Repeat([]byte{'x'}, 1024)
				for i := 0; i < 2048; i++ {
					buffer.Put([]byte(fmt.Sprintf("key:%06d", i)), value)
				}
			},
		},
		{
			name: "mixed 3 MiB",
			fill: func(buffer *Buffer) {
				for i := 0; i < 2048; i++ {
					value := bytes.Repeat([]byte{'x'}, 768+i%1024)
					buffer.Put([]byte(fmt.Sprintf("key:%06d", i)), value)
				}
			},
		},
		{
			name: "updates and deletes 2 MiB",
			fill: func(buffer *Buffer) {
				value := bytes.Repeat([]byte{'x'}, 520)
				updated := bytes.Repeat([]byte{'y'}, 1032)
				for i := 0; i < 4096; i++ {
					key := fmt.Sprintf("key:%06d", i)
					buffer.Put([]byte(key), value)
					if i%4 == 0 {
						buffer.Put([]byte(key), updated)
					}
					if i%5 == 0 {
						buffer.Delete([]byte(key))
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := NewBuffer(42)
			tt.fill(buffer)

			estimated := buffer.EstimatedBytes()
			actual := actualBufferDataBytes(buffer)
			t.Logf("estimated=%d actual=%d", estimated, actual)
			if estimated < 2<<20 || estimated > 3<<20 {
				t.Fatalf("estimated bytes = %d, want 2-3 MiB", estimated)
			}
			if estimated != actual {
				t.Fatalf("estimated bytes = %d, want exact %d", estimated, actual)
			}
		})
	}
}

func TestBufferSafeReadOwnsData(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("alpha"), []byte("stable"))

	op, ok := buffer.SafeReadOp([]byte("alpha"))
	if !ok {
		t.Fatal("safe read alpha: not found")
	}
	op.Data[0] = 'X'

	again, ok := buffer.SafeReadOp([]byte("alpha"))
	if !ok {
		t.Fatal("safe read alpha again: not found")
	}
	if string(again.Data) != "stable" {
		t.Fatalf("safe read exposed shared data: got %q", again.Data)
	}
}

func TestBufferIterOpsOrdered(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("beta"), []byte("2"))
	buffer.Put([]byte("alpha"), []byte("1"))
	buffer.Delete([]byte("gamma"))

	var keys []string
	for key := range buffer.IterOps() {
		keys = append(keys, string(key))
	}

	want := []string{"alpha", "beta", "gamma"}
	if len(keys) != len(want) {
		t.Fatalf("iter keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("iter keys = %v, want %v", keys, want)
		}
	}
}

func TestBufferFreezeDetachesActiveWrites(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("alpha"), []byte("1"))
	buffer.Put([]byte("beta"), []byte("2"))

	if !buffer.Freeze() {
		t.Fatal("freeze returned false")
	}
	if buffer.FrozenLen() != 2 {
		t.Fatalf("frozen len = %d, want 2", buffer.FrozenLen())
	}
	if buffer.Len() != 0 {
		t.Fatalf("active len after freeze = %d, want 0", buffer.Len())
	}

	buffer.Put([]byte("alpha"), []byte("new"))
	buffer.Put([]byte("gamma"), []byte("3"))

	oldAlpha, ok := buffer.ReadFrozen([]byte("alpha"))
	if !ok {
		t.Fatal("frozen read alpha: not found")
	}
	if string(oldAlpha.Data) != "1" {
		t.Fatalf("frozen alpha = %q, want old value 1", oldAlpha.Data)
	}

	newAlpha, ok := buffer.ReadOp([]byte("alpha"))
	if !ok {
		t.Fatal("active read alpha: not found")
	}
	if string(newAlpha.Data) != "new" {
		t.Fatalf("active alpha = %q, want new", newAlpha.Data)
	}

	var frozenKeys []string
	for key := range buffer.IterFrozen() {
		frozenKeys = append(frozenKeys, string(key))
	}
	if got, want := fmt.Sprint(frozenKeys), "[alpha beta]"; got != want {
		t.Fatalf("frozen keys = %s, want %s", got, want)
	}

	var activeKeys []string
	for key := range buffer.IterOps() {
		activeKeys = append(activeKeys, string(key))
	}
	if got, want := fmt.Sprint(activeKeys), "[alpha gamma]"; got != want {
		t.Fatalf("active keys = %s, want %s", got, want)
	}
}

func TestBufferReadFallsBackToFrozen(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("old-only"), []byte("1"))
	buffer.Put([]byte("shadowed"), []byte("old"))
	if !buffer.Freeze() {
		t.Fatal("freeze returned false")
	}

	buffer.Put([]byte("shadowed"), []byte("new"))

	oldOnly, ok := buffer.ReadOp([]byte("old-only"))
	if !ok {
		t.Fatal("read old-only: not found")
	}
	if string(oldOnly.Data) != "1" {
		t.Fatalf("old-only = %q, want 1", oldOnly.Data)
	}

	shadowed, ok := buffer.ReadOp([]byte("shadowed"))
	if !ok {
		t.Fatal("read shadowed: not found")
	}
	if string(shadowed.Data) != "new" {
		t.Fatalf("shadowed = %q, want active value new", shadowed.Data)
	}

	if buffer.FrozenLen() != 2 {
		t.Fatalf("frozen len = %d, want 2", buffer.FrozenLen())
	}

	buffer.ClearFrozen()
	if _, ok := buffer.ReadOp([]byte("old-only")); ok {
		t.Fatal("old-only should not be visible after clearing frozen ops")
	}
}

func TestBufferReadMergesActiveAndFrozenInc(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Inc([]byte("counter"), 10)
	if !buffer.Freeze() {
		t.Fatal("freeze returned false")
	}
	buffer.Inc([]byte("counter"), 5)

	op, ok := buffer.ReadOp([]byte("counter"))
	if !ok {
		t.Fatal("read counter: not found")
	}
	if op.Kind != ops.OpInc {
		t.Fatalf("counter kind = %d, want inc", op.Kind)
	}
	if got := ops.DecodeInc(op); got != 15 {
		t.Fatalf("counter delta = %d, want 15", got)
	}
}

func TestBufferFreezeRejectsExistingFrozen(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put([]byte("alpha"), []byte("1"))
	if !buffer.Freeze() {
		t.Fatal("first freeze returned false")
	}

	buffer.Put([]byte("beta"), []byte("2"))
	if buffer.Freeze() {
		t.Fatal("second freeze should fail while frozen layer exists")
	}
	if buffer.Len() != 1 {
		t.Fatalf("active len after rejected freeze = %d, want 1", buffer.Len())
	}
	if buffer.FrozenLen() != 1 {
		t.Fatalf("frozen len after rejected freeze = %d, want 1", buffer.FrozenLen())
	}
}

func TestBufferRollbackFrozenPreservesGenerationOrder(t *testing.T) {
	buffer := NewBuffer(42)
	buffer.Put([]byte("old-only"), []byte("old"))
	buffer.Inc([]byte("counter"), 2)
	if !buffer.Freeze() {
		t.Fatal("freeze returned false")
	}

	buffer.Put([]byte("new-only"), []byte("new"))
	buffer.Inc([]byte("counter"), 3)
	buffer.Put([]byte("old-only"), []byte("newer"))

	if !buffer.RollbackFrozen() {
		t.Fatal("rollback returned false")
	}
	if buffer.FrozenLen() != 0 {
		t.Fatalf("frozen len after rollback = %d, want 0", buffer.FrozenLen())
	}
	if !buffer.Freeze() {
		t.Fatal("freeze after rollback returned false")
	}

	oldOnly, ok := buffer.ReadFrozen([]byte("old-only"))
	if !ok || string(oldOnly.Data) != "newer" {
		t.Fatalf("old-only after rollback = %#v, %v; want put(newer), true", oldOnly, ok)
	}
	newOnly, ok := buffer.ReadFrozen([]byte("new-only"))
	if !ok || string(newOnly.Data) != "new" {
		t.Fatalf("new-only after rollback = %#v, %v; want put(new), true", newOnly, ok)
	}
	counter, ok := buffer.ReadFrozen([]byte("counter"))
	if !ok || counter.Kind != ops.OpInc || ops.DecodeInc(counter) != 5 {
		t.Fatalf("counter after rollback = %#v, %v; want inc(5), true", counter, ok)
	}
}

func actualBufferDataBytes(buffer *Buffer) int64 {
	var total int64
	for _, op := range buffer.IterOps() {
		total += int64(len(op.Data))
	}
	return total
}
