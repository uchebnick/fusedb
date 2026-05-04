package leaf

import (
	"bytes"
	"fmt"
	"testing"

	"fusedb/internal/skiplist"
)

func TestBufferPutReadOp(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put("alpha", []byte("1"))

	op, ok := buffer.ReadOp("alpha")
	if !ok {
		t.Fatal("read alpha: not found")
	}
	if op.Kind != skiplist.OpPut || !bytes.Equal(op.Data, []byte("1")) {
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

	buffer.Put("alpha", []byte("1"))
	buffer.Delete("alpha")

	op, ok := buffer.ReadOp("alpha")
	if !ok {
		t.Fatal("read alpha: not found")
	}
	if op.Kind != skiplist.OpDelete {
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

	buffer.Inc("counter", 2)
	buffer.Inc("counter", 3)

	op, ok := buffer.ReadOp("counter")
	if !ok {
		t.Fatal("read counter: not found")
	}
	if op.Kind != skiplist.OpInc {
		t.Fatalf("read counter kind = %d, want inc", op.Kind)
	}
	if delta := skiplist.DecodeInc(op); delta != 5 {
		t.Fatalf("counter delta = %d, want 5", delta)
	}
}

func TestBufferEstimatedBytesTracksUniformWrites(t *testing.T) {
	buffer := NewBuffer(42)

	for i := 0; i < 100; i++ {
		buffer.Put(fmt.Sprintf("key:%03d", i), []byte("value"))
	}

	got := buffer.EstimatedBytes()
	want := actualBufferDataBytes(buffer)
	if got != want {
		t.Fatalf("estimated bytes = %d, want exact %d", got, want)
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
					buffer.Put(fmt.Sprintf("key:%03d", i), []byte("value"))
				}
			},
		},
		{
			name: "mixed value sizes",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					value := bytes.Repeat([]byte{'x'}, 1+i%32)
					buffer.Put(fmt.Sprintf("key:%03d", i), value)
				}
			},
		},
		{
			name: "delete heavy",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					key := fmt.Sprintf("key:%03d", i)
					buffer.Put(key, []byte("value"))
					if i%2 == 0 {
						buffer.Delete(key)
					}
				}
			},
		},
		{
			name: "hot key updates",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					buffer.Put("hot", bytes.Repeat([]byte{'x'}, 1+i%64))
				}
			},
		},
		{
			name: "counter increments",
			fill: func(buffer *Buffer) {
				for i := 0; i < 256; i++ {
					buffer.Inc(fmt.Sprintf("counter:%03d", i%64), 1)
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

	buffer.Put("alpha", bytes.Repeat([]byte{'x'}, 100))
	afterPut := buffer.EstimatedBytes()

	buffer.Put("alpha", bytes.Repeat([]byte{'y'}, 100))
	afterUpdate := buffer.EstimatedBytes()

	buffer.Put("alpha", bytes.Repeat([]byte{'z'}, 40))
	afterShortUpdate := buffer.EstimatedBytes()

	buffer.Delete("alpha")
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
					buffer.Put(fmt.Sprintf("key:%06d", i), value)
				}
			},
		},
		{
			name: "mixed 3 MiB",
			fill: func(buffer *Buffer) {
				for i := 0; i < 2048; i++ {
					value := bytes.Repeat([]byte{'x'}, 768+i%1024)
					buffer.Put(fmt.Sprintf("key:%06d", i), value)
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
					buffer.Put(key, value)
					if i%4 == 0 {
						buffer.Put(key, updated)
					}
					if i%5 == 0 {
						buffer.Delete(key)
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

	buffer.Put("alpha", []byte("stable"))

	op, ok := buffer.SafeReadOp("alpha")
	if !ok {
		t.Fatal("safe read alpha: not found")
	}
	op.Data[0] = 'X'

	again, ok := buffer.SafeReadOp("alpha")
	if !ok {
		t.Fatal("safe read alpha again: not found")
	}
	if string(again.Data) != "stable" {
		t.Fatalf("safe read exposed shared data: got %q", again.Data)
	}
}

func TestBufferIterOpsOrdered(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put("beta", []byte("2"))
	buffer.Put("alpha", []byte("1"))
	buffer.Delete("gamma")

	var keys []string
	for key := range buffer.IterOps() {
		keys = append(keys, key)
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

func TestBufferFreezeOpsDetachesActiveWrites(t *testing.T) {
	buffer := NewBuffer(42)

	buffer.Put("alpha", []byte("1"))
	buffer.Put("beta", []byte("2"))

	immutable := buffer.FreezeOps()
	if immutable.Len() != 2 {
		t.Fatalf("immutable len = %d, want 2", immutable.Len())
	}
	if buffer.Len() != 0 {
		t.Fatalf("active len after freeze = %d, want 0", buffer.Len())
	}

	buffer.Put("alpha", []byte("new"))
	buffer.Put("gamma", []byte("3"))

	oldAlpha, ok := immutable.ReadOp("alpha")
	if !ok {
		t.Fatal("immutable read alpha: not found")
	}
	if string(oldAlpha.Data) != "1" {
		t.Fatalf("immutable alpha = %q, want old value 1", oldAlpha.Data)
	}

	newAlpha, ok := buffer.ReadOp("alpha")
	if !ok {
		t.Fatal("active read alpha: not found")
	}
	if string(newAlpha.Data) != "new" {
		t.Fatalf("active alpha = %q, want new", newAlpha.Data)
	}

	var immutableKeys []string
	for key := range immutable.IterOps() {
		immutableKeys = append(immutableKeys, key)
	}
	if got, want := fmt.Sprint(immutableKeys), "[alpha beta]"; got != want {
		t.Fatalf("immutable keys = %s, want %s", got, want)
	}

	var activeKeys []string
	for key := range buffer.IterOps() {
		activeKeys = append(activeKeys, key)
	}
	if got, want := fmt.Sprint(activeKeys), "[alpha gamma]"; got != want {
		t.Fatalf("active keys = %s, want %s", got, want)
	}
}

func actualBufferDataBytes(buffer *Buffer) int64 {
	var total int64
	for _, op := range buffer.IterOps() {
		total += int64(len(op.Data))
	}
	return total
}
