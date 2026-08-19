package disk

import "testing"

type testIOObserver struct {
	read  int
	write int
}

func (o *testIOObserver) AddDiskReadBytes(n int)  { o.read += n }
func (o *testIOObserver) AddDiskWriteBytes(n int) { o.write += n }

func TestMeterCountsPayloadBytes(t *testing.T) {
	observer := &testIOObserver{}
	fs := Meter(NewMemFS(), observer)
	f, err := fs.Create("data")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("ef"), 4); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if _, err := f.ReadAt(buf, 1); err != nil {
		t.Fatal(err)
	}
	if observer.write != 6 || observer.read != 3 {
		t.Fatalf("observed read=%d write=%d", observer.read, observer.write)
	}
}
