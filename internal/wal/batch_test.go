package wal

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/ops"
)

func TestBatchRoundTripAsSingleWALRecord(t *testing.T) {
	var delta [binary.MaxVarintLen64]byte
	deltaBytes := delta[:binary.PutVarint(delta[:], 7)]
	want := []BatchMutation{
		{Kind: ops.OpPut, Key: []byte("like/post-1/user-7"), Payload: []byte("1")},
		{Kind: ops.OpInc, Key: []byte("likes/post-1"), Payload: deltaBytes},
		{Kind: ops.OpDelete, Key: []byte("pending/post-1/user-7")},
	}

	fs := disk.NewMemFS()
	w := newTestWAL(t, fs, true)
	seq, payloadBytes, err := w.AppendBatch(want)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 || payloadBytes == 0 {
		t.Fatalf("append = (seq=%d, bytes=%d)", seq, payloadBytes)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	records, result, err := ReadAll(fs, testPath)
	if err != nil {
		t.Fatal(err)
	}
	if result.Count != 1 || len(records) != 1 || records[0].Kind != ops.OpBatch || len(records[0].Key) != 0 {
		t.Fatalf("records=%+v result=%+v", records, result)
	}
	got, err := DecodeBatch(records[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d mutations, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].Kind != want[index].Kind || string(got[index].Key) != string(want[index].Key) ||
			string(got[index].Payload) != string(want[index].Payload) {
			t.Fatalf("mutation %d = %+v, want %+v", index, got[index], want[index])
		}
	}
}

func TestDecodeBatchRejectsMalformedPayloads(t *testing.T) {
	valid, err := EncodeBatch([]BatchMutation{{Kind: ops.OpPut, Key: []byte("key"), Payload: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		nil,
		{99, 1},
		valid[:len(valid)-1],
		append(append([]byte(nil), valid...), 0),
		{batchCodecVersion, 1, byte(ops.OpDelete), 1, 1, 'k', 'x'},
	}
	for index, payload := range cases {
		if _, err := DecodeBatch(payload); !errors.Is(err, ErrInvalidBatch) {
			t.Fatalf("case %d error = %v, want ErrInvalidBatch", index, err)
		}
	}
}

func FuzzDecodeBatchNeverPanics(f *testing.F) {
	valid, _ := EncodeBatch([]BatchMutation{{Kind: ops.OpInc, Key: []byte("counter"), Payload: []byte{2}}})
	f.Add(valid)
	f.Add([]byte{batchCodecVersion})
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = DecodeBatch(payload)
	})
}
