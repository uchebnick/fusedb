package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestDecodersRejectImpossibleCountsBeforeAllocation(t *testing.T) {
	block := make([]byte, rawBlockHeaderSize+rawBlockChecksumSize)
	copy(block[:4], rawBlockMagic)
	binary.LittleEndian.PutUint32(block[4:8], rawBlockVersion)
	binary.LittleEndian.PutUint32(block[8:12], ^uint32(0))
	if _, err := NewBlockView(block); !errors.Is(err, ErrCorruptBlockOffsets) {
		t.Fatalf("block error = %v, want ErrCorruptBlockOffsets", err)
	}

	index := make([]byte, indexHeaderSize)
	binary.LittleEndian.PutUint32(index, ^uint32(0))
	if _, err := DecodeIndex(index); !errors.Is(err, ErrCorruptIndex) {
		t.Fatalf("index error = %v, want ErrCorruptIndex", err)
	}
}

func TestBlockRawRoundTrip(t *testing.T) {
	block, err := NewBlock([]BlockEntry{
		{Key: []byte("alpha"), Value: []byte("1")},
		{Key: []byte("beta"), Value: []byte("2")},
		{Key: []byte("delta"), Value: []byte("3")},
	})
	if err != nil {
		t.Fatalf("new block: %v", err)
	}

	encoded, err := EncodeBlock(block)
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}
	decoded, err := DecodeBlock(encoded)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}

	if decoded.Len() != block.Len() {
		t.Fatalf("decoded len = %d, want %d", decoded.Len(), block.Len())
	}
	value, ok := decoded.Find([]byte("beta"))
	if !ok {
		t.Fatal("expected beta value")
	}
	if !bytes.Equal(value, []byte("2")) {
		t.Fatalf("beta value = %q, want %q", value, []byte("2"))
	}

	view, err := NewBlockView(encoded)
	if err != nil {
		t.Fatalf("new block view: %v", err)
	}
	value, ok, err = view.Find([]byte("delta"))
	if err != nil {
		t.Fatalf("view find delta: %v", err)
	}
	if !ok {
		t.Fatal("expected delta value")
	}
	if !bytes.Equal(value, []byte("3")) {
		t.Fatalf("delta value = %q, want %q", value, []byte("3"))
	}
}

func TestEncodeBlocksBuildsIndex(t *testing.T) {
	block1, err := NewBlock([]BlockEntry{
		{Key: []byte("a"), Value: []byte("tenant=a|region=eu|kind=session|state=active|user=1|bucket=1")},
		{Key: []byte("c"), Value: []byte("tenant=a|region=eu|kind=session|state=active|user=2|bucket=2")},
	})
	if err != nil {
		t.Fatalf("new block1: %v", err)
	}
	block2, err := NewBlock([]BlockEntry{
		{Key: []byte("d"), Value: []byte("tenant=b|region=us|kind=session|state=disabled|user=3|bucket=1")},
		{Key: []byte("z"), Value: []byte("tenant=b|region=us|kind=session|state=active|user=4|bucket=2")},
	})
	if err != nil {
		t.Fatalf("new block2: %v", err)
	}

	blob, idx, err := EncodeBlocks([]Block{block1, block2})
	if err != nil {
		t.Fatalf("encode blocks: %v", err)
	}

	entry, _, ok := idx.FindBlock([]byte("e"))
	if !ok {
		t.Fatal("expected index hit for key e")
	}

	decoded, err := DecodeIndexedBlock(blob, entry)
	if err != nil {
		t.Fatalf("decode indexed block: %v", err)
	}

	value, ok := decoded.Find([]byte("z"))
	if !ok {
		t.Fatal("expected z value")
	}
	if !bytes.Equal(value, []byte("tenant=b|region=us|kind=session|state=active|user=4|bucket=2")) {
		t.Fatalf("z value = %q", value)
	}
}

func FuzzSegmentMetadataDecodersNeverPanic(f *testing.F) {
	validBlock, err := NewBlock([]BlockEntry{{Key: []byte("key"), Value: []byte("value")}})
	if err != nil {
		f.Fatal(err)
	}
	encodedBlock, err := validBlock.MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	validIndex, err := NewIndex([]BlockIndexEntry{{Separator: []byte("key"), Offset: 0, Length: uint32(len(encodedBlock))}})
	if err != nil {
		f.Fatal(err)
	}
	encodedIndex, err := validIndex.MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encodedBlock)
	f.Add(encodedIndex)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		_, _ = NewBlockView(data)
		_, _ = DecodeBlock(data)
		_, _ = DecodeIndex(data)
		_, _ = DecodeBloomFilter(data)
		_, _ = DecodeHeader(data)
		_, _ = DecodeFooter(data)
	})
}
