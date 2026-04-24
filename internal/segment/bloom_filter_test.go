package segment

import (
	"testing"
)

func TestBloomFilterMembership(t *testing.T) {
	filter, err := BuildBloomFilter([][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("delta"),
	}, 0.01)
	if err != nil {
		t.Fatalf("build bloom filter: %v", err)
	}

	for _, key := range [][]byte{[]byte("alpha"), []byte("beta"), []byte("delta")} {
		if !filter.MayContain(key) {
			t.Fatalf("expected key %q to be present", key)
		}
	}
}

func TestBloomFilterRoundTrip(t *testing.T) {
	filter, err := BuildBloomFilter([][]byte{
		[]byte("k1"),
		[]byte("k2"),
		[]byte("k3"),
	}, 0.01)
	if err != nil {
		t.Fatalf("build bloom filter: %v", err)
	}

	data, err := EncodeBloomFilter(filter)
	if err != nil {
		t.Fatalf("encode bloom filter: %v", err)
	}

	decoded, err := DecodeBloomFilter(data)
	if err != nil {
		t.Fatalf("decode bloom filter: %v", err)
	}

	for _, key := range [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")} {
		if !decoded.MayContain(key) {
			t.Fatalf("decoded filter lost key %q", key)
		}
	}
}

func TestBuildBloomFilterForBlock(t *testing.T) {
	block, err := NewBlock([]BlockEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("z"), Value: []byte("3")},
	})
	if err != nil {
		t.Fatalf("new block: %v", err)
	}

	filter, err := BuildBloomFilterForBlock(block, 0.01)
	if err != nil {
		t.Fatalf("build bloom filter for block: %v", err)
	}

	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("z")} {
		if !filter.MayContain(key) {
			t.Fatalf("expected block key %q to be present", key)
		}
	}
	if filter.MayContain([]byte("missing-nearby-key")) {
		t.Log("false positive observed for missing key; acceptable for bloom filter")
	}
}
