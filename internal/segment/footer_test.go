package segment

import "testing"

func TestFooterRoundTrip(t *testing.T) {
	footer := Footer{
		BlockCount: 4,
		Data:       Section{Offset: 28, Length: 4096},
		Bloom:      Section{Offset: 4124, Length: 256},
		Index:      Section{Offset: 4380, Length: 128},
	}

	data, err := EncodeFooter(footer)
	if err != nil {
		t.Fatalf("encode footer: %v", err)
	}

	decoded, err := DecodeFooter(data)
	if err != nil {
		t.Fatalf("decode footer: %v", err)
	}

	if decoded.BlockCount != footer.BlockCount {
		t.Fatalf("block count = %d, want %d", decoded.BlockCount, footer.BlockCount)
	}
	if decoded.Data != footer.Data || decoded.Bloom != footer.Bloom || decoded.Index != footer.Index {
		t.Fatalf("decoded sections differ: %#v", decoded)
	}
}
