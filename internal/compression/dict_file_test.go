package compression

import (
	"bytes"
	"testing"

	"fusedb/internal/disk"
)

func TestDictionaryRoundTrip(t *testing.T) {
	raw, err := TrainDictionary(TrainOptions{
		ID: 1,
		Samples: [][]byte{
			[]byte("tenant=a|region=eu|state=active|count=1"),
			[]byte("tenant=a|region=eu|state=active|count=2"),
			[]byte("tenant=b|region=us|state=active|count=1"),
			[]byte("tenant=b|region=us|state=disabled|count=8"),
		},
	})
	if err != nil {
		t.Fatalf("train dictionary: %v", err)
	}

	dict, err := NewDictionaryLevel(1, raw, 3)
	if err != nil {
		t.Fatalf("new dictionary: %v", err)
	}
	t.Cleanup(func() { _ = dict.Close() })

	fs := disk.NewMemFS()
	name := DictionaryFileName("dicts", dict.ID())
	if err := SaveDictionary(fs, name, dict); err != nil {
		t.Fatalf("save dictionary: %v", err)
	}

	loaded, err := LoadDictionary(fs, name)
	if err != nil {
		t.Fatalf("load dictionary: %v", err)
	}
	t.Cleanup(func() { _ = loaded.Close() })

	if loaded.ID() != dict.ID() {
		t.Fatalf("loaded id = %d, want %d", loaded.ID(), dict.ID())
	}
	if loaded.Level() != dict.Level() {
		t.Fatalf("loaded level = %d, want %d", loaded.Level(), dict.Level())
	}
	if !bytes.Equal(loaded.Raw(), dict.Raw()) {
		t.Fatal("loaded raw dictionary differs")
	}

	payload := []byte("tenant=a|region=eu|state=active|count=99")
	compressed, err := loaded.Compress(payload)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	decoded, err := loaded.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("decoded payload = %q, want %q", decoded, payload)
	}
}
