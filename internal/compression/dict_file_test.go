package compression

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/uchebnick/fusedb/internal/disk"
)

func TestDictionaryDecoderRejectsOversizedLengthBeforePayload(t *testing.T) {
	data := make([]byte, dictionaryHeaderSize)
	copy(data[:4], dictionaryFileMagic)
	binary.LittleEndian.PutUint32(data[4:8], dictionaryFileVersion)
	binary.LittleEndian.PutUint32(data[8:12], 1)
	binary.LittleEndian.PutUint32(data[12:16], 1)
	binary.LittleEndian.PutUint32(data[16:20], MaxDictionarySize+1)
	if _, err := DecodeDictionary(data); !errors.Is(err, ErrDictionaryTooLarge) {
		t.Fatalf("DecodeDictionary error = %v, want ErrDictionaryTooLarge", err)
	}
}

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
	dpb, err := loaded.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(dpb.Data, payload) {
		t.Fatalf("decoded payload = %q, want %q", dpb.Data, payload)
	}
	dpb.Release()
}

func FuzzDecodeDictionaryNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, dictionaryHeaderSize))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		dict, _ := DecodeDictionary(data)
		if dict != nil {
			_ = dict.Close()
		}
	})
}
