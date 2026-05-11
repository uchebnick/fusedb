package value

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecodeBytes(t *testing.T) {
	raw := []byte{0x02, 0x80, 'x'}
	encoded := EncodeBytes(raw)
	raw[0] = 0xff

	kind, err := KindOf(encoded)
	if err != nil {
		t.Fatalf("kind: %v", err)
	}
	if kind != KindBytes {
		t.Fatalf("kind = %d, want bytes", kind)
	}

	decoded, err := DecodeBytes(encoded)
	if err != nil {
		t.Fatalf("decode bytes: %v", err)
	}
	if !bytes.Equal(decoded, []byte{0x02, 0x80, 'x'}) {
		t.Fatalf("decoded = %v", decoded)
	}

	// DecodeBytes returns borrowed slice - modifying it affects the source
	decoded[0] = 0xee
	again, err := DecodeBytes(encoded)
	if err != nil {
		t.Fatalf("decode bytes again: %v", err)
	}
	if again[0] != 0xee {
		t.Fatalf("decode should return borrowed data: got %v, want 0xee", again[0])
	}
}

func TestEncodeDecodeInt64(t *testing.T) {
	for _, want := range []int64{0, 1, -1, 42, -100500, 1 << 40} {
		encoded := EncodeInt64(want)

		kind, err := KindOf(encoded)
		if err != nil {
			t.Fatalf("kind %d: %v", want, err)
		}
		if kind != KindInt64 {
			t.Fatalf("kind = %d, want int64", kind)
		}

		got, err := DecodeInt64(encoded)
		if err != nil {
			t.Fatalf("decode int64 %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("decoded = %d, want %d", got, want)
		}
	}
}

func TestDecodeRejectsWrongKind(t *testing.T) {
	if _, err := DecodeInt64(EncodeBytes([]byte{0x02})); !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("decode int64 from bytes err = %v, want %v", err, ErrKindMismatch)
	}
	if _, err := DecodeBytes(EncodeInt64(10)); !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("decode bytes from int64 err = %v, want %v", err, ErrKindMismatch)
	}
}

func TestDecodeRejectsCorruptData(t *testing.T) {
	if _, err := KindOf(nil); !errors.Is(err, ErrEmptyValue) {
		t.Fatalf("empty kind err = %v, want %v", err, ErrEmptyValue)
	}
	if _, err := KindOf([]byte{99}); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("unknown kind err = %v, want %v", err, ErrUnknownKind)
	}
	if _, err := DecodeInt64([]byte{0x80, byte(KindInt64)}); !errors.Is(err, ErrInvalidInt64) {
		t.Fatalf("bad int err = %v, want %v", err, ErrInvalidInt64)
	}
	if _, err := DecodeInt64([]byte{0x02, 0x00, byte(KindInt64)}); !errors.Is(err, ErrTrailingIntBytes) {
		t.Fatalf("trailing int err = %v, want %v", err, ErrTrailingIntBytes)
	}
}
