package value

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type Kind byte

const (
	KindBytes Kind = 1
	KindInt64 Kind = 2
)

var (
	ErrEmptyValue       = errors.New("value: empty encoded value")
	ErrUnknownKind      = errors.New("value: unknown kind")
	ErrKindMismatch     = errors.New("value: kind mismatch")
	ErrInvalidInt64     = errors.New("value: invalid int64 payload")
	ErrTrailingIntBytes = errors.New("value: trailing int64 bytes")
)

// KindOf returns the type tag stored in encoded value bytes.
func KindOf(data []byte) (Kind, error) {
	if len(data) == 0 {
		return 0, ErrEmptyValue
	}

	kind := Kind(data[len(data)-1])
	switch kind {
	case KindBytes, KindInt64:
		return kind, nil
	default:
		return 0, fmt.Errorf("%w: %d", ErrUnknownKind, kind)
	}
}

// EncodeBytes wraps raw bytes with a value kind tag.
//
// EncodeBytes never writes into the caller's backing array: appending the tag
// in place would corrupt bytes past len(data) whenever the caller passed a
// slice with spare capacity. The returned slice is owned by the caller and is
// safe to hand to an ownership-taking constructor such as ops.NewPutOwned.
func EncodeBytes(data []byte) []byte {
	encoded := make([]byte, len(data)+1)
	copy(encoded, data)
	encoded[len(data)] = byte(KindBytes)

	return encoded
}

// DecodeBytes returns the tagged bytes value payload.
func DecodeBytes(data []byte) ([]byte, error) {
	kind, err := KindOf(data)
	if err != nil {
		return nil, err
	}
	if kind != KindBytes {
		return nil, fmt.Errorf("%w: got %d want %d", ErrKindMismatch, kind, KindBytes)
	}
	return data[:len(data)-1], nil
}

// EncodeInt64 wraps an int64 varint payload with a value kind tag at the end.
func EncodeInt64(v int64) []byte {
	var buf [binary.MaxVarintLen64 + 1]byte
	n := binary.PutVarint(buf[:], v)
	buf[n] = byte(KindInt64)
	return buf[:n+1]
}

// DecodeInt64 decodes a tagged int64 value.
func DecodeInt64(data []byte) (int64, error) {
	kind, err := KindOf(data)
	if err != nil {
		return 0, err
	}
	if kind != KindInt64 {
		return 0, fmt.Errorf("%w: got %d want %d", ErrKindMismatch, kind, KindInt64)
	}

	value, n := binary.Varint(data[:len(data)-1])
	if n <= 0 {
		return 0, ErrInvalidInt64
	}
	if n+1 != len(data) {
		return 0, ErrTrailingIntBytes
	}
	return value, nil
}
