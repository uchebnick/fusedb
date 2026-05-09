package value

import (
	"bytes"
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

	kind := Kind(data[0])
	switch kind {
	case KindBytes, KindInt64:
		return kind, nil
	default:
		return 0, fmt.Errorf("%w: %d", ErrUnknownKind, kind)
	}
}

// EncodeBytes wraps raw bytes with a value kind tag.
func EncodeBytes(data []byte) []byte {
	out := make([]byte, 1+len(data))
	out[0] = byte(KindBytes)
	copy(out[1:], data)
	return out
}

// DecodeBytes returns an owned copy of a tagged bytes value payload.
func DecodeBytes(data []byte) ([]byte, error) {
	kind, err := KindOf(data)
	if err != nil {
		return nil, err
	}
	if kind != KindBytes {
		return nil, fmt.Errorf("%w: got %d want %d", ErrKindMismatch, kind, KindBytes)
	}
	return bytes.Clone(data[1:]), nil
}

// EncodeInt64 wraps an int64 varint payload with a value kind tag.
func EncodeInt64(v int64) []byte {
	var buf [1 + binary.MaxVarintLen64]byte
	buf[0] = byte(KindInt64)
	n := binary.PutVarint(buf[1:], v)

	out := make([]byte, 1+n)
	copy(out, buf[:1+n])
	return out
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

	value, n := binary.Varint(data[1:])
	if n <= 0 {
		return 0, ErrInvalidInt64
	}
	if 1+n != len(data) {
		return 0, ErrTrailingIntBytes
	}
	return value, nil
}
