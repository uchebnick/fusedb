package wal

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/uchebnick/fusedb/internal/limits"
	"github.com/uchebnick/fusedb/internal/ops"
)

const batchCodecVersion byte = 1

var ErrInvalidBatch = errors.New("wal: invalid batch")

// BatchMutation is one key mutation carried by an atomic WAL record. Payload
// is a raw byte value for OpPut, a signed varint for OpInc, and empty for
// OpDelete.
type BatchMutation struct {
	Kind    ops.OpKind
	Key     []byte
	Payload []byte
}

// EncodeBatch returns the bounded, self-delimiting payload stored inside one
// checksummed WAL record.
func EncodeBatch(mutations []BatchMutation) ([]byte, error) {
	if len(mutations) == 0 || len(mutations) > limits.MaxBatchMutations {
		return nil, fmt.Errorf("%w: mutation count %d", ErrInvalidBatch, len(mutations))
	}

	encoded := make([]byte, 0, 64)
	encoded = append(encoded, batchCodecVersion)
	encoded = binary.AppendUvarint(encoded, uint64(len(mutations)))
	for index, mutation := range mutations {
		if err := validateBatchMutation(mutation); err != nil {
			return nil, fmt.Errorf("%w: mutation %d: %v", ErrInvalidBatch, index, err)
		}
		encoded = append(encoded, byte(mutation.Kind))
		encoded = binary.AppendUvarint(encoded, uint64(len(mutation.Key)))
		encoded = binary.AppendUvarint(encoded, uint64(len(mutation.Payload)))
		encoded = append(encoded, mutation.Key...)
		encoded = append(encoded, mutation.Payload...)
		if len(encoded) > limits.MaxWALPayloadBytes {
			return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalidBatch, limits.MaxWALPayloadBytes)
		}
	}
	return encoded, nil
}

// DecodeBatch validates a persisted batch before returning views into payload.
// The caller owns payload for as long as it uses the returned mutations.
func DecodeBatch(payload []byte) ([]BatchMutation, error) {
	if len(payload) < 2 || payload[0] != batchCodecVersion {
		return nil, fmt.Errorf("%w: unsupported codec", ErrInvalidBatch)
	}
	position := 1
	count, ok := readBatchUvarint(payload, &position)
	if !ok || count == 0 || count > limits.MaxBatchMutations {
		return nil, fmt.Errorf("%w: invalid mutation count", ErrInvalidBatch)
	}

	mutations := make([]BatchMutation, 0, int(count))
	for index := uint64(0); index < count; index++ {
		if position >= len(payload) {
			return nil, fmt.Errorf("%w: truncated mutation %d", ErrInvalidBatch, index)
		}
		kind := ops.OpKind(payload[position])
		position++
		keyLen, keyOK := readBatchUvarint(payload, &position)
		valueLen, valueOK := readBatchUvarint(payload, &position)
		if !keyOK || !valueOK || keyLen == 0 || keyLen > limits.MaxKeyBytes || valueLen > limits.MaxValueBytes {
			return nil, fmt.Errorf("%w: invalid lengths at mutation %d", ErrInvalidBatch, index)
		}
		remaining := uint64(len(payload) - position)
		if keyLen > remaining || valueLen > remaining-keyLen {
			return nil, fmt.Errorf("%w: truncated body at mutation %d", ErrInvalidBatch, index)
		}
		keyEnd := position + int(keyLen)
		valueEnd := keyEnd + int(valueLen)
		mutation := BatchMutation{
			Kind:    kind,
			Key:     payload[position:keyEnd:keyEnd],
			Payload: payload[keyEnd:valueEnd:valueEnd],
		}
		if err := validateBatchMutation(mutation); err != nil {
			return nil, fmt.Errorf("%w: mutation %d: %v", ErrInvalidBatch, index, err)
		}
		mutations = append(mutations, mutation)
		position = valueEnd
	}
	if position != len(payload) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrInvalidBatch)
	}
	return mutations, nil
}

func validateBatchMutation(mutation BatchMutation) error {
	if len(mutation.Key) == 0 || len(mutation.Key) > limits.MaxKeyBytes {
		return errors.New("invalid key length")
	}
	switch mutation.Kind {
	case ops.OpPut:
		if len(mutation.Payload) > limits.MaxValueBytes {
			return errors.New("value too large")
		}
	case ops.OpDelete:
		if len(mutation.Payload) != 0 {
			return errors.New("delete has payload")
		}
	case ops.OpInc:
		_, size := binary.Varint(mutation.Payload)
		if size <= 0 || size != len(mutation.Payload) {
			return errors.New("invalid increment")
		}
	default:
		return errors.New("unsupported operation")
	}
	return nil
}

func readBatchUvarint(payload []byte, position *int) (uint64, bool) {
	if position == nil || *position >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*position:])
	if size <= 0 {
		return 0, false
	}
	*position += size
	return value, true
}
