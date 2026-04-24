package segment

import (
	"errors"
	"fmt"
)

// CompressionKind describes how all blocks inside one segment are stored.
//
// Compression is segment-wide:
// - either all blocks are stored raw
// - or all blocks are stored compressed with the same strategy
type CompressionKind uint32

const (
	CompressionNone CompressionKind = iota
	CompressionZstdDict
)

var (
	ErrInvalidCompressionKind = errors.New("segment: invalid compression kind")
	ErrMissingDictionaryID    = errors.New("segment: missing dictionary id for compressed segment")
	ErrUnexpectedDictionaryID = errors.New("segment: unexpected dictionary id for raw segment")
)

// Header is creation-time segment metadata stored at file start.
//
// It contains only information known before any block is written.
// Section offsets live in Footer.
type Header struct {
	Version      uint64
	Compression  CompressionKind
	DictionaryID uint32
}

// Compressed reports whether the segment stores compressed blocks.
func (h Header) Compressed() bool {
	return h.Compression != CompressionNone
}

// Validate checks header invariants.
func (h Header) Validate() error {
	switch h.Compression {
	case CompressionNone:
		if h.DictionaryID != 0 {
			return ErrUnexpectedDictionaryID
		}
	case CompressionZstdDict:
		if h.DictionaryID == 0 {
			return ErrMissingDictionaryID
		}
	default:
		return fmt.Errorf("%w: %d", ErrInvalidCompressionKind, h.Compression)
	}
	return nil
}
