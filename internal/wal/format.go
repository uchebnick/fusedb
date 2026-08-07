// Package wal implements the FuseDB write-ahead log.
//
// The log is an append-only file of self-describing records. Each record
// carries a WAL-assigned sequence number and a checksum, so a log written by a
// process that was killed mid-write can be replayed up to the last intact
// record.
//
// All filesystem access goes through disk.FS so the log can be exercised on
// disk.MemFS in tests.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"github.com/uchebnick/fusedb/internal/ops"
)

const (
	fileMagic   = "FWAL"
	fileVersion = 1

	// File header: magic[4] | version[4] | baseSeq[8] | crc32[4].
	fileHeaderSize = 4 + 4 + 8 + 4

	// recordChecksumSize is the trailing crc32 of every record.
	recordChecksumSize = 4

	// maxRecordHeaderSize bounds kind[1] plus three uvarints.
	maxRecordHeaderSize = 1 + 3*binary.MaxVarintLen64
)

// On-disk record kinds.
//
// Values start at 1 so that a zero byte is never a valid record start; a run of
// zeros in a torn or preallocated region is rejected instead of decoding into a
// plausible record.
const (
	recordKindPut    byte = 1
	recordKindDelete byte = 2
	recordKindInc    byte = 3
)

// crcTable is the Castagnoli crc32 table used for record and header checksums.
//
// Castagnoli is used here rather than the IEEE polynomial found elsewhere in
// the codebase because it has hardware support on amd64/arm64 and the WAL
// checksums every single mutation on the write path.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrClosed is returned by operations on a closed WAL.
	ErrClosed = errors.New("wal: closed")
	// ErrRecordTooLarge is returned when a key or payload exceeds 4 GiB.
	ErrRecordTooLarge = errors.New("wal: record too large")
	// ErrInvalidOpKind is returned for an operation kind the log cannot encode.
	ErrInvalidOpKind = errors.New("wal: invalid operation kind")
	// ErrNoPath is returned when opening a WAL without a path.
	ErrNoPath = errors.New("wal: empty path")

	// ErrShortWALFile is returned when the file is too small to hold a header.
	ErrShortWALFile = errors.New("wal: short wal file")
	// ErrMagicMismatch is returned when the file header magic does not match.
	ErrMagicMismatch = errors.New("wal: magic mismatch")
	// ErrUnsupportedVersion is returned for an unknown log format version.
	ErrUnsupportedVersion = errors.New("wal: unsupported format version")
	// ErrHeaderChecksum is returned when the file header checksum does not match.
	ErrHeaderChecksum = errors.New("wal: header checksum mismatch")

	// ErrCorruptRecord is returned when a record inside the file, that is not
	// the trailing one, fails checksum or structural validation. Unlike a torn
	// tail this is real corruption and is never silently ignored.
	ErrCorruptRecord = errors.New("wal: corrupt record")
	// ErrSequenceGap is returned when record sequence numbers are not contiguous.
	ErrSequenceGap = errors.New("wal: sequence gap")
)

// Record is one decoded log entry.
//
// Key and Payload are owned by the caller and are not reused between records.
type Record struct {
	Kind    ops.OpKind
	Seq     uint64
	Key     []byte
	Payload []byte
}

// Op converts the record payload back into an ops.Op.
func (r Record) Op() ops.Op {
	return ops.Op{Kind: r.Kind, Data: r.Payload}
}

func encodeFileHeader(baseSeq uint64) []byte {
	buf := make([]byte, fileHeaderSize)
	copy(buf[:4], fileMagic)
	binary.LittleEndian.PutUint32(buf[4:8], fileVersion)
	binary.LittleEndian.PutUint64(buf[8:16], baseSeq)
	binary.LittleEndian.PutUint32(buf[16:20], crc32.Checksum(buf[:16], crcTable))
	return buf
}

func decodeFileHeader(buf []byte) (uint64, error) {
	if len(buf) < fileHeaderSize {
		return 0, ErrShortWALFile
	}
	if string(buf[:4]) != fileMagic {
		return 0, ErrMagicMismatch
	}
	if binary.LittleEndian.Uint32(buf[4:8]) != fileVersion {
		return 0, ErrUnsupportedVersion
	}
	if crc32.Checksum(buf[:16], crcTable) != binary.LittleEndian.Uint32(buf[16:20]) {
		return 0, ErrHeaderChecksum
	}
	return binary.LittleEndian.Uint64(buf[8:16]), nil
}

// appendRecord encodes one record onto dst and returns the grown slice.
//
// Layout, in order:
//
//	kind      1 byte
//	seq       uvarint
//	keyLen    uvarint
//	payloadLen uvarint
//	key       keyLen bytes
//	payload   payloadLen bytes
//	crc32c    4 bytes, little endian, over every byte above
//
// Lengths and the sequence number are varint encoded: sequence numbers start
// small and the common key is far below 128 bytes, so a typical record spends
// 3 header bytes instead of the 20 a fixed-width layout would need. The record
// end is still unambiguous because the decoder learns both lengths before it
// reads any variable-size field.
func appendRecord(dst []byte, kind byte, seq uint64, key, payload []byte) []byte {
	start := len(dst)
	dst = append(dst, kind)
	dst = binary.AppendUvarint(dst, seq)
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	dst = append(dst, key...)
	dst = append(dst, payload...)
	dst = binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], crcTable))
	return dst
}

func recordKindOf(kind ops.OpKind) (byte, error) {
	switch kind {
	case ops.OpPut:
		return recordKindPut, nil
	case ops.OpDelete:
		return recordKindDelete, nil
	case ops.OpInc:
		return recordKindInc, nil
	default:
		return 0, ErrInvalidOpKind
	}
}

func opKindOf(kind byte) (ops.OpKind, bool) {
	switch kind {
	case recordKindPut:
		return ops.OpPut, true
	case recordKindDelete:
		return ops.OpDelete, true
	case recordKindInc:
		return ops.OpInc, true
	default:
		return 0, false
	}
}

func lengthFits(n int) bool {
	return n >= 0 && uint64(n) <= math.MaxUint32
}
