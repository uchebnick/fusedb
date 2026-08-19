package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/uchebnick/fusedb/internal/disk"
	"github.com/uchebnick/fusedb/internal/limits"
)

const readerBufferSize = 64 << 10

// Result summarizes one full pass over a log file.
type Result struct {
	// BaseSeq is the sequence number the file header declares for its first
	// record.
	BaseSeq uint64
	// LastSeq is the sequence number of the last intact record, or BaseSeq-1
	// when the file holds no records.
	LastSeq uint64
	// Count is the number of intact records read.
	Count int
	// TruncatedTail reports that the file ended with a partially written or
	// unverifiable trailing record. This is the expected state after a crash,
	// not corruption.
	TruncatedTail bool
	// ValidEnd is the file offset just past the last intact record. Bytes at or
	// after this offset are the truncated tail and may be overwritten.
	ValidEnd int64
}

// Cursor walks records of a log file from the beginning.
//
// A cursor stops at the first record it cannot verify. If that record is the
// trailing one, iteration ends cleanly with TruncatedTail reporting true and
// Err reporting nil. If it sits earlier in the file, Err reports
// ErrCorruptRecord or ErrSequenceGap.
type Cursor struct {
	file disk.File
	br   *bufio.Reader

	size     int64
	offset   int64
	baseSeq  uint64
	nextSeq  uint64
	count    int
	validEnd int64

	scratch   []byte
	record    Record
	err       error
	done      bool
	truncated bool
}

// NewCursor opens path for reading and validates the file header.
func NewCursor(fs disk.FS, path string) (*Cursor, error) {
	if fs == nil {
		fs = disk.DefaultFS
	}
	if path == "" {
		return nil, ErrNoPath
	}

	file, err := fs.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() < fileHeaderSize {
		_ = file.Close()
		return nil, ErrShortWALFile
	}

	br := bufio.NewReaderSize(file, readerBufferSize)
	header := make([]byte, fileHeaderSize)
	if _, err := io.ReadFull(br, header); err != nil {
		_ = file.Close()
		return nil, ErrShortWALFile
	}
	baseSeq, err := decodeFileHeader(header)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &Cursor{
		file:     file,
		br:       br,
		size:     info.Size(),
		offset:   fileHeaderSize,
		baseSeq:  baseSeq,
		nextSeq:  baseSeq,
		validEnd: fileHeaderSize,
		scratch:  make([]byte, 0, maxRecordHeaderSize),
	}, nil
}

// BaseSeq returns the sequence number of the first record the file can hold.
func (c *Cursor) BaseSeq() uint64 { return c.baseSeq }

// Record returns the record decoded by the last successful Next.
func (c *Cursor) Record() Record { return c.record }

// Err returns the first fatal decoding error, if any.
//
// A truncated tail is not an error; check TruncatedTail for that.
func (c *Cursor) Err() error { return c.err }

// TruncatedTail reports whether iteration stopped on a partially written or
// unverifiable trailing record.
func (c *Cursor) TruncatedTail() bool { return c.truncated }

// Result returns the summary of the traversal performed so far.
func (c *Cursor) Result() Result {
	return Result{
		BaseSeq:       c.baseSeq,
		LastSeq:       c.nextSeq - 1,
		Count:         c.count,
		TruncatedTail: c.truncated,
		ValidEnd:      c.validEnd,
	}
}

// Close releases the underlying file.
func (c *Cursor) Close() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	return err
}

// Next advances to the next intact record and reports whether one was decoded.
func (c *Cursor) Next() bool {
	if c.done || c.err != nil {
		return false
	}
	if c.offset >= c.size {
		c.done = true
		return false
	}

	record, consumed, err := c.readRecord()
	if err != nil {
		c.done = true
		if errors.Is(err, errTornTail) {
			c.truncated = true
			return false
		}
		c.err = err
		return false
	}

	c.offset += consumed
	c.validEnd = c.offset
	c.nextSeq = record.Seq + 1
	c.count++
	c.record = record
	return true
}

// errTornTail marks a record that could not be verified but occupies the end of
// the file, which is the signature of a write interrupted by a crash.
var errTornTail = errors.New("wal: torn tail")

func (c *Cursor) readRecord() (Record, int64, error) {
	remaining := c.size - c.offset
	c.scratch = c.scratch[:0]

	kind, err := c.readByte()
	if err != nil {
		return Record{}, 0, errTornTail
	}
	opKind, ok := opKindOf(kind)
	if !ok {
		// The first byte of a record is never partially written into an
		// existing valid position, so an unknown kind is real corruption.
		return Record{}, 0, fmt.Errorf("%w: unknown kind %d at offset %d", ErrCorruptRecord, kind, c.offset)
	}

	seq, err := binary.ReadUvarint(c)
	if err != nil {
		return Record{}, 0, errTornTail
	}
	keyLen, err := binary.ReadUvarint(c)
	if err != nil {
		return Record{}, 0, errTornTail
	}
	payloadLen, err := binary.ReadUvarint(c)
	if err != nil {
		return Record{}, 0, errTornTail
	}

	headerLen := int64(len(c.scratch))
	if keyLen > limits.MaxKeyBytes || payloadLen > limits.MaxWALPayloadBytes {
		return Record{}, 0, fmt.Errorf("%w: oversized record at offset %d", ErrCorruptRecord, c.offset)
	}
	if keyLen > uint64(remaining) || payloadLen > uint64(remaining) {
		return Record{}, 0, errTornTail
	}
	total := headerLen + int64(keyLen) + int64(payloadLen) + recordChecksumSize
	if total > remaining {
		return Record{}, 0, errTornTail
	}
	atFileEnd := total == remaining

	body := make([]byte, int(keyLen)+int(payloadLen)+recordChecksumSize)
	if _, err := io.ReadFull(c.br, body); err != nil {
		return Record{}, 0, errTornTail
	}

	dataEnd := int(keyLen) + int(payloadLen)
	want := binary.LittleEndian.Uint32(body[dataEnd:])
	got := crc32.Update(crc32.Update(0, crcTable, c.scratch), crcTable, body[:dataEnd])
	if got != want {
		if atFileEnd {
			return Record{}, 0, errTornTail
		}
		return Record{}, 0, fmt.Errorf("%w: checksum mismatch at offset %d", ErrCorruptRecord, c.offset)
	}
	if seq != c.nextSeq {
		if atFileEnd {
			return Record{}, 0, errTornTail
		}
		return Record{}, 0, fmt.Errorf("%w: want seq %d, got %d at offset %d", ErrSequenceGap, c.nextSeq, seq, c.offset)
	}

	record := Record{
		Kind:    opKind,
		Seq:     seq,
		Key:     body[:keyLen:keyLen],
		Payload: body[keyLen:dataEnd:dataEnd],
	}
	return record, total, nil
}

// ReadByte implements io.ByteReader so uvarint fields feed the running record
// checksum as they are consumed.
func (c *Cursor) ReadByte() (byte, error) {
	return c.readByte()
}

func (c *Cursor) readByte() (byte, error) {
	b, err := c.br.ReadByte()
	if err != nil {
		return 0, err
	}
	c.scratch = append(c.scratch, b)
	return b, nil
}

// Iterate replays every intact record of path in order.
//
// The callback may return an error to stop iteration early; that error is
// returned unchanged. A truncated trailing record is reported through the
// Result rather than as an error.
func Iterate(fs disk.FS, path string, fn func(Record) error) (Result, error) {
	cursor, err := NewCursor(fs, path)
	if err != nil {
		return Result{}, err
	}
	// Iterate only reads, so a failure to close the file cannot invalidate the
	// records already decoded; the traversal result stands on its own.
	defer func() { _ = cursor.Close() }()

	for cursor.Next() {
		if fn == nil {
			continue
		}
		if err := fn(cursor.Record()); err != nil {
			return cursor.Result(), err
		}
	}
	return cursor.Result(), cursor.Err()
}

// ReadAll returns every intact record of path together with the traversal
// summary.
func ReadAll(fs disk.FS, path string) ([]Record, Result, error) {
	var records []Record
	result, err := Iterate(fs, path, func(record Record) error {
		records = append(records, record)
		return nil
	})
	return records, result, err
}
