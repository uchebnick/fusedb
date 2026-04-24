package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	headerMagic   = "FHDR"
	headerVersion = 3
	headerSize    = 4 + 4 + 8 + 4 + 4 + 4
)

var (
	ErrShortHeaderBuffer        = errors.New("segment: short header buffer")
	ErrHeaderMagicMismatch      = errors.New("segment: header magic mismatch")
	ErrUnsupportedHeaderVersion = errors.New("segment: unsupported header version")
	ErrHeaderChecksumMismatch   = errors.New("segment: header checksum mismatch")
)

// MarshalBinary encodes header into stable on-disk bytes.
func (h Header) MarshalBinary() ([]byte, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}

	buf := make([]byte, headerSize)
	copy(buf[:4], headerMagic)
	binary.LittleEndian.PutUint32(buf[4:8], headerVersion)
	binary.LittleEndian.PutUint64(buf[8:16], h.Version)
	binary.LittleEndian.PutUint32(buf[16:20], uint32(h.Compression))
	binary.LittleEndian.PutUint32(buf[20:24], h.DictionaryID)

	checksum := crc32.ChecksumIEEE(buf[:24])
	binary.LittleEndian.PutUint32(buf[24:28], checksum)
	return buf, nil
}

// UnmarshalBinary decodes header from stable on-disk bytes.
func (h *Header) UnmarshalBinary(data []byte) error {
	decoded, err := DecodeHeader(data)
	if err != nil {
		return err
	}
	*h = decoded
	return nil
}

// EncodeHeader encodes header into bytes.
func EncodeHeader(h Header) ([]byte, error) {
	return h.MarshalBinary()
}

// DecodeHeader decodes header from bytes.
func DecodeHeader(data []byte) (Header, error) {
	if len(data) < headerSize {
		return Header{}, ErrShortHeaderBuffer
	}
	if string(data[:4]) != headerMagic {
		return Header{}, ErrHeaderMagicMismatch
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version != headerVersion {
		return Header{}, fmt.Errorf("%w: %d", ErrUnsupportedHeaderVersion, version)
	}

	wantChecksum := binary.LittleEndian.Uint32(data[24:28])
	gotChecksum := crc32.ChecksumIEEE(data[:24])
	if wantChecksum != gotChecksum {
		return Header{}, ErrHeaderChecksumMismatch
	}

	h := Header{
		Version:      binary.LittleEndian.Uint64(data[8:16]),
		Compression:  CompressionKind(binary.LittleEndian.Uint32(data[16:20])),
		DictionaryID: binary.LittleEndian.Uint32(data[20:24]),
	}
	if err := h.Validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}
