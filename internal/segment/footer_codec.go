package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	footerMagic       = "FFTR"
	footerVersion     = 1
	footerSectionSize = 8 + 8
	footerSize        = 4 + 4 + 4 + 3*footerSectionSize + 4
)

var (
	ErrShortFooterBuffer        = errors.New("segment: short footer buffer")
	ErrFooterMagicMismatch      = errors.New("segment: footer magic mismatch")
	ErrUnsupportedFooterVersion = errors.New("segment: unsupported footer version")
	ErrFooterChecksumMismatch   = errors.New("segment: footer checksum mismatch")
)

// MarshalBinary encodes footer into stable on-disk bytes.
func (f Footer) MarshalBinary() ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	buf := make([]byte, footerSize)
	copy(buf[:4], footerMagic)
	binary.LittleEndian.PutUint32(buf[4:8], footerVersion)
	binary.LittleEndian.PutUint32(buf[8:12], f.BlockCount)

	pos := 12
	pos = encodeSection(buf, pos, f.Data)
	pos = encodeSection(buf, pos, f.Bloom)
	pos = encodeSection(buf, pos, f.Index)

	checksum := crc32.ChecksumIEEE(buf[:pos])
	binary.LittleEndian.PutUint32(buf[pos:pos+4], checksum)
	return buf, nil
}

// UnmarshalBinary decodes footer from stable on-disk bytes.
func (f *Footer) UnmarshalBinary(data []byte) error {
	decoded, err := DecodeFooter(data)
	if err != nil {
		return err
	}
	*f = decoded
	return nil
}

// EncodeFooter encodes footer into bytes.
func EncodeFooter(f Footer) ([]byte, error) {
	return f.MarshalBinary()
}

// DecodeFooter decodes footer from bytes.
func DecodeFooter(data []byte) (Footer, error) {
	if len(data) < footerSize {
		return Footer{}, ErrShortFooterBuffer
	}
	if string(data[:4]) != footerMagic {
		return Footer{}, ErrFooterMagicMismatch
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version != footerVersion {
		return Footer{}, fmt.Errorf("%w: %d", ErrUnsupportedFooterVersion, version)
	}

	checksumPos := footerSize - 4
	wantChecksum := binary.LittleEndian.Uint32(data[checksumPos:])
	gotChecksum := crc32.ChecksumIEEE(data[:checksumPos])
	if wantChecksum != gotChecksum {
		return Footer{}, ErrFooterChecksumMismatch
	}

	f := Footer{
		BlockCount: binary.LittleEndian.Uint32(data[8:12]),
	}

	pos := 12
	f.Data = decodeSection(data, pos)
	pos += footerSectionSize
	f.Bloom = decodeSection(data, pos)
	pos += footerSectionSize
	f.Index = decodeSection(data, pos)

	if err := f.Validate(); err != nil {
		return Footer{}, err
	}
	return f, nil
}

func encodeSection(buf []byte, pos int, s Section) int {
	binary.LittleEndian.PutUint64(buf[pos:pos+8], s.Offset)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], s.Length)
	pos += 8
	return pos
}

func decodeSection(buf []byte, pos int) Section {
	return Section{
		Offset: binary.LittleEndian.Uint64(buf[pos : pos+8]),
		Length: binary.LittleEndian.Uint64(buf[pos+8 : pos+16]),
	}
}
