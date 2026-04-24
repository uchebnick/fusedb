package compression

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"

	"fusedb/internal/disk"
)

const (
	dictionaryFileMagic   = "FDDC"
	dictionaryFileVersion = 1
	dictionaryHeaderSize  = 4 + 4 + 4 + 4 + 4 + 4
)

var (
	ErrShortDictionaryFile      = errors.New("compression: short dictionary file")
	ErrDictionaryMagicMismatch  = errors.New("compression: dictionary magic mismatch")
	ErrUnsupportedDictionaryFmt = errors.New("compression: unsupported dictionary file version")
	ErrDictionaryChecksum       = errors.New("compression: dictionary checksum mismatch")
	ErrTrailingDictionaryBytes  = errors.New("compression: trailing dictionary bytes")
)

// DictionaryFileName returns a stable file name for one global dictionary
// version. One version is stored in one file.
func DictionaryFileName(dir string, dictID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("dict-%08d.zdict", dictID))
}

// SaveDictionary writes one dictionary version to a file atomically.
func SaveDictionary(fs disk.FS, name string, dict *Dictionary) error {
	data, err := EncodeDictionary(dict)
	if err != nil {
		return err
	}
	return disk.WriteFileAtomically(fs, name, data)
}

// LoadDictionary reads one dictionary version from a file.
func LoadDictionary(fs disk.FS, name string) (*Dictionary, error) {
	data, err := disk.ReadFile(fs, name)
	if err != nil {
		return nil, err
	}
	return DecodeDictionary(data)
}

// EncodeDictionary serializes one dictionary version to bytes.
func EncodeDictionary(dict *Dictionary) ([]byte, error) {
	if dict == nil {
		return nil, ErrNilDictionary
	}

	raw := dict.Raw()
	buf := make([]byte, dictionaryHeaderSize+len(raw))
	copy(buf[:4], dictionaryFileMagic)
	binary.LittleEndian.PutUint32(buf[4:8], dictionaryFileVersion)
	binary.LittleEndian.PutUint32(buf[8:12], dict.ID())
	binary.LittleEndian.PutUint32(buf[12:16], uint32(dict.Level()))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(raw)))
	binary.LittleEndian.PutUint32(buf[20:24], crc32.ChecksumIEEE(raw))
	copy(buf[dictionaryHeaderSize:], raw)
	return buf, nil
}

// DecodeDictionary deserializes one dictionary version from bytes.
func DecodeDictionary(data []byte) (*Dictionary, error) {
	if len(data) < dictionaryHeaderSize {
		return nil, ErrShortDictionaryFile
	}
	if string(data[:4]) != dictionaryFileMagic {
		return nil, ErrDictionaryMagicMismatch
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	if version != dictionaryFileVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedDictionaryFmt, version)
	}

	dictID := binary.LittleEndian.Uint32(data[8:12])
	level := int(binary.LittleEndian.Uint32(data[12:16]))
	rawLen := int(binary.LittleEndian.Uint32(data[16:20]))
	checksum := binary.LittleEndian.Uint32(data[20:24])

	if len(data) < dictionaryHeaderSize+rawLen {
		return nil, ErrShortDictionaryFile
	}
	if len(data) != dictionaryHeaderSize+rawLen {
		return nil, ErrTrailingDictionaryBytes
	}

	raw := append([]byte(nil), data[dictionaryHeaderSize:]...)
	if crc32.ChecksumIEEE(raw) != checksum {
		return nil, ErrDictionaryChecksum
	}

	return NewDictionaryLevel(dictID, raw, level)
}
