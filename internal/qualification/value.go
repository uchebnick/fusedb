package qualification

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	qualificationValueMagic  = uint32(0x46555345) // FUSE
	qualificationValueHeader = 20
)

var (
	errValueTooSmall = errors.New("qualification: value is smaller than integrity header")
	errValueMagic    = errors.New("qualification: invalid value magic")
	errValueKey      = errors.New("qualification: value belongs to another key")
	errValueChecksum = errors.New("qualification: value checksum mismatch")
	qualificationCRC = crc32.MakeTable(crc32.Castagnoli)
)

func buildValue(keyIndex int, generation uint64, size int) []byte {
	value := make([]byte, size)
	binary.LittleEndian.PutUint32(value[0:4], qualificationValueMagic)
	binary.LittleEndian.PutUint32(value[4:8], uint32(keyIndex))
	binary.LittleEndian.PutUint64(value[8:16], generation)
	group := byte((keyIndex / 64) % 251)
	version := byte(generation % 17)
	for i := qualificationValueHeader; i < len(value); i++ {
		value[i] = group
		if i%47 == 0 {
			value[i] = version
		}
	}
	binary.LittleEndian.PutUint32(value[16:20], valueChecksum(value))
	return value
}

func validateValue(value []byte, keyIndex int) error {
	if len(value) < qualificationValueHeader {
		return errValueTooSmall
	}
	if binary.LittleEndian.Uint32(value[0:4]) != qualificationValueMagic {
		return errValueMagic
	}
	if binary.LittleEndian.Uint32(value[4:8]) != uint32(keyIndex) {
		return errValueKey
	}
	if binary.LittleEndian.Uint32(value[16:20]) != valueChecksum(value) {
		return errValueChecksum
	}
	return nil
}

func valueChecksum(value []byte) uint32 {
	checksum := crc32.Update(0, qualificationCRC, value[:16])
	return crc32.Update(checksum, qualificationCRC, value[20:])
}
