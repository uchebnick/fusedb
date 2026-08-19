//go:build !cgo

package compression

func lz4Available() bool {
	return false
}

func lz4CompressBound(int) int {
	return 0
}

func lz4CompressWithDict([]byte, []byte, []byte, int) int {
	return 0
}

func lz4DecompressWithDict([]byte, []byte, []byte) int {
	return 0
}
