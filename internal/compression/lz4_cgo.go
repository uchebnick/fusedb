//go:build cgo

package compression

/*
#cgo darwin CFLAGS: -I/opt/homebrew/include
#cgo darwin LDFLAGS: -L/opt/homebrew/lib -llz4
#cgo linux LDFLAGS: -llz4
#include <stdlib.h>
#include <lz4.h>

static int fusedb_lz4_compress_dict(
	char* src, int srcSize,
	char* dst, int dstCap,
	char* dict, int dictSize,
	int acceleration
) {
	LZ4_stream_t* stream = LZ4_createStream();
	if (stream == NULL) {
		return 0;
	}
	LZ4_loadDict(stream, dict, dictSize);
	int n = LZ4_compress_fast_continue(stream, src, dst, srcSize, dstCap, acceleration);
	LZ4_freeStream(stream);
	return n;
}
*/
import "C"

import "unsafe"

func lz4Available() bool {
	return true
}

func lz4CompressBound(srcLen int) int {
	return int(C.LZ4_compressBound(C.int(srcLen)))
}

func lz4CompressWithDict(dst, src, dict []byte, acceleration int) int {
	return int(C.fusedb_lz4_compress_dict(
		cBytes(src),
		C.int(len(src)),
		cBytes(dst),
		C.int(len(dst)),
		cBytes(dict),
		C.int(len(dict)),
		C.int(acceleration),
	))
}

func lz4DecompressWithDict(dst, src, dict []byte) int {
	return int(C.LZ4_decompress_safe_usingDict(
		cBytes(src),
		cBytes(dst),
		C.int(len(src)),
		C.int(len(dst)),
		cBytes(dict),
		C.int(len(dict)),
	))
}

func cBytes(b []byte) *C.char {
	if len(b) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}
