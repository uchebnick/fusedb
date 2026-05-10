package oneleafdbbench

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"

	"fusedb/internal/compression"
	onedb "fusedb/internal/oneleafdb"
	"fusedb/internal/segment"
	"fusedb/internal/value"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/s2"
)

const compressionProbeJSONL = "../../internal/compression/kv_dict_samples_50k.jsonl"

var compressionProbe struct {
	once sync.Once
	data compressionProbeData
	err  error
}

type compressionProbeData struct {
	raw      [][]byte
	rawBytes int64
	codecs   []compressionProbeCodec
}

type compressionProbeCodec struct {
	name            string
	compress        func(dst, src []byte) ([]byte, error)
	decompress      func(dst, src []byte) ([]byte, error)
	compressed      [][]byte
	compressedBytes int64
}

func TestCompression4KProbeRatio(t *testing.T) {
	data := loadCompressionProbeData(t)
	t.Logf("samples=%d avg_raw=%dB", len(data.raw), data.rawBytes/int64(len(data.raw)))
	for _, codec := range data.codecs {
		ratio := float64(codec.compressedBytes) / float64(data.rawBytes)
		t.Logf("%s avg_compressed=%dB ratio=%.3f saved=%.1f%%",
			codec.name,
			codec.compressedBytes/int64(len(codec.compressed)),
			ratio,
			(1-ratio)*100,
		)
	}
}

func BenchmarkCompression4KCompress(b *testing.B) {
	data := loadCompressionProbeData(b)
	for _, codec := range data.codecs {
		b.Run(codec.name, func(b *testing.B) {
			out := make([]byte, 0, maxCompressionProbeLen(data.raw)*2)
			b.SetBytes(data.rawBytes / int64(len(data.raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				out, err = codec.compress(out[:0], data.raw[i%len(data.raw)])
				if err != nil {
					b.Fatalf("compress: %v", err)
				}
			}
			oneLeafBenchSink = out
		})
	}
}

func BenchmarkCompression4KDecompress(b *testing.B) {
	data := loadCompressionProbeData(b)
	for _, codec := range data.codecs {
		b.Run(codec.name, func(b *testing.B) {
			out := make([]byte, 0, maxCompressionProbeLen(data.raw))
			b.SetBytes(data.rawBytes / int64(len(data.raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				out, err = codec.decompress(out[:0], codec.compressed[i%len(codec.compressed)])
				if err != nil {
					b.Fatalf("decompress: %v", err)
				}
			}
			oneLeafBenchSink = out
		})
	}
}

func loadCompressionProbeData(tb testing.TB) compressionProbeData {
	tb.Helper()
	compressionProbe.once.Do(func() {
		compressionProbe.data, compressionProbe.err = buildCompressionProbeData()
	})
	if compressionProbe.err != nil {
		tb.Fatalf("build compression probe data: %v", compressionProbe.err)
	}
	return compressionProbe.data
}

func buildCompressionProbeData() (compressionProbeData, error) {
	raw, err := compressionProbeBlocks()
	if err != nil {
		return compressionProbeData{}, err
	}

	var rawBytes int64
	for _, block := range raw {
		rawBytes += int64(len(block))
	}

	codecs := make([]compressionProbeCodec, 0, 7)
	for _, size := range []int{4 << 10, 8 << 10, 16 << 10} {
		dict, err := compression.PretrainDictionary(compression.PretrainOptions{
			ID:      uint32(size >> 10),
			Size:    size,
			Level:   compression.DefaultLZ4Acceleration,
			Samples: raw,
		})
		if err != nil {
			return compressionProbeData{}, fmt.Errorf("train lz4 dict %d: %w", size, err)
		}
		codecs = append(codecs, compressionProbeCodec{
			name:       fmt.Sprintf("LZ4Dict%dKB", size>>10),
			compress:   dict.CompressInto,
			decompress: dict.DecompressInto,
		})
	}
	codecs = append(codecs, compressionProbeCodec{
		name: "SnappyNoDict",
		compress: func(dst, src []byte) ([]byte, error) {
			if cap(dst) > 0 {
				dst = dst[:cap(dst)]
			}
			return snappy.Encode(dst, src), nil
		},
		decompress: func(dst, src []byte) ([]byte, error) {
			if cap(dst) > 0 {
				dst = dst[:cap(dst)]
			}
			return snappy.Decode(dst, src)
		},
	})
	for _, size := range []int{4 << 10, 8 << 10, 16 << 10} {
		rawDict, err := dict.BuildS2Dict(raw, dict.Options{
			MaxDictSize: size,
			HashBytes:   6,
		})
		if err != nil {
			return compressionProbeData{}, fmt.Errorf("train s2 dict %d: %w", size, err)
		}
		s2Dict := s2.NewDict(rawDict)
		codecs = append(codecs, compressionProbeCodec{
			name: fmt.Sprintf("S2Dict%dKB", size>>10),
			compress: func(dst, src []byte) ([]byte, error) {
				return s2Dict.Encode(dst, src), nil
			},
			decompress: s2Dict.Decode,
		})
	}
	for i := range codecs {
		for _, block := range raw {
			encoded, err := codecs[i].compress(nil, block)
			if err != nil {
				return compressionProbeData{}, fmt.Errorf("%s compress sample: %w", codecs[i].name, err)
			}
			decoded, err := codecs[i].decompress(nil, encoded)
			if err != nil {
				return compressionProbeData{}, fmt.Errorf("%s decompress sample: %w", codecs[i].name, err)
			}
			if !bytes.Equal(decoded, block) {
				return compressionProbeData{}, fmt.Errorf("%s round trip mismatch", codecs[i].name)
			}
			codecs[i].compressed = append(codecs[i].compressed, encoded)
			codecs[i].compressedBytes += int64(len(encoded))
		}
	}

	return compressionProbeData{
		raw:      raw,
		rawBytes: rawBytes,
		codecs:   codecs,
	}, nil
}

func compressionProbeBlocks() ([][]byte, error) {
	entries, err := onedb.LoadWorkloadFromJSONL(compressionProbeJSONL, 50_000)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	var samples [][]byte
	var block segment.Block
	blockSize := 4 + 4 + 4 + 4
	var prevKey []byte

	flush := func() error {
		if block.Empty() {
			return nil
		}
		raw, err := block.MarshalBinary()
		if err != nil {
			return err
		}
		if len(raw) >= 3<<10 && len(raw) <= 5<<10 {
			samples = append(samples, raw)
		}
		block = segment.Block{}
		blockSize = 4 + 4 + 4 + 4
		return nil
	}

	for _, entry := range entries {
		if bytes.Equal(prevKey, entry.Key) {
			continue
		}
		encodedValue := value.EncodeBytes(entry.Value)
		entrySize := 4 + 4 + len(entry.Key) + len(encodedValue)
		if !block.Empty() && blockSize+entrySize > segment.DefaultTargetBlockSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if err := block.AddKVUnsafe(entry.Key, encodedValue); err != nil {
			return nil, err
		}
		blockSize += entrySize
		prevKey = entry.Key
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if len(samples) == 0 {
		return nil, compression.ErrNoSamples
	}
	return samples, nil
}

func maxCompressionProbeLen(blocks [][]byte) int {
	maxLen := 0
	for _, block := range blocks {
		if len(block) > maxLen {
			maxLen = len(block)
		}
	}
	return maxLen
}
