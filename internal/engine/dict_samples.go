package engine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/uchebnick/fusedb/internal/compression"
	"github.com/uchebnick/fusedb/internal/segment"
	"github.com/uchebnick/fusedb/internal/value"
)

const jsonlScannerMaxToken = 16 << 20

type jsonlRecord struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type workloadEntry struct {
	Key   []byte
	Value []byte
}

func TrainDictionaryFromJSONL(path string, id uint32, size, level, maxRecords int) (*compression.Dictionary, int, error) {
	samples, count, err := blockSamplesFromJSONL(path, maxRecords)
	if err != nil {
		return nil, 0, err
	}
	dict, err := compression.PretrainDictionary(compression.PretrainOptions{
		ID:      id,
		Size:    size,
		Level:   level,
		Samples: samples,
	})
	if err != nil {
		return nil, 0, err
	}
	return dict, count, nil
}

func LoadWorkloadFromJSONL(path string, maxRecords int) ([]workloadEntry, error) {
	return readJSONLEntries(path, maxRecords, false)
}

func blockSamplesFromJSONL(path string, maxRecords int) ([][]byte, int, error) {
	entries, err := readJSONLEntries(path, maxRecords, true)
	if err != nil {
		return nil, 0, err
	}
	if len(entries) == 0 {
		return nil, 0, compression.ErrNoSamples
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	samples := make([][]byte, 0, len(entries)/32)
	var block segment.Block
	blockSize := encodedBlockBaseSize()
	prevKey := []byte(nil)

	flush := func() error {
		if block.Empty() {
			return nil
		}
		raw, err := block.MarshalBinary()
		if err != nil {
			return err
		}
		samples = append(samples, raw)
		block = segment.Block{}
		blockSize = encodedBlockBaseSize()
		return nil
	}

	for _, entry := range entries {
		if bytes.Equal(prevKey, entry.Key) {
			continue
		}
		entrySize := encodedEntrySize(entry)
		if !block.Empty() && blockSize+entrySize > segment.DefaultTargetBlockSize {
			if err := flush(); err != nil {
				return nil, 0, err
			}
		}
		if err := block.AddKVUnsafe(entry.Key, entry.Value); err != nil {
			return nil, 0, err
		}
		blockSize += entrySize
		prevKey = entry.Key
	}
	if err := flush(); err != nil {
		return nil, 0, err
	}
	if len(samples) == 0 {
		return nil, 0, compression.ErrNoSamples
	}
	return samples, len(entries), nil
}

func readJSONLEntries(path string, maxRecords int, encodeValue bool) ([]workloadEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// Read-only handle: a failed close cannot invalidate the entries just read.
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), jsonlScannerMaxToken)

	entries := make([]workloadEntry, 0)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var record jsonlRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("%s:%d: decode jsonl: %w", path, lineNo, err)
		}
		if record.Key == "" {
			return nil, fmt.Errorf("%s:%d: empty key", path, lineNo)
		}

		var compact bytes.Buffer
		if err := json.Compact(&compact, record.Value); err != nil {
			return nil, fmt.Errorf("%s:%d: compact value: %w", path, lineNo, err)
		}

		rawValue := bytes.Clone(compact.Bytes())
		if encodeValue {
			rawValue = value.EncodeBytes(rawValue)
		}
		entries = append(entries, workloadEntry{
			Key:   []byte(record.Key),
			Value: rawValue,
		})

		if maxRecords > 0 && len(entries) >= maxRecords {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func encodedBlockBaseSize() int {
	return 4 + 4 + 4 + 4
}

func encodedEntrySize(entry workloadEntry) int {
	return 4 + 4 + len(entry.Key) + len(entry.Value)
}
