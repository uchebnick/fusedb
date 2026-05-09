package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"fusedb/internal/compression"
	onedb "fusedb/internal/oneleafdb"
)

func main() {
	dir := flag.String("dir", "", "segment directory; defaults to a temporary directory")
	count := flag.Int("n", 100_000, "number of sequential writes")
	valueSize := flag.Int("value-size", 128, "value size in bytes")
	threshold := flag.Int64("threshold", onedb.DefaultMergeThresholdBytes, "merge threshold in buffered bytes")
	dictSamples := flag.String("dict-samples", "", "jsonl key/value samples used to train zstd dictionary")
	dictID := flag.Uint("dict-id", 1, "trained dictionary id")
	dictSize := flag.Int("dict-size", compression.DefaultDictionarySize, "trained dictionary size in bytes")
	dictLevel := flag.Int("dict-level", compression.DefaultZstdLevel, "zstd compression level")
	dictMaxRecords := flag.Int("dict-max-records", 50_000, "maximum jsonl records used for dictionary training")
	workloadJSONL := flag.String("workload-jsonl", "", "jsonl key/value workload; defaults to dict-samples when set")
	enableWAL := flag.Bool("wal", false, "enable simple append-only cmd WAL")
	walPath := flag.String("wal-path", "", "WAL path; defaults to <dir>/oneleaf.wal when -wal is set")
	flag.Parse()

	dbDir := *dir
	if dbDir == "" {
		tempDir, err := os.MkdirTemp("", "fusedb-oneleaf-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
			os.Exit(1)
		}
		defer os.RemoveAll(tempDir)
		dbDir = tempDir
	}
	if *enableWAL && *walPath == "" {
		*walPath = filepath.Join(dbDir, "oneleaf.wal")
	}

	var dict *compression.Dictionary
	if *dictSamples != "" {
		start := time.Now()
		trained, records, err := onedb.TrainDictionaryFromJSONL(
			*dictSamples,
			uint32(*dictID),
			*dictSize,
			*dictLevel,
			*dictMaxRecords,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "train dictionary: %v\n", err)
			os.Exit(1)
		}
		dict = trained
		fmt.Printf(
			"dictionary=id:%d raw_size:%d records:%d elapsed:%s\n",
			dict.ID(),
			len(dict.Raw()),
			records,
			time.Since(start),
		)
	}

	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            dbDir,
		ThresholdBytes: *threshold,
		Dictionary:     dict,
		WALPath:        *walPath,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	workloadPath := *workloadJSONL
	if workloadPath == "" && *dictSamples != "" {
		workloadPath = *dictSamples
	}

	start := time.Now()
	writes := 0
	if workloadPath != "" {
		entries, err := onedb.LoadWorkloadFromJSONL(workloadPath, *count)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load workload: %v\n", err)
			os.Exit(1)
		}
		for i, entry := range entries {
			if err := db.Put(entry.Key, entry.Value); err != nil {
				fmt.Fprintf(os.Stderr, "put workload %d: %v\n", i, err)
				os.Exit(1)
			}
		}
		writes = len(entries)
	} else {
		value := make([]byte, *valueSize)
		for i := 0; i < *count; i++ {
			key := onedb.DBKey(i)
			if err := db.Put(key, value); err != nil {
				fmt.Fprintf(os.Stderr, "put %d: %v\n", i, err)
				os.Exit(1)
			}
		}
		writes = *count
	}
	if err := db.Merge(); err != nil {
		fmt.Fprintf(os.Stderr, "final merge: %v\n", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)

	fmt.Printf("dir=%s\n", dbDir)
	fmt.Printf("writes=%d value_size=%d threshold=%d workload=%s wal=%v\n", writes, *valueSize, *threshold, workloadPath, *walPath != "")
	fmt.Printf("elapsed=%s ops_sec=%.0f\n", elapsed, float64(writes)/elapsed.Seconds())
}
