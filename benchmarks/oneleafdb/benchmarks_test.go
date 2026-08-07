package oneleafdbbench

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/compression"
	onedb "github.com/uchebnick/fusedb/pkg/oneleafdb"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v4"
)

const (
	benchSeedKeys    = 64 * 1024
	benchValueSize   = 128
	benchThreshold   = 5 << 20
	benchCacheBytes  = 5 << 20
	benchPebbleCache = benchCacheBytes
)

const (
	latencyProbeOps       = 200_000
	latencyProbeReadKeys  = 64 * 1024
	latencyProbeValueSize = 128
	latencyProbeBuffer    = 10 << 20
	rateLimiterCounters   = 16 * 1024
	rateLimiterConfigs    = 1024
)

var oneLeafBenchSink []byte

type noopPebbleLogger struct{}

func (noopPebbleLogger) Infof(string, ...any)  {}
func (noopPebbleLogger) Fatalf(string, ...any) {}

type latencySummary struct {
	count int
	avg   time.Duration
	p50   time.Duration
	p95   time.Duration
	p99   time.Duration
	max   time.Duration
}

func TestOneLeafPebbleWriteLatency10MB(t *testing.T) {
	if os.Getenv("FUSEDB_LATENCY_PROBE") != "1" {
		t.Skip("set FUSEDB_LATENCY_PROBE=1 to run the oneleafdb latency probe")
	}

	value := make([]byte, latencyProbeValueSize)

	oneLeaf, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            t.TempDir(),
		ThresholdBytes: latencyProbeBuffer,
		CacheBytes:     benchCacheBytes,
	})
	if err != nil {
		t.Fatalf("open oneleaf: %v", err)
	}
	defer oneLeaf.Close()

	oneLeafSamples := measureLatency(latencyProbeOps, func(i int) error {
		return oneLeaf.Put(onedb.DBKey(i), value)
	})
	t.Logf("oneleaf_put_auto_merge_10mb: %s", summarizeLatency(oneLeafSamples))

	compressed := openOneLeafLatencyProbe(t, true, false)
	defer compressed.Close()

	compressedSamples := measureLatency(latencyProbeOps, func(i int) error {
		return compressed.Put(onedb.DBKey(i), value)
	})
	t.Logf("oneleaf_put_auto_merge_10mb_compressed: %s", summarizeLatency(compressedSamples))

	compressedWAL := openOneLeafLatencyProbe(t, true, true)
	defer compressedWAL.Close()

	compressedWALSamples := measureLatency(latencyProbeOps, func(i int) error {
		return compressedWAL.Put(onedb.DBKey(i), value)
	})
	t.Logf("oneleaf_put_auto_merge_10mb_compressed_wal: %s", summarizeLatency(compressedWALSamples))

	pebbleDB, cache := openPebbleLatencyProbe(t, t.TempDir(), latencyProbeBuffer)
	defer cache.Unref()
	defer pebbleDB.Close()

	pebbleSamples := measureLatency(latencyProbeOps, func(i int) error {
		return pebbleDB.Set(onedb.DBKey(i), value, pebble.NoSync)
	})
	t.Logf("pebble_put_nosync_memtable_10mb: %s", summarizeLatency(pebbleSamples))
}

func TestOneLeafPebbleReadLatency64K(t *testing.T) {
	if os.Getenv("FUSEDB_LATENCY_PROBE") != "1" {
		t.Skip("set FUSEDB_LATENCY_PROBE=1 to run the oneleafdb latency probe")
	}

	oneLeaf := openSeededOneLeafLatencyProbe(t, false, latencyProbeReadKeys)
	defer oneLeaf.Close()

	oneLeafSamples := measureLatency(latencyProbeOps, func(i int) error {
		value, ok, err := oneLeaf.Get(onedb.DBKey(i % latencyProbeReadKeys))
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("oneleaf read miss")
		}
		oneLeafBenchSink = value
		return nil
	})
	t.Logf("oneleaf_read_64k: %s", summarizeLatency(oneLeafSamples))

	compressed := openSeededOneLeafLatencyProbe(t, true, latencyProbeReadKeys)
	defer compressed.Close()

	compressedSamples := measureLatency(latencyProbeOps, func(i int) error {
		value, ok, err := compressed.Get(onedb.DBKey(i % latencyProbeReadKeys))
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("oneleaf compressed read miss")
		}
		oneLeafBenchSink = value
		return nil
	})
	t.Logf("oneleaf_read_64k_compressed: %s", summarizeLatency(compressedSamples))

	pebbleCached := openSeededPebbleLatencyProbe(t, t.TempDir(), latencyProbeReadKeys, benchPebbleCache)
	defer pebbleCached.Close()

	pebbleCachedSamples := measureLatency(latencyProbeOps, func(i int) error {
		value, closer, err := pebbleCached.Get(onedb.DBKey(i % latencyProbeReadKeys))
		if err != nil {
			return err
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], value...)
		return closer.Close()
	})
	t.Logf("pebble_read_64k_cached: %s", summarizeLatency(pebbleCachedSamples))

	pebbleNoCache := openSeededPebbleLatencyProbe(t, t.TempDir(), latencyProbeReadKeys, 0)
	defer pebbleNoCache.Close()

	pebbleNoCacheSamples := measureLatency(latencyProbeOps, func(i int) error {
		value, closer, err := pebbleNoCache.Get(onedb.DBKey(i % latencyProbeReadKeys))
		if err != nil {
			return err
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], value...)
		return closer.Close()
	})
	t.Logf("pebble_read_64k_no_block_cache: %s", summarizeLatency(pebbleNoCacheSamples))
}

func TestOneLeafPebbleRateLimiterLatency10MB(t *testing.T) {
	if os.Getenv("FUSEDB_LATENCY_PROBE") != "1" {
		t.Skip("set FUSEDB_LATENCY_PROBE=1 to run the oneleafdb latency probe")
	}

	updateValue := make([]byte, latencyProbeValueSize)

	oneLeaf, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            t.TempDir(),
		ThresholdBytes: latencyProbeBuffer,
		CacheBytes:     benchCacheBytes,
	})
	if err != nil {
		t.Fatalf("open oneleaf: %v", err)
	}
	defer oneLeaf.Close()

	oneLeafSamples := measureLatency(latencyProbeOps, func(i int) error {
		if isRateLimiterUpdate(i) {
			return oneLeaf.Put(rateLimiterConfigKey(i), updateValue)
		}
		return oneLeaf.Inc(rateLimiterCounterKey(i), 1)
	})
	t.Logf("oneleaf_rate_limiter_95inc_5update_10mb: %s", summarizeLatency(oneLeafSamples))

	compressed := openOneLeafLatencyProbe(t, true, false)
	defer compressed.Close()

	compressedSamples := measureLatency(latencyProbeOps, func(i int) error {
		if isRateLimiterUpdate(i) {
			return compressed.Put(rateLimiterConfigKey(i), updateValue)
		}
		return compressed.Inc(rateLimiterCounterKey(i), 1)
	})
	t.Logf("oneleaf_rate_limiter_95inc_5update_10mb_compressed: %s", summarizeLatency(compressedSamples))

	compressedWAL := openOneLeafLatencyProbe(t, true, true)
	defer compressedWAL.Close()

	compressedWALSamples := measureLatency(latencyProbeOps, func(i int) error {
		if isRateLimiterUpdate(i) {
			return compressedWAL.Put(rateLimiterConfigKey(i), updateValue)
		}
		return compressedWAL.Inc(rateLimiterCounterKey(i), 1)
	})
	t.Logf("oneleaf_rate_limiter_95inc_5update_10mb_compressed_wal: %s", summarizeLatency(compressedWALSamples))

	pebbleDB, cache := openPebbleLatencyProbe(t, t.TempDir(), latencyProbeBuffer)
	defer cache.Unref()
	defer pebbleDB.Close()

	pebbleSamples := measureLatency(latencyProbeOps, func(i int) error {
		if isRateLimiterUpdate(i) {
			return pebbleDB.Set(rateLimiterConfigKey(i), updateValue, pebble.NoSync)
		}
		return pebbleInc(pebbleDB, rateLimiterCounterKey(i))
	})
	t.Logf("pebble_rate_limiter_95inc_5update_10mb: %s", summarizeLatency(pebbleSamples))
}

func BenchmarkOneLeafPutAutoMerge5MB(b *testing.B) {
	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            b.TempDir(),
		ThresholdBytes: benchThreshold,
		CacheBytes:     benchCacheBytes,
	})
	if err != nil {
		b.Fatalf("open oneleaf: %v", err)
	}
	defer db.Close()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(onedb.DBKey(i), value); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkOneLeafPutAutoMerge5MBCompressed(b *testing.B) {
	db := newOneLeafDBForBench(b, true, false)
	defer db.Close()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(onedb.DBKey(i), value); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkOneLeafPutAutoMerge5MBCompressedWAL(b *testing.B) {
	db := newOneLeafDBForBench(b, true, true)
	defer db.Close()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(onedb.DBKey(i), value); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkOneLeafIncAutoMerge5MB(b *testing.B) {
	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            b.TempDir(),
		ThresholdBytes: benchThreshold,
		CacheBytes:     benchCacheBytes,
	})
	if err != nil {
		b.Fatalf("open oneleaf: %v", err)
	}
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Inc(onedb.DBKey(i%1024), 1); err != nil {
			b.Fatalf("inc: %v", err)
		}
	}
}

func BenchmarkOneLeafIncAutoMerge5MBCompressed(b *testing.B) {
	db := newOneLeafDBForBench(b, true, false)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Inc(onedb.DBKey(i%1024), 1); err != nil {
			b.Fatalf("inc: %v", err)
		}
	}
}

func BenchmarkOneLeafIncAutoMerge5MBCompressedWAL(b *testing.B) {
	db := newOneLeafDBForBench(b, true, true)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Inc(onedb.DBKey(i%1024), 1); err != nil {
			b.Fatalf("inc: %v", err)
		}
	}
}

func BenchmarkOneLeafGet64K(b *testing.B) {
	db := newSeededOneLeafDB(b, benchSeedKeys)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, ok, err := db.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil || !ok {
			b.Fatalf("get: ok=%v err=%v", ok, err)
		}
		oneLeafBenchSink = value
	}
}

func BenchmarkOneLeafGet64KCompressed(b *testing.B) {
	db := newSeededOneLeafDBCompressed(b, benchSeedKeys)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, ok, err := db.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil || !ok {
			b.Fatalf("get: ok=%v err=%v", ok, err)
		}
		oneLeafBenchSink = value
	}
}

func BenchmarkOneLeafMixedPutGet5MB(b *testing.B) {
	db := newSeededOneLeafDB(b, benchSeedKeys)
	defer db.Close()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&1 == 0 {
			if err := db.Put(onedb.DBKey(benchSeedKeys+i), value); err != nil {
				b.Fatalf("put: %v", err)
			}
			continue
		}
		got, ok, err := db.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil || !ok {
			b.Fatalf("get: ok=%v err=%v", ok, err)
		}
		oneLeafBenchSink = got
	}
}

func BenchmarkOneLeafMixedPutGet5MBCompressed(b *testing.B) {
	db := newSeededOneLeafDBCompressed(b, benchSeedKeys)
	defer db.Close()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&1 == 0 {
			if err := db.Put(onedb.DBKey(benchSeedKeys+i), value); err != nil {
				b.Fatalf("put: %v", err)
			}
			continue
		}
		got, ok, err := db.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil || !ok {
			b.Fatalf("get: ok=%v err=%v", ok, err)
		}
		oneLeafBenchSink = got
	}
}

func BenchmarkPebblePutNoSync(b *testing.B) {
	db := openPebbleForBench(b)
	defer db.Close()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Set(onedb.DBKey(i), value, pebble.NoSync); err != nil {
			b.Fatalf("set: %v", err)
		}
	}
}

func BenchmarkPebbleGet64K(b *testing.B) {
	db := openSeededPebbleDB(b, benchSeedKeys)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, closer, err := db.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], value...)
		if err := closer.Close(); err != nil {
			b.Fatalf("close get value: %v", err)
		}
	}
}

func BenchmarkPebbleGet64KNoBlockCache(b *testing.B) {
	db := openSeededPebbleDBWithCache(b, benchSeedKeys, 0)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, closer, err := db.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], value...)
		if err := closer.Close(); err != nil {
			b.Fatalf("close get value: %v", err)
		}
	}
}

func BenchmarkOneLeafOpenGet64K(b *testing.B) {
	// Cold start measured honestly: the database is seeded, closed, and then
	// reopened from its own directory, so the timed work is the real open path
	// (manifest load plus segment open) rather than a reader handed in from the
	// outside.
	dir := b.TempDir()
	seedOneLeafDBAt(b, dir, benchSeedKeys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opened, err := onedb.OpenDB(onedb.DBOptions{
			Dir:            dir,
			ThresholdBytes: benchThreshold,
			CacheBytes:     benchCacheBytes,
			DisableWAL:     true,
		})
		if err != nil {
			b.Fatalf("open oneleaf: %v", err)
		}

		value, ok, err := opened.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil || !ok {
			_ = opened.Close()
			b.Fatalf("get: ok=%v err=%v", ok, err)
		}
		oneLeafBenchSink = value
		_ = opened.Close()
	}
}

// seedOneLeafDBAt fills a directory with a merged database and closes it.
func seedOneLeafDBAt(b *testing.B, dir string, keys int) {
	b.Helper()

	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            dir,
		ThresholdBytes: benchThreshold,
		CacheBytes:     benchCacheBytes,
		DisableWAL:     true,
	})
	if err != nil {
		b.Fatalf("open oneleaf: %v", err)
	}

	value := make([]byte, benchValueSize)
	for i := 0; i < keys; i++ {
		if err := db.Put(onedb.DBKey(i), value); err != nil {
			b.Fatalf("put seed %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close seeded oneleaf: %v", err)
	}
}

func BenchmarkPebbleOpenGet64KNoBlockCache(b *testing.B) {
	dir := b.TempDir()
	db := openSeededPebbleDBAtWithCache(b, dir, benchSeedKeys, 0)
	if err := db.Close(); err != nil {
		b.Fatalf("close seeded pebble: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opened, cache := openPebbleForBenchAtNoCleanup(b, dir, 0)
		value, closer, err := opened.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil {
			_ = opened.Close()
			cache.Unref()
			b.Fatalf("get: %v", err)
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], value...)
		if err := closer.Close(); err != nil {
			_ = opened.Close()
			cache.Unref()
			b.Fatalf("close get value: %v", err)
		}
		if err := opened.Close(); err != nil {
			cache.Unref()
			b.Fatalf("close pebble: %v", err)
		}
		cache.Unref()
	}
}

func BenchmarkPebbleMixedPutGetNoSync(b *testing.B) {
	db := openSeededPebbleDB(b, benchSeedKeys)
	defer db.Close()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&1 == 0 {
			if err := db.Set(onedb.DBKey(benchSeedKeys+i), value, pebble.NoSync); err != nil {
				b.Fatalf("set: %v", err)
			}
			continue
		}
		got, closer, err := db.Get(onedb.DBKey(i % benchSeedKeys))
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		oneLeafBenchSink = append(oneLeafBenchSink[:0], got...)
		if err := closer.Close(); err != nil {
			b.Fatalf("close get value: %v", err)
		}
	}
}

func newSeededOneLeafDB(b *testing.B, keys int) *onedb.DB {
	b.Helper()

	db := newOneLeafDBForBench(b, false, false)

	value := make([]byte, benchValueSize)
	for i := 0; i < keys; i++ {
		if err := db.Put(onedb.DBKey(i), value); err != nil {
			b.Fatalf("put seed %d: %v", i, err)
		}
	}
	if err := db.Merge(); err != nil {
		b.Fatalf("seed merge: %v", err)
	}
	return db
}

func newSeededOneLeafDBCompressed(b *testing.B, keys int) *onedb.DB {
	b.Helper()

	db := newOneLeafDBForBench(b, true, false)
	value := make([]byte, benchValueSize)
	for i := 0; i < keys; i++ {
		if err := db.Put(onedb.DBKey(i), value); err != nil {
			b.Fatalf("put seed %d: %v", i, err)
		}
	}
	if err := db.Merge(); err != nil {
		b.Fatalf("seed merge: %v", err)
	}
	return db
}

func newOneLeafDBForBench(b *testing.B, compressed, walEnabled bool) *onedb.DB {
	b.Helper()

	dir := b.TempDir()
	var dict *compression.Dictionary
	if compressed {
		dict = newBenchDictionary(b)
	}
	walPath := ""
	if walEnabled {
		walPath = filepath.Join(dir, "oneleaf.wal")
	}

	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            dir,
		ThresholdBytes: benchThreshold,
		Dictionary:     dict,
		WALPath:        walPath,
		DisableWAL:     !walEnabled,
		CacheBytes:     benchCacheBytes,
	})
	if err != nil {
		b.Fatalf("open oneleaf: %v", err)
	}
	return db
}

func newBenchDictionary(b *testing.B) *compression.Dictionary {
	b.Helper()

	dict, _, err := onedb.TrainDictionaryFromJSONL(
		"../../internal/compression/kv_dict_samples_50k.jsonl",
		1,
		compression.DefaultDictionarySize,
		compression.DefaultLZ4Acceleration,
		50_000,
	)
	if err != nil {
		b.Fatalf("train dict: %v", err)
	}
	return dict
}

func openPebbleForBench(b *testing.B) *pebble.DB {
	b.Helper()

	return openPebbleForBenchAt(b, b.TempDir(), benchPebbleCache)
}

func openPebbleForBenchAt(b *testing.B, dir string, cacheSize int64) *pebble.DB {
	b.Helper()

	db, cache := openPebbleForBenchAtNoCleanup(b, dir, cacheSize)
	b.Cleanup(cache.Unref)
	return db
}

func openPebbleForBenchAtNoCleanup(b *testing.B, dir string, cacheSize int64) (*pebble.DB, *pebble.Cache) {
	b.Helper()

	cache := pebble.NewCache(cacheSize)
	db, err := pebble.Open(dir, &pebble.Options{
		Cache:  cache,
		Logger: noopPebbleLogger{},
	})
	if err != nil {
		cache.Unref()
		b.Fatalf("open pebble: %v", err)
	}
	return db, cache
}

func openSeededPebbleDB(b *testing.B, keys int) *pebble.DB {
	b.Helper()

	return openSeededPebbleDBWithCache(b, keys, benchPebbleCache)
}

func openSeededPebbleDBWithCache(b *testing.B, keys int, cacheSize int64) *pebble.DB {
	b.Helper()

	return openSeededPebbleDBAtWithCache(b, b.TempDir(), keys, cacheSize)
}

func openSeededPebbleDBAtWithCache(b *testing.B, dir string, keys int, cacheSize int64) *pebble.DB {
	b.Helper()

	db := openPebbleForBenchAt(b, dir, cacheSize)
	value := make([]byte, benchValueSize)
	for i := 0; i < keys; i++ {
		if err := db.Set(onedb.DBKey(i), value, pebble.NoSync); err != nil {
			b.Fatalf("seed pebble %d: %v", i, err)
		}
	}
	if err := db.Flush(); err != nil {
		b.Fatalf("flush pebble: %v", err)
	}
	return db
}

func measureLatency(n int, fn func(int) error) []time.Duration {
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		if err := fn(i); err != nil {
			panic(err)
		}
		samples = append(samples, time.Since(start))
	}
	return samples
}

func isRateLimiterUpdate(i int) bool {
	return i%20 == 0
}

func rateLimiterCounterKey(i int) []byte {
	return []byte(fmt.Sprintf("counter:%016d", i%rateLimiterCounters))
}

func rateLimiterConfigKey(i int) []byte {
	return []byte(fmt.Sprintf("config:%016d", i%rateLimiterConfigs))
}

func pebbleInc(db *pebble.DB, key []byte) error {
	current, closer, err := db.Get(key)
	var next uint64 = 1
	if err == nil {
		if len(current) == 8 {
			next = binary.LittleEndian.Uint64(current) + 1
		}
		if closeErr := closer.Close(); closeErr != nil {
			return closeErr
		}
	} else if err != pebble.ErrNotFound {
		return err
	}

	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], next)
	return db.Set(key, encoded[:], pebble.NoSync)
}

func summarizeLatency(samples []time.Duration) latencySummary {
	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})

	var total time.Duration
	for _, sample := range samples {
		total += sample
	}

	return latencySummary{
		count: len(samples),
		avg:   total / time.Duration(len(samples)),
		p50:   percentileLatency(samples, 0.50),
		p95:   percentileLatency(samples, 0.95),
		p99:   percentileLatency(samples, 0.99),
		max:   samples[len(samples)-1],
	}
}

func percentileLatency(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(samples))*p)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(samples) {
		index = len(samples) - 1
	}
	return samples[index]
}

func (s latencySummary) String() string {
	return fmt.Sprintf(
		"count=%d avg=%s p50=%s p95=%s p99=%s max=%s",
		s.count,
		s.avg,
		s.p50,
		s.p95,
		s.p99,
		s.max,
	)
}

func openOneLeafLatencyProbe(t testing.TB, compressed, walEnabled bool) *onedb.DB {
	t.Helper()

	dir := t.TempDir()
	var dict *compression.Dictionary
	if compressed {
		dict = newProbeDictionary(t)
	}
	walPath := ""
	if walEnabled {
		walPath = filepath.Join(dir, "oneleaf.wal")
	}

	db, err := onedb.OpenDB(onedb.DBOptions{
		Dir:            dir,
		ThresholdBytes: latencyProbeBuffer,
		Dictionary:     dict,
		WALPath:        walPath,
		CacheBytes:     benchCacheBytes,
	})
	if err != nil {
		t.Fatalf("open oneleaf: %v", err)
	}
	return db
}

func openSeededOneLeafLatencyProbe(t testing.TB, compressed bool, keys int) *onedb.DB {
	t.Helper()

	db := openOneLeafLatencyProbe(t, compressed, false)
	value := make([]byte, latencyProbeValueSize)
	for i := 0; i < keys; i++ {
		if err := db.Put(onedb.DBKey(i), value); err != nil {
			t.Fatalf("seed oneleaf %d: %v", i, err)
		}
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("merge seeded oneleaf: %v", err)
	}
	return db
}

func openSeededPebbleLatencyProbe(t testing.TB, dir string, keys int, cacheSize int64) *pebble.DB {
	t.Helper()

	db, cache := openPebbleLatencyProbeWithCache(t, dir, latencyProbeBuffer, cacheSize)
	t.Cleanup(cache.Unref)

	value := make([]byte, latencyProbeValueSize)
	for i := 0; i < keys; i++ {
		if err := db.Set(onedb.DBKey(i), value, pebble.NoSync); err != nil {
			t.Fatalf("seed pebble %d: %v", i, err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush pebble: %v", err)
	}
	return db
}

func newProbeDictionary(t testing.TB) *compression.Dictionary {
	t.Helper()

	dict, _, err := onedb.TrainDictionaryFromJSONL(
		"../../internal/compression/kv_dict_samples_50k.jsonl",
		1,
		compression.DefaultDictionarySize,
		compression.DefaultLZ4Acceleration,
		50_000,
	)
	if err != nil {
		t.Fatalf("train dict: %v", err)
	}
	return dict
}

func openPebbleLatencyProbe(t testing.TB, dir string, memTableSize int) (*pebble.DB, *pebble.Cache) {
	return openPebbleLatencyProbeWithCache(t, dir, memTableSize, 0)
}

func openPebbleLatencyProbeWithCache(t testing.TB, dir string, memTableSize int, cacheSize int64) (*pebble.DB, *pebble.Cache) {
	t.Helper()

	cache := pebble.NewCache(cacheSize)
	db, err := pebble.Open(dir, &pebble.Options{
		Cache:                       cache,
		Logger:                      noopPebbleLogger{},
		MemTableSize:                uint64(memTableSize),
		MemTableStopWritesThreshold: 4,
	})
	if err != nil {
		cache.Unref()
		t.Fatalf("open pebble: %v", err)
	}
	return db, cache
}

// BadgerDB benchmarks

func BenchmarkBadgerPutAutoMerge5MB(b *testing.B) {
	db := openBadgerDB(b)
	defer func() { _ = db.Close() }()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := onedb.DBKey(i)
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		}); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkBadgerGet64K(b *testing.B) {
	db := openSeededBadgerDB(b, benchSeedKeys)
	defer func() { _ = db.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := onedb.DBKey(i % benchSeedKeys)
		if err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				oneLeafBenchSink = append(oneLeafBenchSink[:0], val...)
				return nil
			})
		}); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkBadgerMixedPutGet5MB(b *testing.B) {
	db := openSeededBadgerDB(b, benchSeedKeys)
	defer func() { _ = db.Close() }()

	value := make([]byte, benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			key := onedb.DBKey(i)
			if err := db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, value)
			}); err != nil {
				b.Fatalf("put: %v", err)
			}
		} else {
			key := onedb.DBKey(i % benchSeedKeys)
			if err := db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					oneLeafBenchSink = append(oneLeafBenchSink[:0], val...)
					return nil
				})
			}); err != nil {
				b.Fatalf("get: %v", err)
			}
		}
	}
}

func openBadgerDB(b testing.TB) *badger.DB {
	b.Helper()

	opts := badger.DefaultOptions(b.TempDir()).
		WithLogger(nil).
		WithMemTableSize(benchThreshold).
		WithBaseTableSize(4 << 20).
		WithValueThreshold(256).
		WithValueLogFileSize(10 << 20).
		WithSyncWrites(false).
		WithNumMemtables(2)

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("open badger: %v", err)
	}

	return db
}

func openSeededBadgerDB(b testing.TB, keys int) *badger.DB {
	b.Helper()

	db := openBadgerDB(b)

	value := make([]byte, benchValueSize)
	for i := 0; i < keys; i++ {
		key := onedb.DBKey(i)
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		}); err != nil {
			_ = db.Close()
			b.Fatalf("seed badger %d: %v", i, err)
		}
	}

	return db
}
