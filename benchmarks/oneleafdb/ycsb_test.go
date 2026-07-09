package oneleafdbbench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/uchebnick/fusedb/internal/compression"
	onedb "github.com/uchebnick/fusedb/pkg/oneleafdb"

	"github.com/cockroachdb/pebble"
	"github.com/magiconair/properties"
	yclient "github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	_ "github.com/pingcap/go-ycsb/pkg/workload"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	ycsbRecordCount    = 64 * 1024
	ycsbOperationCount = 100_000
	ycsbThreadCount    = 1
)

var errYCSBScanUnsupported = errors.New("oneleafdbbench: scan is not supported by the ycsb adapter")

type ycsbWorkloadSpec struct {
	name                string
	description         string
	doTransactions      bool
	readProportion      string
	updateProportion    string
	insertProportion    string
	scanProportion      string
	readModifyWriteProp string
}

type ycsbEngineSpec struct {
	name string
	open func(*testing.T, *ycsbLatencyRecorder) (ycsb.DB, func())
}

type ycsbRunResult struct {
	workload string
	engine   string
	summary  latencySummary
}

func TestGoYCSBCoreLatency(t *testing.T) {
	if os.Getenv("FUSEDB_REAL_YCSB") != "1" {
		t.Skip("set FUSEDB_REAL_YCSB=1 to run real go-ycsb Core workload benchmarks")
	}

	workloads := []ycsbWorkloadSpec{
		{
			name:           "Load",
			description:    "100% insert",
			doTransactions: false,
		},
		{
			name:             "A",
			description:      "50% read, 50% update",
			doTransactions:   true,
			readProportion:   "0.5",
			updateProportion: "0.5",
		},
		{
			name:             "B",
			description:      "95% read, 5% update",
			doTransactions:   true,
			readProportion:   "0.95",
			updateProportion: "0.05",
		},
		{
			name:           "C",
			description:    "100% read",
			doTransactions: true,
			readProportion: "1",
		},
		{
			name:                "F",
			description:         "read-modify-write",
			doTransactions:      true,
			readModifyWriteProp: "1",
		},
	}

	compressedDict := newProbeDictionary(t)
	compressedDictRaw := compressedDict.Raw()
	defer compressedDict.Close()
	engines := []ycsbEngineSpec{
		{name: "OneLeaf raw", open: openYCSBOneLeaf(false, false, nil)},
		{name: "OneLeaf LZ4Dict4KB", open: openYCSBOneLeaf(true, false, compressedDictRaw)},
		{name: "OneLeaf raw + async WAL", open: openYCSBOneLeaf(false, true, nil)},
		{name: "OneLeaf LZ4Dict4KB + async WAL", open: openYCSBOneLeaf(true, true, compressedDictRaw)},
		{name: "Pebble NoSync", open: openYCSBPebble},
	}

	var results []ycsbRunResult
	for _, workload := range workloads {
		for _, engine := range engines {
			recorder := &ycsbLatencyRecorder{}
			db, cleanup := engine.open(t, recorder)
			if workload.doTransactions {
				runGoYCSB(t, db, ycsbLoadProperties(ycsbRecordCount))
				recorder.Reset()
			}

			runGoYCSB(t, db, ycsbCoreProperties(workload))
			summary := recorder.Summary()
			if summary.count == 0 {
				cleanup()
				t.Fatalf("%s %s produced no latency samples", workload.name, engine.name)
			}
			if err := recorder.Err(); err != nil {
				cleanup()
				t.Fatalf("%s %s failed: %v", workload.name, engine.name, err)
			}

			results = append(results, ycsbRunResult{
				workload: workload.name,
				engine:   engine.name,
				summary:  summary,
			})
			cleanup()
		}
	}

	logYCSBResults(t, workloads, results)
}

func runGoYCSB(t *testing.T, db ycsb.DB, props *properties.Properties) {
	t.Helper()

	measurement.InitMeasure(props)

	creator := ycsb.GetWorkloadCreator(props.GetString(prop.Workload, "core"))
	if creator == nil {
		t.Fatalf("go-ycsb workload creator is not registered")
	}
	workload, err := creator.Create(props)
	if err != nil {
		t.Fatalf("create go-ycsb workload: %v", err)
	}
	defer workload.Close()

	yclient.NewClient(props, workload, db).Run(context.Background())
}

func ycsbLoadProperties(insertCount int) *properties.Properties {
	props := ycsbBaseProperties()
	setYCSBProp(props, prop.DoTransactions, "false")
	setYCSBProp(props, prop.InsertCount, fmt.Sprint(insertCount))
	return props
}

func ycsbCoreProperties(spec ycsbWorkloadSpec) *properties.Properties {
	props := ycsbBaseProperties()
	setYCSBProp(props, prop.DoTransactions, fmt.Sprint(spec.doTransactions))
	setYCSBProp(props, prop.OperationCount, fmt.Sprint(ycsbOperationCount))
	setYCSBProp(props, prop.ReadProportion, defaultYCSBProportion(spec.readProportion))
	setYCSBProp(props, prop.UpdateProportion, defaultYCSBProportion(spec.updateProportion))
	setYCSBProp(props, prop.InsertProportion, defaultYCSBProportion(spec.insertProportion))
	setYCSBProp(props, prop.ScanProportion, defaultYCSBProportion(spec.scanProportion))
	setYCSBProp(props, prop.ReadModifyWriteProportion, defaultYCSBProportion(spec.readModifyWriteProp))
	return props
}

func ycsbBaseProperties() *properties.Properties {
	props := properties.NewProperties()
	setYCSBProp(props, prop.Workload, "core")
	setYCSBProp(props, prop.RecordCount, fmt.Sprint(ycsbRecordCount))
	setYCSBProp(props, prop.OperationCount, fmt.Sprint(ycsbOperationCount))
	setYCSBProp(props, prop.ThreadCount, fmt.Sprint(ycsbThreadCount))
	setYCSBProp(props, prop.TableName, "usertable")
	setYCSBProp(props, prop.FieldCount, "1")
	setYCSBProp(props, prop.FieldLength, fmt.Sprint(benchValueSize))
	setYCSBProp(props, prop.FieldLengthDistribution, "constant")
	setYCSBProp(props, prop.ReadAllFields, "true")
	setYCSBProp(props, prop.WriteAllFields, "true")
	setYCSBProp(props, prop.RequestDistribution, "uniform")
	setYCSBProp(props, prop.InsertOrder, "hashed")
	setYCSBProp(props, prop.Silence, "true")
	setYCSBProp(props, prop.WarmUpTime, "0")
	setYCSBProp(props, prop.LogInterval, "3600")
	return props
}

func setYCSBProp(props *properties.Properties, key, value string) {
	if _, _, err := props.Set(key, value); err != nil {
		panic(err)
	}
}

func defaultYCSBProportion(value string) string {
	if value == "" {
		return "0"
	}
	return value
}

func openYCSBOneLeaf(compressed bool, walEnabled bool, dictRaw []byte) func(*testing.T, *ycsbLatencyRecorder) (ycsb.DB, func()) {
	return func(t *testing.T, recorder *ycsbLatencyRecorder) (ycsb.DB, func()) {
		t.Helper()

		dir := t.TempDir()
		opts := onedb.DBOptions{
			Dir:            dir,
			ThresholdBytes: latencyProbeBuffer,
			CacheBytes:     benchCacheBytes,
		}
		if compressed {
			dict, err := compression.NewDictionaryLevel(1, dictRaw, compression.DefaultLZ4Acceleration)
			if err != nil {
				t.Fatalf("create ycsb dictionary: %v", err)
			}
			opts.Dictionary = dict
		}
		if walEnabled {
			opts.WALPath = dir + "/oneleaf.wal"
		}

		db, err := onedb.OpenDB(opts)
		if err != nil {
			t.Fatalf("open oneleaf: %v", err)
		}
		adapter := &oneLeafYCSBDB{db: db, recorder: recorder}
		return adapter, func() {
			if err := adapter.Close(); err != nil {
				t.Fatalf("close oneleaf: %v", err)
			}
		}
	}
}

func openYCSBPebble(t *testing.T, recorder *ycsbLatencyRecorder) (ycsb.DB, func()) {
	t.Helper()

	db, cache := openPebbleLatencyProbeWithCache(t, t.TempDir(), latencyProbeBuffer, benchPebbleCache)
	adapter := &pebbleYCSBDB{db: db, recorder: recorder}
	return adapter, func() {
		if err := adapter.Close(); err != nil {
			t.Fatalf("close pebble: %v", err)
		}
		cache.Unref()
	}
}

func logYCSBResults(t *testing.T, workloads []ycsbWorkloadSpec, results []ycsbRunResult) {
	t.Helper()

	for _, workload := range workloads {
		t.Logf("YCSB %s (%s)", workload.name, workload.description)
		t.Log("| Engine | p50 | p95 | p99 | avg | max |")
		t.Log("|---|---:|---:|---:|---:|---:|")
		for _, result := range results {
			if result.workload != workload.name {
				continue
			}
			summary := result.summary
			t.Logf(
				"| %s | `%s` | `%s` | `%s` | `%s` | `%s` |",
				result.engine,
				summary.p50,
				summary.p95,
				summary.p99,
				summary.avg,
				summary.max,
			)
		}
	}
}

type ycsbLatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
	err     error
}

func (r *ycsbLatencyRecorder) Record(start time.Time, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err != nil && r.err == nil {
		r.err = err
	}
	r.samples = append(r.samples, time.Since(start))
}

func (r *ycsbLatencyRecorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples = nil
	r.err = nil
}

func (r *ycsbLatencyRecorder) Summary() latencySummary {
	r.mu.Lock()
	defer r.mu.Unlock()

	samples := append([]time.Duration(nil), r.samples...)
	return summarizeLatency(samples)
}

func (r *ycsbLatencyRecorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

type oneLeafYCSBDB struct {
	db       *onedb.DB
	recorder *ycsbLatencyRecorder
}

func (db *oneLeafYCSBDB) Close() error {
	return db.db.Close()
}

func (db *oneLeafYCSBDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *oneLeafYCSBDB) CleanupThread(context.Context) {}

func (db *oneLeafYCSBDB) Read(_ context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	start := time.Now()
	value, ok, err := db.db.Get(ycsbKey(table, key))
	if err == nil && !ok {
		err = fmt.Errorf("oneleaf ycsb read miss: %s/%s", table, key)
	}
	db.recorder.Record(start, err)
	if err != nil {
		return nil, err
	}
	return ycsbReadResult(value, fields), nil
}

func (db *oneLeafYCSBDB) Scan(context.Context, string, string, int, []string) ([]map[string][]byte, error) {
	return nil, errYCSBScanUnsupported
}

func (db *oneLeafYCSBDB) Update(_ context.Context, table string, key string, values map[string][]byte) error {
	start := time.Now()
	err := db.db.Put(ycsbKey(table, key), ycsbPackValues(values))
	db.recorder.Record(start, err)
	return err
}

func (db *oneLeafYCSBDB) Insert(_ context.Context, table string, key string, values map[string][]byte) error {
	start := time.Now()
	err := db.db.Put(ycsbKey(table, key), ycsbPackValues(values))
	db.recorder.Record(start, err)
	return err
}

func (db *oneLeafYCSBDB) Delete(_ context.Context, table string, key string) error {
	start := time.Now()
	err := db.db.Delete(ycsbKey(table, key))
	db.recorder.Record(start, err)
	return err
}

type pebbleYCSBDB struct {
	db       *pebble.DB
	recorder *ycsbLatencyRecorder
}

func (db *pebbleYCSBDB) Close() error {
	return db.db.Close()
}

func (db *pebbleYCSBDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *pebbleYCSBDB) CleanupThread(context.Context) {}

func (db *pebbleYCSBDB) Read(_ context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	start := time.Now()
	value, closer, err := db.db.Get(ycsbKey(table, key))
	if err == nil {
		oneLeafBenchSink = append(oneLeafBenchSink[:0], value...)
		err = closer.Close()
	}
	db.recorder.Record(start, err)
	if err != nil {
		return nil, err
	}
	return ycsbReadResult(oneLeafBenchSink, fields), nil
}

func (db *pebbleYCSBDB) Scan(context.Context, string, string, int, []string) ([]map[string][]byte, error) {
	return nil, errYCSBScanUnsupported
}

func (db *pebbleYCSBDB) Update(_ context.Context, table string, key string, values map[string][]byte) error {
	start := time.Now()
	err := db.db.Set(ycsbKey(table, key), ycsbPackValues(values), pebble.NoSync)
	db.recorder.Record(start, err)
	return err
}

func (db *pebbleYCSBDB) Insert(_ context.Context, table string, key string, values map[string][]byte) error {
	start := time.Now()
	err := db.db.Set(ycsbKey(table, key), ycsbPackValues(values), pebble.NoSync)
	db.recorder.Record(start, err)
	return err
}

func (db *pebbleYCSBDB) Delete(_ context.Context, table string, key string) error {
	start := time.Now()
	err := db.db.Delete(ycsbKey(table, key), pebble.NoSync)
	db.recorder.Record(start, err)
	return err
}

func ycsbKey(table string, key string) []byte {
	buf := make([]byte, 0, len(table)+1+len(key))
	buf = append(buf, table...)
	buf = append(buf, ':')
	buf = append(buf, key...)
	return buf
}

func ycsbPackValues(values map[string][]byte) []byte {
	if len(values) == 1 {
		for _, value := range values {
			return bytes.Clone(value)
		}
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var buf []byte
	for _, key := range keys {
		buf = append(buf, key...)
		buf = append(buf, '=')
		buf = append(buf, values[key]...)
		buf = append(buf, '\n')
	}
	return buf
}

func ycsbReadResult(value []byte, fields []string) map[string][]byte {
	if len(fields) == 0 {
		return map[string][]byte{"field0": bytes.Clone(value)}
	}

	result := make(map[string][]byte, len(fields))
	for _, field := range fields {
		if strings.TrimSpace(field) == "" {
			continue
		}
		result[field] = bytes.Clone(value)
	}
	return result
}
