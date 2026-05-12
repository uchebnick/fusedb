package oneleafdbbench

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const benchmarksMarkdownPath = "../benchmarks.md"

var (
	benchmarkLineRE = regexp.MustCompile(`^(Benchmark[^\s]+?)(?:-\d+)?\s+\d+\s+([\d.]+)\s+ns/op(?:\s+([\d.]+)\s+MB/s)?\s+([\d.]+)\s+B/op\s+([\d.]+)\s+allocs/op$`)
	latencyLineRE   = regexp.MustCompile(`^([^:]+):\s*count=(\d+)\s+avg=([^\s]+)\s+p50=([^\s]+)\s+p95=([^\s]+)\s+p99=([^\s]+)\s+max=([^\s]+)$`)
	ycsbHeaderRE    = regexp.MustCompile(`^YCSB\s+(.+?)\s+\((.+)\)$`)
	compressionRE   = regexp.MustCompile(`^([A-Za-z0-9]+)\s+avg_compressed=(\d+)B\s+ratio=([0-9.]+)\s+saved=([0-9.]+)%$`)
	logPrefixRE     = regexp.MustCompile(`^.*\.go:\d+:\s*(.*)$`)
)

type benchSample struct {
	ns     float64
	bytes  float64
	allocs float64
	mbps   float64
}

type benchSpec struct {
	name   string
	engine string
	mode   string
}

type latencySpec struct {
	name   string
	engine string
	mode   string
}

type latencyRow struct {
	engine string
	mode   string
	p50    string
	p95    string
	p99    string
	avg    string
	max    string
}

type ycsbRow struct {
	engine string
	p50    string
	p95    string
	p99    string
	avg    string
	max    string
}

type ycsbSection struct {
	title        string
	description  string
	columnsTitle string
	rows         []ycsbRow
}

type compressionRatioRow struct {
	codec         string
	avgCompressed string
	ratio         string
	saved         string
}

type compressionRatioSummary struct {
	samples string
	avgRaw  string
	rows    []compressionRatioRow
}

type compressionSpeedRow struct {
	codec   string
	nsAvg   string
	nsRange string
	mbpsAvg string
	bOp     string
	allocs  string
}

// Run with: GENERATE_BENCHMARKS=1 go test ./benchmarks/oneleafdb -run TestGenerateBenchmarksMD -v
func TestGenerateBenchmarksMD(t *testing.T) {
	if os.Getenv("GENERATE_BENCHMARKS") != "1" {
		t.Skip("set GENERATE_BENCHMARKS=1 to auto-generate benchmarks.md")
	}

	var out bytes.Buffer
	out.WriteString("# OneLeaf Benchmarks\n\n")
	out.WriteString(fmt.Sprintf("Run Date: `%s`\n\n", time.Now().Format("2006-01-02 15:04:05")))
	out.WriteString("Fresh run after switching segment compression to `LZ4Dict4KB`.\n\n")

	throughput := parseBenchmarkOutput(runGoTest(t,
		[]string{},
		"-run", "^$",
		"-bench", "Benchmark(OneLeaf|Pebble)",
		"-benchmem",
		"-benchtime=2s",
		"-count=3",
	))
	writeThroughputSection(&out, throughput)

	latency := parseLatencyOutput(runGoTest(t,
		[]string{"FUSEDB_LATENCY_PROBE=1"},
		"-run", "TestOneLeafPebble(WriteLatency10MB|ReadLatency64K|RateLimiterLatency10MB)",
		"-count=1",
		"-v",
	))
	writeLatencySection(&out, latency)

	ycsb := parseYCSBOutput(runGoTest(t,
		[]string{"FUSEDB_REAL_YCSB=1"},
		"-run", "TestGoYCSBCoreLatency",
		"-count=1",
		"-v",
	))
	writeYCSBSection(&out, ycsb)

	compressionRatio := parseCompressionRatioOutput(runGoTest(t,
		[]string{},
		"-run", "TestCompression4KProbeRatio",
		"-count=1",
		"-v",
	))
	writeCompressionRatioSection(&out, compressionRatio)

	compressionSpeed := parseBenchmarkOutput(runGoTest(t,
		[]string{},
		"-run", "^$",
		"-bench", "BenchmarkCompression4K(Compress|Decompress)",
		"-benchmem",
		"-benchtime=2s",
		"-count=5",
	))
	writeCompressionSpeedSection(&out, compressionSpeed)

	out.WriteString("## Notes\n\n")
	out.WriteString("- OneLeaf and Pebble both use a `5 MB` cache budget.\n")
	out.WriteString("- `compressed` means `LZ4Dict4KB` trained from `internal/compression/kv_dict_samples_50k.jsonl`.\n")
	out.WriteString("- Async WAL returns after appending to the in-memory WAL buffer.\n")
	out.WriteString("- Pebble rows use `NoSync`; they are a low-latency baseline, not durable-per-write.\n")
	out.WriteString("- go-ycsb workload E is not included because OneLeaf does not expose DB-level scan/range reads yet.\n")
	out.WriteString("- RocksDB benchmarks are behind `-tags rocksdb` and were not included in this run.\n")

	if err := os.WriteFile(benchmarksMarkdownPath, out.Bytes(), 0o644); err != nil {
		t.Fatalf("write benchmarks.md: %v", err)
	}
}

func runGoTest(t *testing.T, env []string, args ...string) string {
	t.Helper()

	cmdArgs := append([]string{"test", "."}, args...)
	cmd := exec.Command("go", cmdArgs...)
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), env...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(cmdArgs, " "), err, out)
	}
	return string(out)
}

func parseBenchmarkOutput(out string) map[string][]benchSample {
	rows := make(map[string][]benchSample)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		m := benchmarkLineRE.FindStringSubmatch(line)
		if len(m) == 0 {
			continue
		}

		sample := benchSample{
			ns:     mustParseFloat(m[2]),
			bytes:  mustParseFloat(m[4]),
			allocs: mustParseFloat(m[5]),
		}
		if m[3] != "" {
			sample.mbps = mustParseFloat(m[3])
		}
		rows[m[1]] = append(rows[m[1]], sample)
	}
	return rows
}

func parseLatencyOutput(out string) map[string]latencyRow {
	rows := make(map[string]latencyRow)
	for _, line := range strings.Split(out, "\n") {
		line = stripGoTestLogPrefix(strings.TrimSpace(line))
		m := latencyLineRE.FindStringSubmatch(line)
		if len(m) == 0 {
			continue
		}
		rows[m[1]] = latencyRow{
			p50: m[4],
			p95: m[5],
			p99: m[6],
			avg: m[3],
			max: m[7],
		}
	}
	return rows
}

func parseYCSBOutput(out string) []ycsbSection {
	var sections []ycsbSection
	var current *ycsbSection

	for _, line := range strings.Split(out, "\n") {
		line = stripGoTestLogPrefix(strings.TrimSpace(line))
		if line == "" {
			continue
		}

		if m := ycsbHeaderRE.FindStringSubmatch(line); len(m) > 0 {
			sections = append(sections, ycsbSection{
				title:       m[1],
				description: m[2],
			})
			current = &sections[len(sections)-1]
			continue
		}
		if current == nil || !strings.HasPrefix(line, "|") {
			continue
		}
		if strings.Contains(line, "Engine") || strings.HasPrefix(line, "|---") {
			continue
		}

		fields := splitMarkdownRow(line)
		if len(fields) != 6 {
			continue
		}
		current.rows = append(current.rows, ycsbRow{
			engine: fields[0],
			p50:    fields[1],
			p95:    fields[2],
			p99:    fields[3],
			avg:    fields[4],
			max:    fields[5],
		})
	}
	return sections
}

func parseCompressionRatioOutput(out string) compressionRatioSummary {
	summary := compressionRatioSummary{}
	for _, line := range strings.Split(out, "\n") {
		line = stripGoTestLogPrefix(strings.TrimSpace(line))
		if strings.HasPrefix(line, "samples=") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				summary.samples = strings.TrimPrefix(fields[0], "samples=")
				summary.avgRaw = strings.TrimPrefix(fields[1], "avg_raw=")
				summary.avgRaw = strings.TrimSuffix(summary.avgRaw, "B") + " B"
			}
			continue
		}
		m := compressionRE.FindStringSubmatch(line)
		if len(m) == 0 {
			continue
		}
		summary.rows = append(summary.rows, compressionRatioRow{
			codec:         m[1],
			avgCompressed: m[2] + " B",
			ratio:         m[3],
			saved:         m[4] + "%",
		})
	}
	return summary
}

func writeThroughputSection(out *bytes.Buffer, samples map[string][]benchSample) {
	out.WriteString("## Throughput\n\n")
	writeBenchTable(out, "### Put", []benchSpec{
		{name: "BenchmarkOneLeafPutAutoMerge5MB", engine: "OneLeaf", mode: "raw"},
		{name: "BenchmarkOneLeafPutAutoMerge5MBCompressed", engine: "OneLeaf", mode: "LZ4Dict4KB"},
		{name: "BenchmarkOneLeafPutAutoMerge5MBCompressedWAL", engine: "OneLeaf", mode: "LZ4Dict4KB + async WAL"},
		{name: "BenchmarkPebblePutNoSync", engine: "Pebble", mode: "NoSync"},
	}, samples)
	writeBenchTable(out, "### Read", []benchSpec{
		{name: "BenchmarkOneLeafGet64K", engine: "OneLeaf", mode: "raw, 64K keys"},
		{name: "BenchmarkOneLeafGet64KCompressed", engine: "OneLeaf", mode: "LZ4Dict4KB, 64K keys"},
		{name: "BenchmarkPebbleGet64K", engine: "Pebble", mode: "5 MB block cache, 64K keys"},
		{name: "BenchmarkPebbleGet64KNoBlockCache", engine: "Pebble", mode: "no block cache, 64K keys"},
	}, samples)
	writeBenchTable(out, "### Mixed Put/Get", []benchSpec{
		{name: "BenchmarkOneLeafMixedPutGet5MB", engine: "OneLeaf", mode: "raw"},
		{name: "BenchmarkOneLeafMixedPutGet5MBCompressed", engine: "OneLeaf", mode: "LZ4Dict4KB"},
		{name: "BenchmarkPebbleMixedPutGetNoSync", engine: "Pebble", mode: "NoSync"},
	}, samples)
	writeBenchTable(out, "### Open + Get", []benchSpec{
		{name: "BenchmarkOneLeafOpenGet64K", engine: "OneLeaf", mode: "open DB + get, 64K keys"},
		{name: "BenchmarkPebbleOpenGet64KNoBlockCache", engine: "Pebble", mode: "open DB + get, no block cache"},
	}, samples)
	out.WriteString("\n")
}

func writeLatencySection(out *bytes.Buffer, rows map[string]latencyRow) {
	out.WriteString("## Latency\n\n")
	out.WriteString("Columns always ordered as `p50`, `p95`, `p99`, `avg`, `max`.\n\n")
	writeLatencyTable(out, "### Write", []latencySpec{
		{name: "oneleaf_put_auto_merge_10mb", engine: "OneLeaf", mode: "raw"},
		{name: "oneleaf_put_auto_merge_10mb_compressed", engine: "OneLeaf", mode: "LZ4Dict4KB"},
		{name: "oneleaf_put_auto_merge_10mb_compressed_wal", engine: "OneLeaf", mode: "LZ4Dict4KB + async WAL"},
		{name: "pebble_put_nosync_memtable_10mb", engine: "Pebble", mode: "NoSync"},
	}, rows)
	writeLatencyTable(out, "### Read", []latencySpec{
		{name: "oneleaf_read_64k", engine: "OneLeaf", mode: "raw, 64K keys"},
		{name: "oneleaf_read_64k_compressed", engine: "OneLeaf", mode: "LZ4Dict4KB, 64K keys"},
		{name: "pebble_read_64k_cached", engine: "Pebble", mode: "5 MB block cache, 64K keys"},
		{name: "pebble_read_64k_no_block_cache", engine: "Pebble", mode: "no block cache, 64K keys"},
	}, rows)
	out.WriteString("### Rate Limiter\n\n")
	out.WriteString("Workload: `95% Inc`, `5% config update`.\n\n")
	writeLatencyTable(out, "", []latencySpec{
		{name: "oneleaf_rate_limiter_95inc_5update_10mb", engine: "OneLeaf", mode: "raw"},
		{name: "oneleaf_rate_limiter_95inc_5update_10mb_compressed", engine: "OneLeaf", mode: "LZ4Dict4KB"},
		{name: "oneleaf_rate_limiter_95inc_5update_10mb_compressed_wal", engine: "OneLeaf", mode: "LZ4Dict4KB + async WAL"},
		{name: "pebble_rate_limiter_95inc_5update_10mb", engine: "Pebble", mode: "NoSync"},
	}, rows)
	out.WriteString("\n")
}

func writeYCSBSection(out *bytes.Buffer, sections []ycsbSection) {
	if len(sections) == 0 {
		return
	}

	out.WriteString("## Real go-ycsb Core Latency\n\n")
	out.WriteString("These rows use `github.com/pingcap/go-ycsb v1.0.3` Core workload generation.\n\n")
	for _, section := range sections {
		out.WriteString(fmt.Sprintf("### %s\n\n", section.title))
		out.WriteString(fmt.Sprintf("Workload: `%s`.\n\n", section.description))
		out.WriteString("| Engine | p50 | p95 | p99 | avg | max |\n")
		out.WriteString("|---|---:|---:|---:|---:|---:|\n")
		for _, row := range section.rows {
			out.WriteString(fmt.Sprintf("| %s | `%s` | `%s` | `%s` | `%s` | `%s` |\n",
				row.engine, row.p50, row.p95, row.p99, row.avg, row.max))
		}
		out.WriteString("\n")
	}
}

func writeCompressionRatioSection(out *bytes.Buffer, summary compressionRatioSummary) {
	if len(summary.rows) == 0 {
		return
	}

	out.WriteString("## 4KB Compression Probe\n\n")
	if summary.samples != "" && summary.avgRaw != "" {
		out.WriteString(fmt.Sprintf("Input corpus: `%s` blocks, avg raw block `%s`.\n\n", summary.samples, summary.avgRaw))
	}
	out.WriteString("### Ratio\n\n")
	out.WriteString("| Codec | Avg compressed | Ratio | Saved |\n")
	out.WriteString("|---|---:|---:|---:|\n")
	for _, row := range summary.rows {
		out.WriteString(fmt.Sprintf("| %s | `%s` | `%s` | `%s` |\n", row.codec, row.avgCompressed, row.ratio, row.saved))
	}
	out.WriteString("\n")
}

func writeCompressionSpeedSection(out *bytes.Buffer, samples map[string][]benchSample) {
	if len(samples) == 0 {
		return
	}

	codecs := []string{"LZ4Dict4KB", "LZ4Dict8KB", "LZ4Dict16KB", "SnappyNoDict", "S2Dict4KB", "S2Dict8KB", "S2Dict16KB"}
	out.WriteString("### Compression Speed\n\n")
	out.WriteString("Average of 5 benchmark runs.\n\n")
	out.WriteString("| Codec | ns/op avg | ns/op range | MB/s avg | B/op | allocs/op |\n")
	out.WriteString("|---|---:|---:|---:|---:|---:|\n")
	for _, codec := range codecs {
		name := "BenchmarkCompression4KCompress/" + codec
		s := samples[name]
		if len(s) == 0 {
			continue
		}
		out.WriteString(fmt.Sprintf("| %s | `%s` | `%s` | `%s` | `%s` | `%s` |\n",
			codec,
			averageBenchSeries(s, func(v benchSample) float64 { return v.ns }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.ns }),
			averageBenchSeries(s, func(v benchSample) float64 { return v.mbps }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.bytes }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.allocs }),
		))
	}
	out.WriteString("\n### Decompression Speed\n\n")
	out.WriteString("Average of 5 benchmark runs.\n\n")
	out.WriteString("| Codec | ns/op avg | ns/op range | MB/s avg | B/op | allocs/op |\n")
	out.WriteString("|---|---:|---:|---:|---:|---:|\n")
	for _, codec := range codecs {
		name := "BenchmarkCompression4KDecompress/" + codec
		s := samples[name]
		if len(s) == 0 {
			continue
		}
		out.WriteString(fmt.Sprintf("| %s | `%s` | `%s` | `%s` | `%s` | `%s` |\n",
			codec,
			averageBenchSeries(s, func(v benchSample) float64 { return v.ns }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.ns }),
			averageBenchSeries(s, func(v benchSample) float64 { return v.mbps }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.bytes }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.allocs }),
		))
	}
	out.WriteString("\n")
}

func writeBenchTable(out *bytes.Buffer, title string, specs []benchSpec, samples map[string][]benchSample) {
	out.WriteString(title + "\n\n")
	out.WriteString("| Engine | Mode | ns/op | B/op | allocs/op |\n")
	out.WriteString("|---|---|---:|---:|---:|\n")
	for _, spec := range specs {
		s := samples[spec.name]
		if len(s) == 0 {
			continue
		}
		out.WriteString(fmt.Sprintf("| %s | %s | `%s` | `%s` | `%s` |\n",
			spec.engine,
			spec.mode,
			rangeBenchSeries(s, func(v benchSample) float64 { return v.ns }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.bytes }),
			rangeBenchSeries(s, func(v benchSample) float64 { return v.allocs }),
		))
	}
	out.WriteString("\n")
}

func writeLatencyTable(out *bytes.Buffer, title string, specs []latencySpec, rows map[string]latencyRow) {
	if title != "" {
		out.WriteString(title + "\n\n")
	}
	out.WriteString("| Engine | Mode | p50 | p95 | p99 | avg | max |\n")
	out.WriteString("|---|---|---:|---:|---:|---:|---:|\n")
	for _, spec := range specs {
		row, ok := rows[spec.name]
		if !ok {
			continue
		}
		out.WriteString(fmt.Sprintf("| %s | %s | `%s` | `%s` | `%s` | `%s` | `%s` |\n",
			spec.engine, spec.mode, row.p50, row.p95, row.p99, row.avg, row.max))
	}
	out.WriteString("\n")
}

func rangeBenchSeries(samples []benchSample, pick func(benchSample) float64) string {
	if len(samples) == 0 {
		return "-"
	}
	min := pick(samples[0])
	max := min
	for _, sample := range samples[1:] {
		v := pick(sample)
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	if len(samples) == 1 || min == max {
		return trimFloat(min)
	}
	return fmt.Sprintf("%s-%s", trimFloat(min), trimFloat(max))
}

func averageBenchSeries(samples []benchSample, pick func(benchSample) float64) string {
	if len(samples) == 0 {
		return "-"
	}
	var total float64
	for _, sample := range samples {
		total += pick(sample)
	}
	return trimFloatRounded(total/float64(len(samples)), 1)
}

func trimFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func trimFloatRounded(v float64, precision int) string {
	return strconv.FormatFloat(math.Round(v*math.Pow10(precision))/math.Pow10(precision), 'f', -1, 64)
}

func mustParseFloat(v string) float64 {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(err)
	}
	return f
}

func splitMarkdownRow(line string) []string {
	parts := strings.Split(line, "|")
	fields := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields = append(fields, strings.Trim(part, "`"))
	}
	return fields
}

func stripGoTestLogPrefix(line string) string {
	if m := logPrefixRE.FindStringSubmatch(line); len(m) > 0 {
		return strings.TrimSpace(m[1])
	}
	return line
}
