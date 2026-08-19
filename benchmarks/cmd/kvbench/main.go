package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/uchebnick/fusedb/benchmarks/kvbench"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "kvbench: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	profile, err := requestedProfile(args)
	if err != nil {
		return err
	}
	config := kvbench.QuickConfig()
	if profile == "standard" {
		config = kvbench.StandardConfig()
	}

	flags := flag.NewFlagSet("kvbench", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profileFlag := flags.String("profile", profile, "quick or standard")
	enginesFlag := flags.String("engines", strings.Join(config.Engines, ","), "comma-separated engines or all")
	workloadsFlag := flags.String("workloads", strings.Join(config.Workloads, ","), "comma-separated workloads")
	durabilityFlag := flags.String("durability", joinDurabilities(config.Durabilities), "async, sync, or both")
	workersFlag := flags.String("workers", joinInts(config.WorkerCounts), "comma-separated worker counts")
	keysFlag := flags.Int("keys", config.Keys, "number of preloaded keys")
	valueBytesFlag := flags.Int("value-bytes", config.ValueBytes, "value size in bytes")
	cacheBytesFlag := flags.Int64("cache-bytes", config.CacheBytes, "per-engine block/value cache target")
	memtableBytesFlag := flags.Int64("memtable-bytes", config.MemtableBytes, "per-engine memtable/merge target")
	warmupFlag := flags.Duration("warmup", config.Warmup, "warmup time per matrix cell")
	durationFlag := flags.Duration("duration", config.Duration, "measured time per matrix cell")
	repetitionsFlag := flags.Int("repetitions", config.Repetitions, "repetitions per matrix cell")
	seedFlag := flags.Uint64("seed", config.Seed, "deterministic workload seed")
	rootFlag := flags.String("root", "", "temporary database parent directory")
	jsonFlag := flags.String("json", "", "write the full JSON report to this path")
	markdownFlag := flags.String("markdown", "", "write the Markdown report to this path")
	listFlag := flags.Bool("list", false, "list adapters and workloads, then exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *profileFlag != "quick" && *profileFlag != "standard" {
		return fmt.Errorf("unknown profile %q", *profileFlag)
	}
	if *listFlag {
		printCatalog(stdout)
		return nil
	}

	engines := splitCSV(*enginesFlag)
	if len(engines) == 1 && engines[0] == "all" {
		engines = nil
		for _, engine := range kvbench.Engines() {
			engines = append(engines, engine.Name)
		}
	}
	durabilities, err := parseDurabilities(*durabilityFlag)
	if err != nil {
		return err
	}
	workers, err := parseInts(*workersFlag)
	if err != nil {
		return fmt.Errorf("workers: %w", err)
	}

	temporaryRoot := false
	root := *rootFlag
	if root == "" {
		root, err = os.MkdirTemp("", "fusedb-kvbench-")
		if err != nil {
			return fmt.Errorf("create temporary root: %w", err)
		}
		temporaryRoot = true
	}
	if temporaryRoot {
		defer os.Remove(root) // Runner removes every child directory.
	}
	config.RootDir = root
	config.Engines = engines
	config.Workloads = splitCSV(*workloadsFlag)
	config.Durabilities = durabilities
	config.WorkerCounts = workers
	config.Keys = *keysFlag
	config.ValueBytes = *valueBytesFlag
	config.CacheBytes = *cacheBytesFlag
	config.MemtableBytes = *memtableBytesFlag
	config.Warmup = *warmupFlag
	config.Duration = *durationFlag
	config.Repetitions = *repetitionsFlag
	config.Seed = *seedFlag

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	report, runErr := kvbench.Run(ctx, config, stderr)
	if len(report.Results) == 0 {
		return runErr
	}
	report.Command = benchmarkCommand(args)
	jsonData, err := report.JSON()
	if err != nil {
		return errors.Join(runErr, fmt.Errorf("encode JSON report: %w", err))
	}
	markdown := report.Markdown()
	if *jsonFlag != "" {
		if err := writeFile(*jsonFlag, append(jsonData, '\n')); err != nil {
			return errors.Join(runErr, err)
		}
	}
	if *markdownFlag != "" {
		if err := writeFile(*markdownFlag, []byte(markdown)); err != nil {
			return errors.Join(runErr, err)
		}
	}
	if *markdownFlag == "" {
		fmt.Fprint(stdout, markdown)
	}
	return runErr
}

func benchmarkCommand(args []string) []string {
	command := []string{"go", "run"}
	for _, engine := range kvbench.Engines() {
		if engine.Name == "rocksdb" && engine.Available {
			command = append(command, "-tags", "rocksdb")
			break
		}
	}
	command = append(command, "./cmd/kvbench")
	return append(command, args...)
}

func requestedProfile(args []string) (string, error) {
	profile := "quick"
	for index, arg := range args {
		if strings.HasPrefix(arg, "-profile=") {
			profile = strings.TrimPrefix(arg, "-profile=")
		}
		if arg == "-profile" && index+1 < len(args) {
			profile = args[index+1]
		}
	}
	if profile != "quick" && profile != "standard" {
		return "", fmt.Errorf("unknown profile %q", profile)
	}
	return profile, nil
}

func parseDurabilities(value string) ([]kvbench.Durability, error) {
	if value == "both" {
		return []kvbench.Durability{kvbench.DurabilityAsync, kvbench.DurabilitySync}, nil
	}
	values := splitCSV(value)
	result := make([]kvbench.Durability, 0, len(values))
	for _, item := range values {
		durability := kvbench.Durability(item)
		if durability != kvbench.DurabilityAsync && durability != kvbench.DurabilitySync {
			return nil, fmt.Errorf("unknown durability %q", item)
		}
		result = append(result, durability)
	}
	return result, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func parseInts(value string) ([]int, error) {
	parts := splitCSV(value)
	result := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		result = append(result, parsed)
	}
	return result, nil
}

func joinDurabilities(values []kvbench.Durability) string {
	items := make([]string, len(values))
	for index, value := range values {
		items[index] = string(value)
	}
	return strings.Join(items, ",")
}

func joinInts(values []int) string {
	items := make([]string, len(values))
	for index, value := range values {
		items[index] = strconv.Itoa(value)
	}
	return strings.Join(items, ",")
}

func printCatalog(output io.Writer) {
	fmt.Fprintln(output, "Engines:")
	for _, engine := range kvbench.Engines() {
		status := "available"
		if !engine.Available {
			status = engine.Unavailable
		}
		fmt.Fprintf(output, "  %-8s %-18s %s\n", engine.Name, engine.Version, status)
	}
	fmt.Fprintln(output, "Workloads:")
	for _, workload := range kvbench.Workloads() {
		fmt.Fprintf(output, "  %-12s %d%% read / %d%% overwrite\n", workload.Name, workload.ReadPercent, 100-workload.ReadPercent)
	}
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".kvbench-*")
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish output: %w", err)
	}
	return nil
}
