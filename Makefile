.PHONY: all check check-format test test-nocgo test-benchmarks test-crash test-fuzz test-monitoring test-qualification qualification test-coverage bench bench-latency bench-ycsb security vulncheck license-check sbom lint fmt clean build tidy ci help

FUZZTIME ?= 5s
PROMETHEUS_IMAGE ?= prom/prometheus:v3.14.0@sha256:5ce7540c3c00ef4ab0c9d2c995c6a5b9c421f44b4a115d97a2c7af3b1c21cbb0
GOVULNCHECK_VERSION ?= v1.7.0
GO_LICENSES_VERSION ?= v2.0.1
CYCLONEDX_GOMOD_VERSION ?= v1.10.0
SBOM_DIR ?= dist
GO_FILES := $(shell git ls-files -co --exclude-standard -- '*.go' | while IFS= read -r file; do test ! -f "$$file" || printf '%s\n' "$$file"; done)

all: check

check: check-format lint build test

check-format:
	@test -z "$$(gofmt -l $(GO_FILES))"
	@test -z "$$(goimports -l $(GO_FILES))"

test:
	go test -v -race -coverprofile=coverage.txt ./...

test-nocgo:
	CGO_ENABLED=0 go test ./internal/compression -run '^TestDictionaryReportsCGORequirement$$'
	CGO_ENABLED=0 go build ./...

test-benchmarks:
	cd benchmarks && go test ./...

test-crash:
	go test -run='^TestProcessCrashMatrix$$' -count=3 -timeout=5m ./pkg/oneleafdb

test-fuzz:
	go test -run='^$$' -fuzz=FuzzSegmentMetadataDecodersNeverPanic -fuzztime=$(FUZZTIME) ./internal/segment
	go test -run='^$$' -fuzz=FuzzDecodeDictionaryNeverPanics -fuzztime=$(FUZZTIME) ./internal/compression
	go test -run='^$$' -fuzz=FuzzIterateNeverPanics -fuzztime=$(FUZZTIME) ./internal/wal
	go test -run='^$$' -fuzz=FuzzDecodeBatchNeverPanics -fuzztime=$(FUZZTIME) ./internal/wal
	go test -run='^$$' -fuzz=FuzzDecodeManifestNeverPanics -fuzztime=$(FUZZTIME) ./internal/manifest
	go test -run='^$$' -fuzz=FuzzInspectNeverPanics -fuzztime=$(FUZZTIME) ./internal/backup

test-monitoring:
	docker run --rm --entrypoint /bin/promtool -v "$(CURDIR):/workspace:ro" $(PROMETHEUS_IMAGE) check rules /workspace/monitoring/prometheus/fusedb.rules.yml
	go test ./monitoring/... ./pkg/fusedb/prometheus

test-qualification:
	go test -race -count=3 ./internal/qualification
	go test ./cmd/fusedb-qualify

security: vulncheck license-check

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

license-check:
	go run github.com/google/go-licenses/v2@$(GO_LICENSES_VERSION) check --include_tests --ignore github.com/uchebnick/fusedb --disallowed_types=forbidden,restricted,unknown ./...

sbom:
	mkdir -p "$(SBOM_DIR)"
	go run github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@$(CYCLONEDX_GOMOD_VERSION) mod -json -licenses -type library -noserial -notimestamp -output "$(SBOM_DIR)/fusedb.cdx.json" .
	test -s "$(SBOM_DIR)/fusedb.cdx.json"

qualification:
	@test -n "$(DIR)" || (echo "DIR is required: make qualification DIR=/new/path" >&2; exit 1)
	go run ./cmd/fusedb-qualify -dir "$(DIR)"

test-coverage: test
	go tool cover -html=coverage.txt -o coverage.html

bench:
	cd benchmarks && go test -run=^$$ -bench=. -benchmem -benchtime=2s ./oneleafdb

bench-latency:
	cd benchmarks && FUSEDB_LATENCY_PROBE=1 go test -run=Latency -v ./oneleafdb

bench-ycsb:
	cd benchmarks && FUSEDB_REAL_YCSB=1 go test -run=GoYCSB -v ./oneleafdb

lint:
	golangci-lint run --timeout=5m

fmt:
	gofmt -s -w $(GO_FILES)
	goimports -w $(GO_FILES)

clean:
	rm -f coverage.txt coverage.html
	rm -rf .gocache
	go clean -cache -testcache

build:
	go build -v ./...

tidy:
	go mod tidy
	cd benchmarks && go mod tidy

ci: check

help:
	@echo "Available targets:"
	@echo "  check            - Verify formatting, lint, build, and tests"
	@echo "  check-format     - Verify gofmt and goimports without modifying files"
	@echo "  test             - Run tests"
	@echo "  test-nocgo       - Verify the explicit no-CGO dictionary contract"
	@echo "  test-benchmarks  - Build and test the isolated benchmark module"
	@echo "  test-crash       - Repeatedly kill and recover at storage commit boundaries"
	@echo "  test-fuzz        - Run bounded persisted-decoder fuzz campaigns"
	@echo "  test-monitoring  - Validate Prometheus rules and Grafana/collector contracts"
	@echo "  test-qualification - Repeat the phased qualification integration under race"
	@echo "  qualification    - Run the default qualification cycle (requires DIR)"
	@echo "  security         - Check reachable vulnerabilities and dependency licenses"
	@echo "  vulncheck        - Scan reachable production code with govulncheck"
	@echo "  license-check    - Reject forbidden, restricted, or unknown licenses"
	@echo "  sbom             - Generate a deterministic CycloneDX library SBOM"
	@echo "  bench            - Run benchmarks"
	@echo "  bench-latency    - Run latency probes"
	@echo "  bench-ycsb       - Run YCSB workloads"
	@echo "  lint             - Run linter"
	@echo "  fmt              - Format code"
	@echo "  clean            - Clean artifacts"
	@echo "  build            - Build packages"
	@echo "  tidy             - Run go mod tidy"
	@echo "  ci               - Run CI checks"
