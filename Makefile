.PHONY: all test bench lint fmt clean help

all: fmt lint test

test:
	go test -v -race -coverprofile=coverage.txt ./...

test-coverage: test
	go tool cover -html=coverage.txt -o coverage.html

bench:
	go test -run=^$$ -bench=. -benchmem -benchtime=2s ./benchmarks/oneleafdb

bench-latency:
	FUSEDB_LATENCY_PROBE=1 go test -run=Latency -v ./benchmarks/oneleafdb

bench-ycsb:
	FUSEDB_REAL_YCSB=1 go test -run=GoYCSB -v ./benchmarks/oneleafdb

lint:
	golangci-lint run --timeout=5m

fmt:
	gofmt -s -w .
	goimports -w .

clean:
	rm -f coverage.txt coverage.html
	rm -rf .gocache
	go clean -cache -testcache

build:
	go build -v ./...

tidy:
	go mod tidy

ci: fmt lint test

help:
	@echo "Available targets:"
	@echo "  test             - Run tests"
	@echo "  bench            - Run benchmarks"
	@echo "  bench-latency    - Run latency probes"
	@echo "  bench-ycsb       - Run YCSB workloads"
	@echo "  lint             - Run linter"
	@echo "  fmt              - Format code"
	@echo "  clean            - Clean artifacts"
	@echo "  build            - Build packages"
	@echo "  tidy             - Run go mod tidy"
	@echo "  ci               - Run CI checks"
