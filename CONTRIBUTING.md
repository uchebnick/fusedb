# Contributing to FuseDB

FuseDB is a research-stage storage engine. Small, reviewable changes with a
clear correctness argument are preferred over broad rewrites.

## Prerequisites

- Go 1.25.13 or newer within the supported 1.25 release line
- A C toolchain and native LZ4 development library (`liblz4-dev` or Homebrew `lz4`)
- `golangci-lint` 2.x for `make lint`
- `goimports` for `make fmt`
- Docker for the pinned `promtool` used by `make test-monitoring`

## Development workflow

```bash
git clone https://github.com/uchebnick/fusedb.git
cd fusedb
go mod download
make check
```

Before opening a pull request:

```bash
make fmt
make check
make security
make test-benchmarks
```

`make check` verifies formatting, runs the production-code linters, builds all
packages, and executes the regular test suite. Some benchmark probes need
external services, native libraries, or opt-in environment variables and are
not part of the default check. They live in the independent `benchmarks` Go
module so comparison-only dependencies never enter the production module.

`make security` runs a reachable-code vulnerability scan and rejects
forbidden, restricted, or unknown dependency licenses. `make sbom` creates a
deterministic CycloneDX library inventory in `dist/fusedb.cdx.json`.

## Focused tests

Use the narrowest relevant suite while iterating:

```bash
go test ./internal/skiplist
go test ./internal/segment
go test ./pkg/fusedb
```

For concurrent write-path changes, also run:

```bash
go test -race ./internal/skiplist ./internal/tree ./pkg/fusedb
```

For WAL, segment commit, manifest, or checkpoint changes, run the repeated
OS-filesystem process-kill matrix:

```bash
make test-crash
make test-fuzz
```

`test-fuzz` exercises persisted-data decoders under their allocation budgets.
When adding a length, count, offset, or nested binary structure, add its decoder
to this gate and reject impossible values before allocation.

For collector, alert, recording-rule, or dashboard changes, run:

```bash
make test-monitoring
```

This checks PromQL with the pinned Prometheus tool and validates collector
cardinality/lifecycle plus the importable Grafana JSON contract.

For scheduler admission, merge/checkpoint debt, latency telemetry, or public
resource-budget changes, also run:

```bash
make test-qualification
```

This repeats the real quiet/steady/spike/recovery integration under the race
detector and proves `Verify`, close/reopen, and exact counter recovery. Longer
target-hardware runs are described in
[`docs/qualification.md`](./docs/qualification.md).

Changes to merge, manifest, or WAL behavior should include the durability suite
in `pkg/fusedb/durability_test.go`. Add a behavior test before relying on a
benchmark result.

## Engine invariants

- All engine filesystem access goes through `internal/disk.FS` so the in-memory
  filesystem remains a valid test target.
- Never return internal buffer, cache, block, or pooled memory to callers.
- Do not retain caller-owned byte slices unless an API explicitly transfers
  ownership.
- Deletes remain tombstones until merge; active skiplist nodes are not removed.
- Manifest `AppliedSeq` is exact. A conservative watermark can replay an
  already-materialized increment and silently double it.
- A write rejected by a detached leaf was not applied and must be retried
  against the newly published leaf set.

The full set of design contracts is documented in
[`AGENTS.md`](./AGENTS.md) and [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## Style and pull requests

- Follow standard Go naming and documentation conventions.
- Keep unrelated cleanup out of functional changes.
- Explain the failure mode a correctness test protects against.
- Use conventional commit subjects such as `fix(wal): ...` or
  `docs(readme): ...`.
- Include benchmark methodology, not only headline numbers, for performance
  claims.

By contributing, you agree that your contribution is provided under the
[MIT License](./LICENSE).
