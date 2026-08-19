# Release engineering

FuseDB is a pre-1.0 Go library. This document defines the release mechanics;
a versioned release does not by itself make the database production-supported.

## Supported build surface

| Surface | Contract |
|---|---|
| Public module | `github.com/uchebnick/fusedb/pkg/fusedb` |
| Go toolchain | Go 1.25.13 or a newer patched 1.25 toolchain |
| Runtime OS | Linux and macOS on amd64/arm64 |
| Native dependency | CGO plus LZ4 headers/runtime; every supported OS/architecture is built and tested natively in CI |
| Windows | Not supported: the OS filesystem lock intentionally returns `ErrLockUnsupported` |
| Benchmark tooling | Separate `github.com/uchebnick/fusedb/benchmarks` module; never part of the library dependency graph |
| On-disk compatibility | Governed by [`docs/format.md`](./format.md); no long-term stable epoch promise yet |

The benchmark module uses a local `replace` only for repository development.
It carries Pebble, Badger, RocksDB, and YCSB comparisons without making those
dependencies part of the production module or its SBOM.

## Required gates

Run from a clean checkout:

```bash
make check
make test-nocgo
make test-crash
make test-fuzz FUZZTIME=30s
make test-monitoring
make test-qualification
make test-benchmarks
make security
make sbom
```

`make security` pins and runs `govulncheck` against reachable production code,
then rejects forbidden, restricted, or unknown licenses, including test-only
dependencies. `make sbom` emits a deterministic CycloneDX 1.6 library SBOM at
`dist/fusedb.cdx.json`; serial number and timestamp are omitted so identical
source and module graphs produce comparable output.

CI repeats the production suite under the race detector, validates the isolated
benchmark module, and builds/tests on native Linux and macOS runners. A tag is
release-eligible only after these gates pass.

## Release evidence

For a release candidate, retain:

1. the exact Git tag and Go patch version;
2. `go.sum` and `dist/fusedb.cdx.json`;
3. vulnerability and license-gate logs;
4. the target-hardware qualification JSON report;
5. crash/fuzz/backup-restore drill results;
6. the native LZ4 version and SHA-256 checksums for any distributed binaries.

Consumers should pin a semantic version rather than a branch or pseudo-version.
Before a stable format/support policy exists, upgrades must be treated as data
migrations and exercised on a restored copy first.
