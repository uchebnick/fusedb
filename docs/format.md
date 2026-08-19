# On-disk compatibility

FuseDB persists a checksummed `FORMAT` descriptor in every database directory.
It is a compatibility gate, not yet a promise that every experimental release
will support every previous layout.

## Descriptor

`FORMAT` records:

- the database format epoch;
- the minimum reader and writer epochs;
- required feature bits, which every opener must understand;
- optional feature bits, which older readers may ignore.

The current epoch is `3`. Its required features are manifest v4, exact
per-leaf WAL watermarks, persistent dictionary-group assignments, and the
external dictionary catalog, bounded record decoding, and atomic WAL batches.
Persistent scheduler history is optional because it can be discarded and
relearned without changing user data.

Epoch 3 adds WAL record kind 4. It contains at most 4,096 validated Put,
Delete, or Inc mutations under one sequence number and checksum. During replay,
each member is compared with the watermark of its own leaf, so a crash after
only some leaves were independently merged neither loses nor duplicates the
remaining members.

## Bounded decoding contract

Epoch 2 introduced hard availability ceilings checked before allocation on both
the live write path and every persisted decoder:

| Item | Maximum |
|---|---:|
| Key | 1 MiB |
| User byte value | 64 MiB |
| LZ4 dictionary | 64 KiB |
| Keys recorded for one segment | 16,777,216 |
| Encoded manifest | 64 MiB |
| Dictionary-group catalog | 32 MiB |
| Segment index or bloom section | 64 MiB each |

`Put`, `Delete`, and `Inc` reject an oversized key before touching the WAL;
`Put` does the same for an oversized value. Disk-derived lengths are compared
as unsigned values and validated against the containing buffer before any
conversion to `int`, slice operation, or allocation. Oversized metadata fails
open instead of risking OOM or a panic.

`DB.Format()` returns the descriptor accepted by the open handle. `DB.Verify`
rereads and checksums it, and every new backup includes it.

## Open rules

Open performs format negotiation while holding the directory lock and before
creating or rewriting database metadata:

- a newer epoch returns `fusedb.ErrFormatTooNew`;
- an older unsupported epoch returns `fusedb.ErrFormatTooOld`;
- an unknown required bit returns `fusedb.ErrUnknownRequiredFeature`;
- a required bit missing from the declared epoch returns
  `fusedb.ErrMissingRequiredFeature`;
- an invalid checksum returns `fusedb.ErrFormatCorrupt`;
- a current descriptor paired with a legacy manifest or missing required
  catalog returns `fusedb.ErrFormatMismatch`.

Unknown optional bits do not block open. This is intentional: optional files
such as `SCHEDULER-STATE` affect performance learning, not data interpretation.

## Legacy migration

A database without `FORMAT` is treated as a pre-descriptor database. Manifest
v2 and v3 are decoded and upgraded once:

1. validate the legacy manifest and referenced segments;
2. create or migrate the dictionary catalog;
3. atomically rewrite `MANIFEST` as v4;
4. atomically publish the current `FORMAT` descriptor last.

Manifest v2's global replay watermark initializes every leaf watermark.
Manifest v3 already has per-leaf watermarks; its leaves start in dictionary
group 1 until the normal contiguous group rebalance runs.

Epoch-1 and epoch-2 directories are opened under the bounds understood by their
descriptor, including a complete WAL scan, before `FORMAT` is atomically
advanced to epoch 3. Publishing the new descriptor is the final migration step;
invalid persisted data leaves the previous marker unchanged.

Publishing `FORMAT` last makes the migration retryable. A crash before that
point leaves a legacy directory that can be migrated again. A directory-sync
failure after the rename returns `fusedb.ErrCommitUncertain`; close and reopen
before retrying, because the complete descriptor may already be visible.

Backups created before `FORMAT` existed remain restorable when their manifest
version is supported. The restored directory is migrated on first open.

## Upgrade and downgrade policy

Before upgrading an application, make and restore-test a backup with the old
binary, then test a copy with the new binary. Once a descriptor or manifest is
upgraded, do not open that directory with an older binary unless that binary
explicitly documents support for the epoch and required bits.

Binaries released before the `FORMAT` gate cannot be forced retroactively to
honor it. Therefore the project still does not claim general downgrade safety
or indefinite cross-release compatibility. Pin the FuseDB version for pilots
and preserve a rebuildable source of truth.
