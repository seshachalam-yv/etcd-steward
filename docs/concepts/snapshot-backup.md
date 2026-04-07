<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Snapshot and Backup

## Overview

When a backup store is configured (via `--storage-provider`), etcd-steward manages the full snapshot lifecycle: taking snapshots, compressing them, uploading them to object storage, restoring from them on data loss, and garbage collecting old ones.

```
etcd DB
   │
   ├── Full snapshot (cron schedule)
   │      │ compress (gzip/zstd)
   │      └─→ Snapstore.Save()  ──→  object storage
   │
   └── Delta snapshot (period or size limit)
          │ MVCC events since last full
          └─→ Snapstore.Save()  ──→  object storage
```

## Snapshot naming

Snapshots are named using a fixed format that encodes all metadata needed for listing and restoration:

```
{Kind}-{StartRevision:016d}-{LastRevision:016d}-{UnixNano}[.gz|.zst]
```

Examples:
```
Full-0000000000000001-0000000000001000-1712345678901234567.zst
Incremental-0000000000001001-0000000000001500-1712345679012345678.zst
```

- `Kind`: `Full` or `Incremental`
- `StartRevision`: first etcd revision in this snapshot
- `LastRevision`: last etcd revision in this snapshot
- `UnixNano`: nanosecond timestamp of when the snapshot was taken
- Extension: absent (no compression), `.gz` (gzip), `.zst` (zstd)

## Snapshot sets

A **snapshot set** is one full snapshot plus all incremental snapshots taken after it, up until the next full snapshot. Restoration uses a complete snapshot set: the full snapshot is applied first, then each incremental is applied in order.

```
Full-0000000000000001-0000000000001000-...   ← set 1
Incremental-0000000000001001-...             ← set 1
Incremental-0000000000001501-...             ← set 1
Full-0000000000002000-0000000000003000-...   ← set 2
Incremental-0000000000003001-...             ← set 2
```

## Full snapshots

Full snapshots are triggered by a cron schedule (default: `0 */24 * * *` — daily). They can also be triggered on-demand:

```bash
curl -X POST http://localhost:8080/snapshot/full
```

For Gardener control-plane migration, a final full snapshot must be marked as such:

```bash
curl -X POST "http://localhost:8080/snapshot/full?final=true"
```

A final snapshot signals that no more writes will occur and the backup is safe to use for migration.

Full snapshots are **leader-only**. Follower members do not take snapshots.

## Delta snapshots

Delta snapshots capture MVCC events since the last full snapshot. They are smaller and faster than full snapshots. A delta flush is triggered when either:

- The configured period elapses (`--delta-snapshot-period`, default `1m`), or
- The accumulated in-memory size exceeds `--delta-snapshot-memory-limit` (default 100 MiB)

Delta snapshots can also be triggered on-demand:

```bash
curl -X POST http://localhost:8080/snapshot/delta
```

## Latest snapshot

To query the most recent available snapshot:

```bash
curl http://localhost:8080/snapshot/latest
```

Returns JSON with the latest full and latest incremental snapshot metadata.

## Compression

All snapshots are compressed before upload (default: `zstd`). To change the algorithm:

```
--compress-snapshots=true --compression-policy=gzip
```

| Algorithm | Flag value | Extension | Notes |
|-----------|------------|-----------|-------|
| None | `none` | (none) | No compression |
| gzip | `gzip` | `.gz` | Widely compatible |
| zstd | `zstd` | `.zst` | Best ratio/speed; recommended |

## Garbage collection

Old snapshot sets are deleted automatically by the garbage collector (leader-only). By default the last 7 full snapshot sets are retained (`--max-backups=7`).

GC runs every `--garbage-collection-period` (default `12h`). It processes one GC cycle per run, deleting all snapshot sets beyond the retention count.

## Restoration

When the etcd data directory is empty and snapshots exist in the store, etcd-steward restores the latest snapshot set during initialization:

1. Lists all snapshots; identifies the latest full snapshot
2. Downloads and decompresses the full snapshot to a temp file
3. Moves the restored DB to the data directory
4. Signals initialization `Successful`

Delta snapshot replay (applying incremental snapshots on top of the full) is not yet implemented. Restoration currently uses only the latest full snapshot.

## Storage providers

| Provider | Status | Config |
|----------|--------|--------|
| Local filesystem | ✅ | `--storage-provider=Local --store-container=/path/to/dir` |
| AWS S3 | ⚠️ Planned | `--storage-provider=S3 --store-container=bucket-name` |
| Google Cloud Storage | ⚠️ Planned | `--storage-provider=GCS --store-container=bucket-name` |
| Azure Blob Storage | ⚠️ Planned | `--storage-provider=ABS --store-container=container-name` |

## Snapshot compaction

Over time, many incremental snapshots accumulate between full snapshots. The `compact` subcommand can condense a snapshot set into a single full snapshot for faster restoration:

```bash
etcd-steward compact \
  --etcd-endpoint=http://localhost:2379 \
  --revision=<target-revision>
```

This is typically run as a Kubernetes `Job` via etcd-druid's compaction controller.
