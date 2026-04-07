<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Component Reference

etcd-steward is composed of independent components, each enabled/disabled via configuration. This document describes what each component does, what it requires, and what it produces.

---

## HTTP Server

**Package:** `pkg/server`  
**Always on:** yes — starts before any other component  
**Config flag:** `--server-port` (default `8080`), `--server-cert` / `--server-key` for TLS

The HTTP server runs from the moment the process starts. It exposes endpoints that etcd-wrapper and etcd-druid depend on.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/initialization/start` | GET | Triggers initialization; returns immediately with current status |
| `/initialization/start` | POST | Triggers initialization; blocks until `Successful` or `Failed` |
| `/initialization/status` | GET | Returns current status: `New`, `InProgress`, `Successful` |
| `/config` | GET | Returns the etcd YAML configuration from disk |
| `/snapshot/full` | POST | Triggers an on-demand full snapshot |
| `/snapshot/delta` | POST | Triggers an on-demand delta snapshot |
| `/snapshot/latest` | GET | Returns metadata of the latest snapshot |
| `/healthz` | GET | Returns `200 ok`; returns `503` during graceful shutdown |
| `/metrics` | GET | Prometheus metrics |

---

## Initializer

**Package:** `pkg/initializer`  
**Always on:** yes  
**Triggered by:** etcd-wrapper calling `POST /initialization/start`

Orchestrates the DEP-04 etcd member lifecycle. Chooses one of four initialization paths based on the state of the data directory, snapshot store, and cluster:

| Path | Condition | Action |
|------|-----------|--------|
| A | Data dir exists, DB valid | Signal `Successful` immediately |
| B | Data dir empty, cluster has quorum | Join as learner, wait for sync, promote |
| C | Data dir empty, no snapshots, single-node | Signal `Successful` (fresh bootstrap) |
| D | Data dir empty, snapshots available | Download + restore from latest snapshot |

See [etcd Member Lifecycle](etcd-member-lifecycle.md) for the full state machine.

---

## Validator

**Package:** `pkg/validator`  
**Always on:** yes (called by initializer)

Validates the etcd bbolt database before deciding on an initialization path.

| Mode | Trigger | Check performed |
|------|---------|-----------------|
| Sanity | Clean exit marker present | Opens DB; verifies it is not locked |
| Full | No exit marker (crash/kill) | Opens DB + runs bbolt `tx.Check()` integrity scan |

A **clean exit marker** is written to the data directory on graceful shutdown (SIGTERM). Its presence on the next startup allows the cheaper sanity check rather than a full integrity scan.

---

## Leader Watcher

**Package:** `pkg/leaderwatch`  
**Config flag:** component is always started when etcd client is available

Polls the etcd `Status` API at a configured interval (default 10s). Detects when this member becomes leader or loses leadership. Records the role change as a state transition in `EtcdMember.status`.

The `IsLeader()` function it exposes is used as a gate by:
- Snapshotter (leader-only snapshots)
- Garbage collector (leader-only GC)

---

## Member Lease Renewer

**Package:** `pkg/lease`  
**Config flag:** `--enable-member-lease-renewal` (default `true`), `--k8s-heartbeat-duration` (default `10s`)

Renews the Kubernetes `Lease` resource for this member on every heartbeat interval. The lease TTL is set to 3× the heartbeat duration, so etcd-druid considers a member dead if it misses 3 consecutive renewals.

The lease `holderIdentity` encodes `{memberID}:{clusterID}:{role}`, giving etcd-druid the information needed to compute cluster health without querying etcd directly.

---

## EtcdMember Status Updater

**Package:** `pkg/member`, `pkg/statemachine`  
**Always on:** yes

Two distinct update paths:

| Type | When | Mechanism |
|------|------|-----------|
| **Synchronous** | State transitions during initialization | K8s patch must complete before next step proceeds |
| **Asynchronous** | Role changes detected by leaderwatch | K8s patch on best-effort basis |

`EtcdMember.status` is owned exclusively by etcd-steward. etcd-druid reads it but never writes it.

---

## Snapshotter

**Package:** `pkg/snapshotter`  
**Config flag:** `--storage-provider` (set automatically when backup is configured)  
**Leader-only:** yes

Takes periodic snapshots of the etcd database and uploads them to the configured store.

| Snapshot type | Trigger | Description |
|---------------|---------|-------------|
| Full | Cron schedule (`--schedule`) | Complete etcd DB snapshot |
| Delta | Period (`--delta-snapshot-period`) or size limit | MVCC event log since last full snapshot |

A full snapshot can be marked `isFinal` (via `POST /snapshot/full?final=true`), used during Gardener control-plane migration.

---

## Snapstore

**Package:** `pkg/snapstore`  
**Used by:** Snapshotter, GC, Restoration, copy-backups subcommand

Abstract storage backend for snapshots. Interface:

```go
type Snapstore interface {
    Save(snap Snapshot, r io.ReadCloser) error
    Fetch(snap Snapshot) (io.ReadCloser, error)
    List() ([]Snapshot, error)
    Delete(snap Snapshot) error
}
```

| Provider | Status |
|----------|--------|
| `Local` | ✅ Implemented |
| `S3` | ⚠️ Not yet implemented |
| `GCS` | ⚠️ Not yet implemented |
| `ABS` | ⚠️ Not yet implemented |

---

## Compression

**Package:** `pkg/compression`  
**Config flag:** `--compression-policy` (`none`, `gzip`, `zstd`)

Applied to snapshots before upload and reversed on download. Interface:

```go
type Compressor interface {
    Compress(w io.Writer) (io.WriteCloser, error)
    Decompress(r io.Reader) (io.ReadCloser, error)
    FileExtension() string
}
```

| Algorithm | Status | Notes |
|-----------|--------|-------|
| `none` | ✅ | No compression |
| `gzip` | ✅ | Wide compatibility |
| `zstd` | ✅ | Best compression ratio / speed; default |

---

## Restoration

**Package:** `pkg/restoration`  
**Used by:** Initializer (Path D)

Downloads and applies snapshots to restore the etcd database. Current implementation:

1. Lists snapshots; finds the latest full snapshot
2. Fetches and decompresses the full snapshot to a temp file
3. Moves the restored DB to the data directory

Delta snapshot replay (applying MVCC events from incremental snapshots) is not yet implemented.

---

## Garbage Collector

**Package:** `pkg/gc`  
**Config flag:** `--garbage-collection-policy`, `--garbage-collection-period`, `--max-backups`  
**Leader-only:** yes

Groups snapshots into sets (one full snapshot + all deltas until the next full). Retains the most recent N sets and deletes older ones.

| Policy | Config value | Behaviour |
|--------|-------------|-----------|
| LimitBased | `LimitBased` | Retain last N full snapshot sets (N = `--max-backups`) |
| Exponential | `Exponential` | Not yet implemented |

---

## Alarm Handler

**Package:** `pkg/alarm`  
**Config flag:** `alarmCheckInterval` (default `30s`), `compactRevisionLag` (default `1000`)  
**Always on:** yes when etcd client is available

Polls etcd for active alarms and takes action:

| Alarm | Action |
|-------|--------|
| `NOSPACE` | Compact to `currentRevision - compactRevisionLag`, defrag, disarm |
| `CORRUPT` | Log warning; no automatic remediation |
| Other | Log and ignore |

NOSPACE remediation bypasses the distributed lock intentionally — the cluster is in read-only mode and cannot be written to, so etcd-lock-based coordination is impossible.

---

## Distributed Lock

**Package:** `pkg/lock`  
**Used by:** Defragmentation coordinator (when implemented)

An etcd-native distributed lock using lease-grant + atomic transaction. Only one steward instance holds the lock for a given key at a time. Contention is resolved by watching for key deletion and retrying.

```go
l := lock.New(etcdClient, namespace, name)
if err := l.Acquire(ctx); err != nil { ... }
defer l.Release(ctx)
```

---

## Subcommands

### compact

Compacts the etcd revision history to a given revision. Intended to be run as a Kubernetes `Job` after identifying the target revision.

```bash
etcd-steward compact --etcd-endpoint=http://localhost:2379 --revision=1234567
```

### copy-backups

Copies all snapshots from one store location to another. Useful for cross-provider or cross-region migrations.

```bash
etcd-steward copy-backups \
  --source-provider=Local --source-container=/old \
  --dest-provider=Local   --dest-container=/new
```
