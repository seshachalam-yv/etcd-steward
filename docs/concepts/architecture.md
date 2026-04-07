<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Architecture

## Overview

etcd-steward is a sidecar process that manages a single etcd member. One etcd-steward instance runs per pod, alongside the etcd process (hosted by etcd-wrapper).

```
┌─────────────────── Pod ──────────────────────────────┐
│                                                       │
│  ┌──────────────────────────────────────────────┐    │
│  │                etcd-wrapper                  │    │
│  │  - embeds etcd server                        │    │
│  │  - calls etcd-steward /initialization/start  │    │
│  │  - calls etcd-steward /config to get config  │    │
│  └──────────────────────────────────────────────┘    │
│                        │ HTTP :8080                   │
│  ┌─────────────────────▼────────────────────────┐    │
│  │               etcd-steward                   │    │
│  │                                              │    │
│  │  ┌────────────┐  ┌──────────┐  ┌─────────┐  │    │
│  │  │ initializer│  │snapshotter│  │  alarm  │  │    │
│  │  └────────────┘  └──────────┘  └─────────┘  │    │
│  │  ┌────────────┐  ┌──────────┐  ┌─────────┐  │    │
│  │  │leaderwatch │  │    gc    │  │  lease  │  │    │
│  │  └────────────┘  └──────────┘  └─────────┘  │    │
│  │  ┌────────────┐  ┌──────────┐                │    │
│  │  │   member   │  │   lock   │                │    │
│  │  └────────────┘  └──────────┘                │    │
│  └──────────────────────────────────────────────┘    │
│          │ etcd client          │ K8s API             │
└──────────┼──────────────────────┼────────────────────┘
           ▼                      ▼
      etcd :2379              kube-apiserver
```

## Component model

Each component is independently enabled/disabled via configuration flags. Components communicate via:

- **Shared context**: cancelling the root context stops all components gracefully.
- **Function callbacks**: `isLeader()` passed into snapshotter, GC — no shared state.
- **Interfaces**: each component depends on narrow interfaces (e.g. `StatusAPI`, `MaintenanceAPI`), not concrete types.

Components do not call each other directly. The only shared objects are the etcd client and the K8s dynamic client, both of which are safe for concurrent use.

## HTTP server

The HTTP server starts before initialization begins, so etcd-wrapper can call `/initialization/start` and `/config` immediately. All handlers are registered at startup and remain active for the lifetime of the process.

During shutdown, the server sets `isShutdown = true` (an atomic bool), causing `/healthz` to return 503. The server itself keeps running until the process exits — it does not call `http.Server.Shutdown()` — so in-flight requests complete normally.

## etcd-wrapper contract

etcd-wrapper expects:

1. `GET /config` — returns the etcd YAML config (read from `/var/etcd/config/etcd.conf.yaml` on every request).
2. `POST /initialization/start?mode=<sanity|full>` — blocks until initialization is `Successful` or `Failed`, then returns the status string followed by `\n`.
3. `GET /initialization/status` — returns the current status string.

This contract is identical to `etcd-backup-restore`. etcd-steward is a drop-in replacement.

## Leader-only operations

Snapshots and garbage collection run only on the etcd leader. Leadership is detected by `pkg/leaderwatch`, which polls the etcd `Status` API every 10 seconds. The result is exposed via an `IsLeader() bool` function that snapshotter and GC call before each operation.

This avoids running multiple concurrent snapshot uploads or GC passes in a 3-node cluster.

## Distributed lock

`pkg/lock` implements an etcd-backed distributed lock using:

1. `LeaseGrant` — acquire a lease with a TTL.
2. Atomic `Txn` — `If(createRevision(key) == 0).Then(Put(key, leaseID))` — acquires only if key doesn't exist.
3. `Watch` — if the Txn fails (key exists), watch the key for deletion, then retry.

This is used by the defragmentation coordinator to ensure only one member defragments at a time, preventing quorum loss.

## Snapshot pipeline

```
Leader detects tick/event
       │
       ▼
TriggerFullSnapshot()
       │
       ├─ etcd.Snapshot() → io.Reader
       │
       ├─ compress (gzip/zstd/noop)
       │
       ├─ Snapstore.Save(snap, reader)
       │
       └─ update snapshotlease (druid compatibility)

Delta snapshot loop:
       │
       ├─ watch etcd events since last full snapshot revision
       │
       ├─ accumulate in memory (up to maxDeltaSnapshotSize)
       │
       └─ flush as delta snapshot on period or size limit
```

## Restoration pipeline

```
Download full snapshot from Snapstore
       │
       ▼
Decompress → write to temp file
       │
       ▼
etcd snapshot restore API → temp data dir
       │
       ▼
Apply delta snapshots sequentially (using embedded etcd)
       │
       ▼
Move restored DB to data-dir
       │
       ▼
Signal Successful
```

## EtcdMember status ownership

`EtcdMember.status` is owned exclusively by etcd-steward. etcd-druid reads the status but never writes it. This avoids conflicts and ensures the status always reflects what the sidecar actually observed.

State transitions are written **synchronously** (blocking K8s API call) so that they are durable before the next step in the initialization sequence. Other status fields (snapshots, defragmentation timestamps) are updated **asynchronously**.
