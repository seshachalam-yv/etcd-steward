<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# etcd Member Lifecycle

etcd-steward implements the initialization state machine defined in [DEP-04: EtcdMember Custom Resource](https://github.com/gardener/etcd-druid/blob/master/docs/proposals/04-etcd-member-custom-resource.md).

## State machine

Each etcd member begins in the `New` state when its pod starts. The member moves through the following states:

```
New
 │
 ├─[data dir empty, no snapshots, single-node] ──→ Bootstrapping (Path C)
 │                                                  └─→ Ready
 │
 ├─[data dir empty, no snapshots, multi-node]  ──→ WaitingAsLearner (Path B)
 │                                                  └─→ [promoted] Ready
 │
 ├─[data dir empty, snapshots available]       ──→ Restoring
 │                                                  └─→ Ready
 │
 ├─[data dir exists, valid DB]                 ──→ Ready (Path A)
 │
 └─[data dir exists, corrupt DB]              ──→ DataLossRecovery
                                                   └─→ Restoring → Ready
```

## Initialization paths

### Path A — Existing valid data

The data directory exists and the etcd DB passes validation. etcd-steward signals `Successful` immediately. etcd-wrapper starts etcd with `initial-cluster-state: existing`.

### Path B — New member join (learner)

The data directory is empty and the cluster already has quorum (detected by TCP-probing peer endpoints). etcd-steward uses the etcd member add API to add this member as a learner, waits for the learner to sync to the leader's revision, then promotes it to a full voting member.

### Path C — Fresh single-node bootstrap

The data directory is empty, no snapshots are available (or backup is not configured), and the cluster has one member. etcd-steward signals `Successful` and etcd-wrapper starts etcd with `initial-cluster-state: new`.

### Path D — Restore from snapshot

The data directory is empty and snapshots are available in the backup store. etcd-steward downloads the latest full snapshot, applies delta snapshots on top of it using an embedded etcd, and writes the final DB to the data directory.

## Reachability detection for multi-node

Before deciding between Path B and Path C, etcd-steward checks whether any cluster peers are reachable:

1. Parses `initial-cluster` to extract peer URLs for all members except self.
2. Converts peer URLs (port 2380) to client URLs (port 2379).
3. Attempts a TCP dial to each client URL with a 3-second timeout.
4. If any peer is reachable → cluster exists → use Path B.
5. If no peer is reachable → fresh cluster → use Path C or Path D.

This avoids the gRPC connection hang that occurs when the etcd endpoint is not yet listening.

## State persistence

Every state transition is persisted synchronously to `EtcdMember.status.transitions` via the Kubernetes API before control flow advances. This ensures deterministic recovery upon pod restart: etcd-steward reads the last recorded transition and resumes from the correct state.

## Clean exit marker

On graceful shutdown (SIGTERM), etcd-steward writes a marker file to the data directory. On the next startup, the presence of this file informs the validator that the previous shutdown was clean, enabling Path A instead of triggering a full validation scan.
