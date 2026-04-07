<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# etcd Member Lifecycle

etcd-steward implements the initialization state machine defined in [DEP-04: EtcdMember Custom Resource](https://github.com/gardener/etcd-druid/blob/master/docs/proposals/04-etcd-member-custom-resource.md).

## State machine

Each etcd member transitions through the following states. Every transition is recorded synchronously to `EtcdMember.status.transitions` before control flow advances.

```
New ──────────────────────────────────────────────────────────────────────────────┐
 │  reason: NewSingleNodeClusterCreated | ClusterScaledUp | DataLossRecoveryStarted│
 │                                                                                  │
 ├─[scale-up: CreateAsLearner annotation]──→ Initializing (Reason: ClusterScaledUp)│
 │                                               └─→ Starting/PendingLearner        │
 │                                                       └─→ Starting/Learner       │
 │                                                               └─→ Started/Follower│
 │                                                                                  │
 ├─[data dir valid, or fresh single-node]──→ Initializing/DBValidationSanity|Full  │
 │                                               ├─[valid] Started/Leader|Follower  │
 │                                               └─[invalid, single-node]           │
 │                                                     Initializing/Restoration     │
 │                                                          └─→ Started/Leader      │
 │                                                                                  │
 └─[multi-node, invalid DB]──→ New (DataLossRecoveryStarted)──→ Starting/PendingLearner─┘
                                                                    └─→ Starting/Learner
                                                                            └─→ Started/Follower
```

## States and sub-states

| State | Sub-State | Description |
|-------|-----------|-------------|
| `New` | — | Member just created or reset after data-loss detection |
| `Initializing` | `DBValidationSanity` | Sanity DB open check (clean exit detected) |
| `Initializing` | `DBValidationFull` | Full bbolt integrity scan (crash/kill detected) |
| `Initializing` | `Restoration` | Restoring from snapshot (single-node, validation failed) |
| `Starting` | `PendingLearner` | Waiting for cluster to accept learner add request |
| `Starting` | `Learner` | Learner added, syncing from leader |
| `Started` | `Follower` | Voting follower; after promotion or leader election loss |
| `Started` | `Leader` | Elected cluster leader |

## Reason codes

| Reason | From state | To state |
|--------|-----------|---------|
| `NewSingleNodeClusterCreated` | (startup) | `New` |
| `ClusterScaledUp` | (startup) | `New` → `Initializing` |
| `DataLossRecoveryStarted` | (DB corrupt / PVC replaced) | `New` |
| `DetectedPreviousCleanExit` | `New` | `Initializing/DBValidationSanity` |
| `DetectedPreviousUncleanExit` | `New` | `Initializing/DBValidationFull` |
| `DBValidationSucceeded` | `Initializing` | `Started/Leader` or `Started/Follower` |
| `DBValidationFailed` | `Initializing` | `Initializing/Restoration` (single-node) or `New` (multi-node) |
| `RestorationSucceeded` | `Initializing/Restoration` | `Started/Leader` |
| `WaitingToJoinAsLearner` | `New`/`Initializing` | `Starting/PendingLearner` |
| `JoinedAsLearner` | `Starting/PendingLearner` | `Starting/Learner` |
| `PromotedAsVotingMember` | `Starting/Learner` | `Started/Follower` |
| `GainedClusterLeadership` | `Started/Follower` | `Started/Leader` |
| `LostClusterLeadership` | `Started/Leader` | `Started/Follower` |

## Initialization paths

### Path A — Existing valid data

The data directory exists and the etcd DB passes validation (sanity or full). etcd-steward transitions `New → Initializing/DBValidation* → Started/Leader|Follower` and signals `Successful`. etcd-wrapper starts etcd with `initial-cluster-state: existing`.

### Path B — New member join (scale-up)

The `druid.gardener.cloud/create-as-learner` annotation is present on the `EtcdMember`. etcd-steward records `New → Initializing → Starting/PendingLearner`, calls the etcd member add API to add this member as a learner, waits for the learner to sync to the leader's revision, promotes it to a full voting member (`Started/Follower`), then removes the annotation.

### Path C — Fresh single-node bootstrap

The data directory is empty, no snapshots are available (or backup is not configured), and the cluster has one member. etcd-steward transitions `New → Initializing/DBValidationFull → Started/Leader` and signals `Successful`. etcd-wrapper starts etcd with `initial-cluster-state: new`.

### Path D — Restore from snapshot

The data directory is empty and snapshots are available in the backup store. etcd-steward transitions `New → Initializing/Restoration`, downloads the latest full snapshot, writes the restored DB to the data directory, then records `Started/Leader` and updates `EtcdMember.status.lastRestoration`.

Delta snapshot replay is not yet implemented — restoration uses only the latest full snapshot.

### Data-loss recovery (multi-node)

When a multi-node member's DB fails validation (corrupt or missing after PVC replacement), etcd-steward:
1. Records `New` with reason `DataLossRecoveryStarted`
2. Removes the data directory
3. Calls `RemoveStaleMember` on the cluster to remove the stale entry
4. Proceeds with Path B (learner join)

This prevents an infinite restart loop where the member repeatedly fails validation.

## Reachability detection

Before deciding between paths, etcd-steward checks whether any cluster peers are reachable:

1. Parses `initial-cluster` to extract peer URLs for all members except self.
2. Converts peer port 2380 → client port 2379.
3. TCP-dials each with a 3-second timeout.
4. If any peer responds → cluster exists → use Path B or data-loss recovery.
5. If no peer responds → fresh cluster → use Path C or Path D.

This avoids the gRPC connection hang when no etcd is yet listening.

## State persistence and clean exit marker

Every transition is written synchronously to `EtcdMember.status.transitions` via Kubernetes merge-patch before the next step begins. A maximum of 100 transitions are retained.

On graceful shutdown (SIGTERM), etcd-steward writes a clean exit marker to the data directory. Its presence on the next startup triggers `DBValidationSanity` (cheap open check) instead of `DBValidationFull` (full bbolt integrity scan).
