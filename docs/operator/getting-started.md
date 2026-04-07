<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Operator Guide

## Deployment

etcd-steward is deployed automatically by etcd-druid when the `UseEtcdSteward` feature gate is enabled. It runs as the `backup-restore` container in each etcd pod — same container name and port as the original `etcd-backup-restore`, preserving compatibility with etcd-wrapper and etcd-druid controllers.

## Feature gate

To enable etcd-steward in etcd-druid, add to the operator Deployment:

```yaml
args:
  - --feature-gates=UseEtcdSteward=true
```

This is an **alpha** feature gate, disabled by default. It causes etcd-druid to:
- Inject the `etcd-steward` image instead of `etcd-backup-restore`
- Create and manage `EtcdMember` custom resources for each member
- Route compaction requests to `POST /snapshot/full` on etcd-steward

## Health monitoring

### Liveness

etcd-steward uses the Kubernetes member lease for liveness. The lease is renewed every 10 seconds (configurable via `--k8s-heartbeat-duration`). If a member stops renewing its lease, etcd-druid considers it dead.

### Readiness

`GET /healthz` returns `200 ok` while running and `503 Service Unavailable` during graceful shutdown. etcd-wrapper uses this to gate etcd startup.

### Initialization status

`GET /initialization/status` returns the current state:

| Value | Meaning |
|-------|---------|
| `New` | Process just started; initialization not yet triggered |
| `InProgress` | Initialization underway |
| `Successful` | etcd is ready to serve traffic |
| `Failed` | Initialization failed; pod will be restarted by Kubernetes |

## EtcdMember resource

When `UseEtcdSteward` is enabled, etcd-druid creates one `EtcdMember` custom resource per pod. etcd-steward is the sole writer of `EtcdMember.status`. The status includes:

- `state` and `role` (Leader / Member)
- State machine transitions (from DEP-04)
- Timestamps for last snapshot, last defragmentation

Operators can read these resources to understand member-level health:

```bash
kubectl get etcdmembers -n <namespace>
```

## Metrics

etcd-steward exposes Prometheus metrics at `GET /metrics` on port 8080. Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_steward_component_health` | Gauge | 1 if component is running, 0 if stopped |
| `etcd_steward_state_transitions_total` | Counter | State transitions by from/to state |
| `etcd_steward_initialization_duration_seconds` | Histogram | Time taken for each initialization |
| `etcd_steward_snapshot_duration_seconds` | Histogram | Time taken for full/delta snapshots |
| `etcd_steward_defragmentation_duration_seconds` | Histogram | Time taken for defragmentation |

## Graceful shutdown

On `SIGTERM` or `SIGINT`, etcd-steward:

1. Sets `isShutdown = true` — `/healthz` begins returning 503
2. Cancels the root context — all components stop
3. Waits for in-flight operations to complete

Kubernetes will stop routing traffic to the pod as soon as `/healthz` returns 503.

## Troubleshooting

### Initialization stuck at `InProgress`

Check etcd-steward logs:

```bash
kubectl logs -n <namespace> <pod> -c backup-restore
```

Common causes:
- Cannot reach etcd peers (network policy or TLS mismatch)
- Snapshot restore failing (storage credentials, corrupted snapshot)
- K8s API server unreachable (in-cluster config issue)

### CORRUPT alarm logged

etcd-steward logs CORRUPT alarms but does not auto-disarm them. Manual intervention is required:

1. Identify the corrupted member from the logs
2. Delete the member's data directory
3. Restart the pod — etcd-steward will restore from snapshot

### NOSPACE alarm auto-remediation

etcd-steward automatically remediates NOSPACE alarms by:

1. Compacting to `currentRevision - compactRevisionLag` (default lag: 1000)
2. Defragmenting the member DB
3. Disarming the alarm

No manual intervention is needed unless the alarm recurs immediately (indicates DB size is consistently at the quota limit — increase `--auto-compaction-retention` or etcd quota).
