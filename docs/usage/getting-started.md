<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Getting Started

`etcd-steward` runs as a sidecar container alongside each etcd member. It replaces `etcd-backup-restore` and is managed by [etcd-druid](https://github.com/gardener/etcd-druid).

## Prerequisites

- Kubernetes cluster (etcd-steward runs in-cluster)
- etcd-druid v0.34+ with the `UseEtcdSteward` feature gate enabled
- etcd-wrapper container in the same pod

## Deployment

etcd-steward is injected automatically by etcd-druid when the `UseEtcdSteward` feature gate is enabled on the operator. You do not deploy it manually.

To enable the feature gate, set the following in the etcd-druid operator configuration:

```yaml
featureGates:
  UseEtcdSteward: true
```

Once enabled, etcd-druid creates an `etcd-steward` container in every etcd StatefulSet pod alongside the `etcd` and `backup-restore` containers (the `backup-restore` container is replaced by `etcd-steward`).

## What etcd-steward does

On startup, etcd-steward:

1. Runs an HTTP server on port `8080` (all endpoints available immediately)
2. Initializes the etcd member following the [DEP-04 lifecycle](../concepts/etcd-member-lifecycle.md)
3. Starts background components based on configuration:
   - Member lease heartbeat renewal
   - Leadership observation
   - Snapshots (full + delta), if a backup store is configured
   - Snapshot garbage collection, if a backup store is configured
   - Alarm monitoring and NOSPACE remediation
   - EtcdMember status updates

## HTTP endpoints

All endpoints are available from the moment the process starts, before initialization completes.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/initialization/start` | GET, POST | Start initialization; GET returns immediately, POST polls until complete |
| `/initialization/status` | GET | Current initialization status (`New`, `InProgress`, `Successful`, `Failed`) |
| `/config` | GET | Returns the etcd configuration YAML for etcd-wrapper |
| `/snapshot/full` | POST | Trigger an on-demand full snapshot |
| `/snapshot/delta` | POST | Trigger an on-demand delta snapshot |
| `/snapshot/latest` | GET | Returns metadata of the latest available snapshot |
| `/healthz` | GET | Returns `ok` (200) while running, `503` during shutdown |
| `/metrics` | GET | Prometheus metrics |

## Subcommands

etcd-steward ships with two utility subcommands:

### compact

Compacts a set of snapshots (full + deltas) into a single full snapshot to speed up future restorations.

```bash
etcd-steward compact \
  --etcd-endpoint=http://localhost:2379 \
  --revision=1234567
```

### copy-backups

Copies all snapshots from one storage location to another.

```bash
etcd-steward copy-backups \
  --source-provider=Local --source-container=/mnt/backup/source \
  --dest-provider=Local   --dest-container=/mnt/backup/dest
```
