<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Configuration Reference

etcd-steward is configured via CLI flags injected by etcd-druid. All flags can also be set via environment variables for the subset listed below.

## Identity

These are injected by etcd-druid via the Kubernetes Downward API.

| Environment variable | Description |
|---------------------|-------------|
| `POD_NAME` | Pod name (e.g. `test-0`). Used to derive member name and etcd name. |
| `POD_NAMESPACE` | Namespace where the etcd cluster runs. |
| `ETCD_ENDPOINT` | etcd client endpoint URL (e.g. `http://localhost:2379`). |
| `ETCD_PEER_URL` | This member's peer URL. Derived from `initial-cluster` if not set. |
| `INITIAL_CLUSTER` | The `initial-cluster` string in `name=url,...` format. |
| `CREATE_AS_LEARNER` | Set to `"true"` to join as a learner (new member add flow). |

## Server

| Flag | Default | Description |
|------|---------|-------------|
| `--server-port` | `8080` | HTTP server port |
| `--server-cert` | | TLS certificate file for HTTPS |
| `--server-key` | | TLS key file for HTTPS |

## etcd connectivity

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoints` | | etcd client endpoint URL |
| `--initial-cluster` | | etcd `initial-cluster` string |
| `--etcd-peer-url` | | This member's peer URL |
| `--data-dir` | `/var/etcd/data` | etcd data directory |
| `--cacert` | | CA certificate for etcd client TLS |
| `--cert` | | Client certificate for etcd client TLS |
| `--key` | | Client key for etcd client TLS |
| `--etcd-connection-timeout` | `5m` | Timeout for etcd connection establishment |

## Member lease

| Flag | Default | Description |
|------|---------|-------------|
| `--enable-member-lease-renewal` | `true` | Enable Kubernetes member lease heartbeat renewal |
| `--k8s-heartbeat-duration` | `10s` | Heartbeat interval (lease TTL = 3× this value) |

## Snapshots

These flags are only meaningful when `--storage-provider` is set.

| Flag | Default | Description |
|------|---------|-------------|
| `--storage-provider` | | Snapstore provider: `Local`, `S3`, `GCS`, `ABS` |
| `--store-container` | | Bucket or directory for snapshots |
| `--store-prefix` | | Path prefix within the container |
| `--store-tempdir` | | Local temp directory for snapshot staging |
| `--schedule` | `0 */24 * * *` | Cron schedule for full snapshots |
| `--delta-snapshot-period` | `1m` | Interval between delta snapshots |
| `--delta-snapshot-memory-limit` | `100MiB` | Max in-memory delta accumulation before forced flush |
| `--compress-snapshots` | `false` | Enable snapshot compression |
| `--compression-policy` | `gzip` | Compression algorithm: `none`, `gzip`, `zstd` |
| `--etcd-snapshot-timeout` | `10m` | Timeout for snapshot operations |

## Garbage collection

| Flag | Default | Description |
|------|---------|-------------|
| `--garbage-collection-policy` | `Exponential` | GC policy: `LimitBased` or `Exponential` |
| `--garbage-collection-period` | `12h` | How often GC runs |
| `--max-backups` | `7` | Maximum snapshot sets to retain (LimitBased policy) |

## Defragmentation

| Flag | Default | Description |
|------|---------|-------------|
| `--defrag-schedule` | | Cron schedule for scheduled defragmentation |
| `--etcd-defrag-timeout` | `15m` | Timeout for defragmentation operations |

## Auto-compaction

| Flag | Default | Description |
|------|---------|-------------|
| `--auto-compaction-mode` | `periodic` | etcd auto-compaction mode |
| `--auto-compaction-retention` | `30m` | etcd auto-compaction retention |

## Alarm handling

Alarm monitoring is always enabled. NOSPACE alarms trigger compact + defrag + disarm automatically. CORRUPT alarms are logged but not auto-disarmed.

| Config field | Default | Description |
|-------------|---------|-------------|
| `alarmCheckInterval` | `30s` | How often to poll for etcd alarms |
| `compactRevisionLag` | `1000` | Revisions to retain behind current when compacting on NOSPACE |
