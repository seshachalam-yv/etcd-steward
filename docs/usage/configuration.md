<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Configuration Reference

etcd-steward is configured through CLI flags and an optional YAML config file. Flags always take precedence over file values.

## Precedence

```
CLI flag > config file > default value
```

When a flag is explicitly set on the command line, the config file value for that field is ignored. This is enforced by checking `pflag.Changed` before applying file values.

## CLI Flags

### General

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | string | `""` | Path to the YAML configuration file |
| `--server-port` | int | `8080` | HTTP server listen port |
| `--pod-name` | string | `""` | Name of the pod running etcd-steward (**required**) |
| `--pod-namespace` | string | `""` | Kubernetes namespace of the pod (**required**) |

### etcd Connection

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--etcd-endpoints` | string slice | `[]` | Comma-separated list of etcd client endpoints (**required**) |
| `--etcd-connection-timeout` | duration | `30s` | Timeout for connecting to etcd |

### TLS

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--ca-cert` | string | `""` | Path to the CA certificate for TLS |
| `--cert` | string | `""` | Path to the client certificate for TLS |
| `--key` | string | `""` | Path to the client private key for TLS |

### etcd Cluster

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--data-dir` | string | `/var/etcd/data/new.etcd` | etcd data directory |
| `--initial-cluster` | string | `""` | Initial etcd cluster configuration |
| `--initial-cluster-token` | string | `""` | Initial etcd cluster token |
| `--initial-cluster-state` | string | `new` | Initial etcd cluster state (`new` or `existing`) |
| `--initial-advertise-peer-urls` | string | `""` | Initial advertise peer URLs |
| `--listen-peer-urls` | string | `""` | URLs on which etcd listens for peer traffic |
| `--advertise-client-urls` | string | `""` | URLs advertised to clients |
| `--listen-client-urls` | string | `""` | URLs on which etcd listens for client traffic |
| `--auto-compaction-mode` | string | `periodic` | etcd auto-compaction mode (`periodic` or `revision`) |
| `--auto-compaction-retention` | string | `""` | etcd auto-compaction retention value |
| `--embedded-etcd-quota-bytes` | int64 | `8589934592` (8 GiB) | etcd backend quota in bytes |

### Component Enable/Disable

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--enable-snapshotter` | bool | `true` | Enable periodic snapshots |
| `--enable-gc` | bool | `true` | Enable garbage collection of old snapshots |
| `--enable-defrag` | bool | `true` | Enable periodic defragmentation |
| `--enable-alarm-handler` | bool | `true` | Enable the etcd alarm handler |
| `--enable-member-lease-renewal` | bool | `true` | Enable member lease renewal |
| `--enable-snapshot-lease-renewal` | bool | `true` | Enable snapshot lease renewal |

### Intervals and Timeouts

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--full-snapshot-interval` | duration | `24h` | Interval between full snapshots |
| `--delta-snapshot-interval` | duration | `5m` | Interval between delta snapshots |
| `--gc-period` | duration | `1h` | Garbage collection period |
| `--defrag-interval` | duration | `24h` | Interval between defragmentation runs |
| `--defrag-timeout` | duration | `10m` | Timeout for a single defragmentation operation |
| `--alarm-poll-interval` | duration | `30s` | Polling interval for alarm checks |
| `--k8s-heartbeat-duration` | duration | `2s` | Heartbeat duration for Kubernetes lease renewal |
| `--leader-election-reelection-period` | duration | `5s` | Leader election re-election period |

### Retention and Compaction

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--max-full-snapshots` | int | `7` | Maximum number of full snapshots to retain |
| `--compact-revision-lag` | int64 | `10000` | Revision lag for compaction |

### Lease Names

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--full-snapshot-lease-name` | string | `""` | Name of the full snapshot Kubernetes coordination lease |
| `--delta-snapshot-lease-name` | string | `""` | Name of the delta snapshot Kubernetes coordination lease |

### Miscellaneous

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--defrag-schedule` | string | `""` | Cron schedule for defragmentation (overrides interval) |
| `--restoration-temp-dir` | string | `/var/etcd/data/restoration.tmp` | Temporary directory used during restoration |

### Snapshot Store

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--store-provider` | string | `Local` | Snapshot store provider (`Local`, `S3`, `GCS`, `ABS`) |
| `--store-prefix` | string | `""` | Key prefix under which snapshots are stored |
| `--store-container` | string | `/var/etcd/data/snapshots` | Bucket/container name or root directory for Local |

## Cloud Provider Configuration

### Local

```bash
--store-provider=Local
--store-container=/var/etcd/data/snapshots
```

Stores snapshots on the local filesystem. The `--store-container` path is the root directory.

### AWS S3

```bash
--store-provider=S3
--store-container=my-bucket-name
--store-prefix=etcd-backups/cluster-1
```

S3 credentials are resolved from the standard AWS credential chain (environment variables, shared credentials file, IAM role).

### Google Cloud Storage (GCS)

```bash
--store-provider=GCS
--store-container=my-gcs-bucket
--store-prefix=etcd-backups/cluster-1
```

GCS credentials are resolved from the standard Google Application Default Credentials (ADC) chain.

### Azure Blob Storage (ABS)

```bash
--store-provider=ABS
--store-container=my-azure-container
--store-prefix=etcd-backups/cluster-1
```

Azure credentials are resolved from the standard Azure SDK credential chain.

## TLS Configuration

For production deployments, etcd typically uses mutual TLS. Configure the steward with paths to the certificate files:

```bash
--ca-cert=/var/etcd/ssl/ca/ca.crt
--cert=/var/etcd/ssl/client/tls.crt
--key=/var/etcd/ssl/client/tls.key
```

These paths are typically mounted from Kubernetes Secrets.

## Config File Format

The config file uses YAML format. Keys match the CLI flag names (kebab-case):

```yaml
# General
server-port: 8080
pod-name: etcd-main-0
pod-namespace: shoot--project--cluster

# etcd connection
etcd-endpoints:
  - https://etcd-main-0.etcd-main-peer.shoot--project--cluster.svc:2379
etcd-connection-timeout: 30s

# TLS
ca-cert: /var/etcd/ssl/ca/ca.crt
cert: /var/etcd/ssl/client/tls.crt
key: /var/etcd/ssl/client/tls.key

# etcd cluster
data-dir: /var/etcd/data/new.etcd
initial-cluster: etcd-main-0=https://etcd-main-0.etcd-main-peer:2380
initial-cluster-token: etcd-main
initial-cluster-state: new
embedded-etcd-quota-bytes: 8589934592

# Component toggles
enable-snapshotter: true
enable-gc: true
enable-defrag: true
enable-alarm-handler: true
enable-member-lease-renewal: true
enable-snapshot-lease-renewal: true

# Intervals
full-snapshot-interval: 24h
delta-snapshot-interval: 5m
gc-period: 1h
defrag-interval: 24h
defrag-timeout: 10m
alarm-poll-interval: 30s
k8s-heartbeat-duration: 2s
leader-election-reelection-period: 5s

# Retention
max-full-snapshots: 7
compact-revision-lag: 10000

# Lease names
full-snapshot-lease-name: etcd-main-full-snap
delta-snapshot-lease-name: etcd-main-delta-snap

# Restoration
restoration-temp-dir: /var/etcd/data/restoration.tmp

# Snapshot store
store-provider: S3
store-prefix: etcd-main
store-container: my-backup-bucket
```

Load it with:

```bash
etcd-steward --config-file=/etc/etcd-steward/config.yaml
```

Any flag explicitly set on the command line overrides the corresponding config file value.

## Validation

The following fields are validated at startup:

- `--pod-name` must not be empty.
- `--pod-namespace` must not be empty.
- `--etcd-endpoints` must contain at least one endpoint.
- `--server-port` must be between 1 and 65535.
- `--full-snapshot-interval` must not be less than `--delta-snapshot-interval`.
