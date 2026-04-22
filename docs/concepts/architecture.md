<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Architecture

etcd-steward is a sidecar process that manages operational tasks for an etcd cluster member. It runs alongside the etcd process (managed by etcd-wrapper) and handles snapshotting, restoration, defragmentation, garbage collection, alarm handling, lease renewal, and member lifecycle management.

## Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        etcd-steward daemon                         │
│                                                                     │
│  ┌──────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐  │
│  │bootstrapper│  │  snapshotter │  │  restorer  │  │  compactor   │  │
│  │           │  │ (full+delta) │  │(full+delta)│  │  (offline)   │  │
│  └─────┬─────┘  └──────┬───────┘  └─────┬─────┘  └──────┬───────┘  │
│        │               │                │                │          │
│  ┌─────┴─────┐  ┌──────┴───────┐  ┌─────┴──────┐  ┌─────┴───────┐ │
│  │ validator  │  │  leaderwatch │  │ compression│  │  snapstore  │ │
│  │(data dir)  │  │ (etcd key)  │  │(gzip/zstd) │  │(Local/S3/   │ │
│  └───────────┘  └──────────────┘  └────────────┘  │ GCS/ABS)    │ │
│                                                     └─────────────┘ │
│  ┌───────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐ │
│  │   defrag   │  │    alarm     │  │    gc      │  │    lease     │ │
│  │(leader-   │  │  (NOSPACE/   │  │(snapshot-  │  │ (K8s coord   │ │
│  │coordinated)│  │  CORRUPT)   │  │ set based) │  │  renewal)    │ │
│  └───────────┘  └──────────────┘  └───────────┘  └──────────────┘ │
│                                                                     │
│  ┌───────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐ │
│  │  member    │  │ statemachine │  │  server    │  │  etcdclient  │ │
│  │ (updater + │  │ (lifecycle)  │  │ (HTTP API) │  │ (KV/Maint/  │ │
│  │ providers) │  │              │  │            │  │  Cluster)    │ │
│  └───────────┘  └──────────────┘  └───────────┘  └──────────────┘ │
│                                                                     │
│  ┌───────────┐  ┌──────────────┐                                   │
│  │  errors    │  │   metrics    │                                   │
│  │(codes +   │  │(per-component│                                   │
│  │ wrapping)  │  │ prometheus)  │                                   │
│  └───────────┘  └──────────────┘                                   │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                     config (viper + pflags)                   │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                │                    │
                ▼                    ▼
        ┌──────────────┐    ┌──────────────┐
        │  etcd (via    │    │  etcd-wrapper │
        │  clientv3)    │    │  (sidecar)    │
        └──────────────┘    └──────────────┘
```

## CLI Modes

etcd-steward provides three CLI modes via cobra sub-commands:

### Default: Daemon Mode

```bash
etcd-steward [flags]
```

Runs the full daemon that wires all enabled components. This is the primary mode used in production. The daemon:

1. Loads configuration from flags and an optional YAML config file.
2. Creates the snapstore backend.
3. Initializes the member via the bootstrapper (validate, restore, start etcd).
4. Starts all enabled components as concurrent goroutines.
5. Runs the HTTP server for health checks, metrics, and initialization endpoints.
6. Shuts down gracefully on context cancellation (SIGTERM).

### `compact`

```bash
etcd-steward compact [flags]
```

Performs offline compaction of etcd snapshots. Finds the latest full snapshot, identifies all subsequent delta snapshots, restores the full snapshot to a temporary directory, and uploads a new compacted full snapshot with the final revision. This is typically run as a Kubernetes Job.

### `copy-backups`

```bash
etcd-steward copy-backups [flags]
```

Copies snapshots between storage backends. Supports cross-provider copy (e.g. Local to S3, GCS to ABS). Snapshots that already exist in the destination (by name) are skipped.

## Configuration

Configuration uses a layered approach:

1. **Defaults**: `config.DefaultConfig()` provides sensible production defaults.
2. **Config file**: A YAML file loaded via `--config-file`. Parsed by viper.
3. **CLI flags**: Every `Config` field has a corresponding kebab-case flag. Flags always take precedence over file values.

The precedence rule is: **CLI flag > config file > default**. This is enforced by checking `pflag.Changed` before applying file values.

## Logging

All components use `go.uber.org/zap` for structured JSON logging:

```go
logger, _ := zap.NewProduction()
logger.Info("starting reconciliation",
    zap.String("pod", podName),
    zap.Int64("revision", rev),
)
logger.Error("operation failed", zap.Error(err))
```

Key conventions:

- `Info` for normal operations and state transitions.
- `Error` for failures requiring attention.
- `Warn` for recoverable issues (e.g. NOSPACE alarm detected).
- `Debug` for detailed tracing (e.g. snapshot info collection).

## Error Handling

etcd-steward defines a custom `Error` type in `internal/errors` with typed error codes:

```go
type Error struct {
    code      ErrCode    // e.g. ErrCodeEtcd, ErrCodeSnapshot, ErrCodeConfig
    subCode   int        // optional sub-classification
    cause     error      // wrapped cause
    message   string     // human-readable message
    operation string     // optional operation name
}
```

Error codes categorize failures for programmatic handling:

| Code | Meaning |
|------|---------|
| `ErrCodeConfig` | Configuration error |
| `ErrCodeIO` | I/O error |
| `ErrCodeEtcd` | etcd client error |
| `ErrCodeSnapshot` | Snapshot operation error |
| `ErrCodeRestore` | Restore operation error |
| `ErrCodeStorage` | Storage backend error |
| `ErrCodeTimeout` | Timeout |
| `ErrCodeValidation` | Validation failure |
| `ErrCodeInvalidTransition` | Invalid state machine transition |

Errors are created via `errors.New()`, `errors.Wrap()`, or `errors.NewWithOperation()` and support the standard `errors.Is()` / `errors.As()` / `Unwrap()` chain.

## Metrics

Each component registers its own Prometheus metrics via `metrics.RegisterComponent()`:

```go
m := metrics.RegisterComponent(metrics.Snapshotter)
```

Every component gets three standard metrics:

- `etcd_steward_<component>_operation_duration_seconds` (histogram)
- `etcd_steward_<component>_operation_errors_total` (counter)
- `etcd_steward_<component>_operation_total` (counter)

Components may also register component-specific metrics (e.g. `snapshot_revision`, `defrag_db_size_before_bytes`, `gc_deleted_snapshots_total`, `alarm_active`).

Metrics are served at `GET /metrics` via the HTTP server using the standard `promhttp.Handler()`.

## HTTP Server

The server (`internal/server`) exposes:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Liveness probe (always returns 200) |
| `/metrics` | GET | Prometheus metrics |
| `/initialization/status` | GET | Current init status (New/Progress/Successful/Failed) |
| `/initialization/start` | POST | Trigger initialization with a mode parameter |
| `/config` | GET | Current config as raw bytes |

The server runs on the port specified by `--server-port` (default: 8080) and performs graceful shutdown with a 5-second timeout on context cancellation.
