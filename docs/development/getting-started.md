<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Developer Guide

## Repository layout

```
etcd-steward/
├── cmd/
│   └── etcd-steward/
│       ├── main.go              # Entry point; flag parsing; component wiring
│       ├── compact/             # `compact` subcommand
│       └── copybackups/         # `copy-backups` subcommand
├── pkg/
│   ├── alarm/          # Alarm monitoring and NOSPACE remediation
│   ├── compression/    # Snapshot compression (gzip, zstd, noop)
│   ├── config/         # Config struct, defaults, validation
│   ├── etcdclient/     # etcd cluster client wrapper
│   ├── gc/             # Snapshot garbage collection
│   ├── initializer/    # etcd member lifecycle initialization (DEP-04)
│   ├── integration/    # Integration test helpers (not a runtime package)
│   ├── leaderwatch/    # Leadership observation via etcd Status polling
│   ├── lease/          # Kubernetes member lease heartbeat renewal
│   ├── lock/           # etcd-backed distributed lock
│   ├── member/         # EtcdMember K8s status updater
│   ├── metrics/        # Prometheus metric registrations
│   ├── restoration/    # etcd restoration from snapshots
│   ├── server/         # HTTP server and handlers
│   ├── snapshotlease/  # Snapshot lease renewal (druid compatibility)
│   ├── snapshotter/    # Full + delta snapshot pipeline
│   ├── snapstore/      # Snapstore interface + Local implementation
│   ├── statemachine/   # DEP-04 state/transition types; K8s recorder
│   └── validator/      # etcd DB validation (corruption, exit marker)
└── docs/               # This documentation
```

## Prerequisites

- Go 1.23+
- `golangci-lint` v2 (installed automatically by `make check`)
- A running etcd for integration tests (`make test` uses a pre-built embedded etcd)

## Make targets

| Target | Description |
|--------|-------------|
| `make build` | Build `etcd-steward` binary to `./bin/etcd-steward` |
| `make test` | Run all unit + integration tests |
| `make check` | Run `golangci-lint` (auto-installs if missing) |
| `make image` | Build Docker image using `ko` or `docker build` |

Run both gates before opening a PR:

```bash
make check && make test
```

## Testing

### Unit tests

Every package in `pkg/` has a `*_test.go` file using the standard `testing` package (no Ginkgo). Tests use table-driven patterns and fake implementations — no real etcd required.

```bash
go test ./pkg/... -count=1
```

### Integration tests

`pkg/integration` contains end-to-end HTTP lifecycle tests that wire the full server + initializer together using an in-process embedded bbolt database. No external services needed.

```bash
go test ./pkg/integration/... -v -count=1
```

### Coverage

```bash
go test ./pkg/... -cover -count=1
```

Target: ≥ 80% per package.

## Adding a new component

1. Create `pkg/<component>/` with a `Run(ctx context.Context)` method.
2. Register metrics in `pkg/metrics/metrics.go`.
3. Add enable flag to `pkg/config/config.go` (`Enable<Component> bool`).
4. Wire in `cmd/etcd-steward/main.go` — guard with `if cfg.Enable<Component>`.
5. Write tests achieving ≥ 80% coverage.
6. Run `make check && make test`.

## Adding a snapstore provider

The `pkg/snapstore.Snapstore` interface requires four methods:

```go
type Snapstore interface {
    Save(snap Snapshot, r io.ReadCloser) error
    Fetch(snap Snapshot) (io.ReadCloser, error)
    List() ([]Snapshot, error)
    Delete(snap Snapshot) error
}
```

To add a provider (e.g. S3):

1. Create `pkg/snapstore/s3.go` implementing `Snapstore`.
2. Add a `case "S3"` branch in `NewSnapstore()` in `pkg/snapstore/snapstore.go`.
3. Add any required configuration fields to `SnapstoreConfig`.
4. Write tests in `pkg/snapstore/s3_test.go`.

## Code conventions

- **Errors**: wrap with `fmt.Errorf("context: %w", err)` — never swallow.
- **Logging**: `zap` structured JSON; pass `logger.Named("<component>")` to each component.
- **No Ginkgo**: use standard `testing.T` and table-driven tests.
- **No `time.Sleep` in tests**: use channels, `sync.WaitGroup`, or condition loops.
- **Commit style**: imperative sentence case, no trailing period. Example: `Fix NOSPACE remediation race condition`
- **Imports**: stdlib → external → internal, separated by blank lines.

## Dependency injection

All components receive their dependencies via constructor parameters — no global state. This makes unit testing straightforward: pass in fake implementations of the interface your component uses.

Example: to test `alarm.Handler`, provide a `fakeMaintenance` that implements `MaintenanceAPI` and a `fakeKV` that implements `KVCompactAPI`. See `pkg/alarm/alarm_test.go`.
