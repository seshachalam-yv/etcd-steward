<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Getting Started

This guide explains how to set up a local development environment for etcd-steward, build the binary, and run the test suites.

## Prerequisites

| Tool | Minimum Version | Purpose |
|------|----------------|---------|
| Go | 1.24+ | Build and test |
| Docker | 20.10+ | Container image builds and KIND clusters |
| KIND | 0.20+ | Local Kubernetes clusters for e2e tests |
| kubectl | 1.28+ | Interacting with test clusters |

Verify your setup:

```bash
go version        # go1.24 or later
docker version    # daemon running
kind version      # v0.20+
kubectl version --client
```

## Clone and Build

```bash
git clone https://github.com/gardener/etcd-steward.git
cd etcd-steward

make build
```

The binary is placed at `bin/etcd-steward`. Build uses `CGO_ENABLED=0` for a statically linked binary suitable for distroless container images.

To build the container image:

```bash
docker build -t etcd-steward:local .
```

The multi-stage `Dockerfile` produces a minimal image based on `gcr.io/distroless/static-debian12:nonroot`.

## Running Tests

### Unit Tests

```bash
make test-unit
```

This runs all unit tests with the race detector enabled:

```bash
go test -count=1 -race ./internal/... ./cmd/...
```

### End-to-End Tests

E2e tests require a running KIND cluster with etcd-steward deployed:

```bash
make test-e2e
```

Under the hood this executes:

```bash
go test -count=1 -race -tags=e2e ./test/e2e/...
```

E2e test files carry the `//go:build e2e` build tag so they are excluded from unit test runs. See [Testing](testing.md) for details on writing e2e tests.

### Lint

```bash
make check
```

Runs `golangci-lint` with the project configuration in `.golangci.yaml`.

### Full Verification

```bash
make verify
```

Runs both the linter and unit tests in sequence.

## Project Structure

```
etcd-steward/
├── cmd/
│   └── etcdsteward/
│       ├── main.go              # CLI entry point (daemon, version)
│       ├── compact/             # `compact` sub-command
│       └── copybackups/         # `copy-backups` sub-command
├── internal/
│   ├── alarm/                   # Etcd alarm polling and NOSPACE recovery
│   ├── bootstrapper/            # Member init: validate, restore, start etcd
│   ├── compactor/               # Offline snapshot compaction
│   ├── compression/             # gzip / zstd compression helpers
│   ├── config/                  # Config struct, CLI flags, file loading
│   ├── defrag/                  # Leader-coordinated defragmentation
│   ├── errors/                  # Structured error type with codes
│   ├── etcdclient/              # Typed wrappers around etcd clientv3
│   ├── gc/                      # Snapshot garbage collection (count, time, calendar)
│   ├── lease/                   # Kubernetes coordination lease renewal
│   ├── leaderwatch/             # Leadership detection via etcd key watch
│   ├── member/                  # EtcdMember status updater and info providers
│   ├── metrics/                 # Per-component Prometheus metric registration
│   ├── restorer/                # Full + delta snapshot restore
│   ├── server/                  # HTTP server (/healthz, /metrics, /init, /config)
│   ├── snapshotter/             # Full and delta snapshot capture (NDJSON events)
│   ├── snapstore/               # Snapshot storage backends (Local, S3, GCS, ABS)
│   ├── statemachine/            # Member lifecycle state machine and K8s recorder
│   └── validator/               # Data directory integrity checks (bbolt)
├── test/
│   └── e2e/                     # End-to-end tests (build tag: e2e)
├── hack/                        # Build and development scripts
├── Makefile                     # Build, test, lint targets
├── Dockerfile                   # Multi-stage distroless image build
└── go.mod                       # Module: github.com/gardener/etcd-steward
```

## Configuration

etcd-steward accepts configuration through two mechanisms:

1. **CLI flags** -- every field in the `Config` struct is exposed as a kebab-case flag (e.g. `--server-port`, `--enable-snapshotter`).
2. **YAML config file** -- pass `--config-file=path/to/config.yaml` to load values from a file.

**Flags always override file values.** When a flag is explicitly set on the command line, the corresponding file value is ignored. This is enforced through the `Changed` check on each `pflag` entry.

See [Configuration Reference](../usage/configuration.md) for the full flag list and an example config file.
