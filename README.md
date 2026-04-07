<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# etcd-steward

[![REUSE status](https://api.reuse.software/badge/github.com/gardener/etcd-steward)](https://api.reuse.software/info/github.com/gardener/etcd-steward)
[![CI Build status](https://concourse.ci.gardener.cloud/api/v1/teams/gardener/pipelines/etcd-steward-master/jobs/master-head-update-job/badge)](https://concourse.ci.gardener.cloud/api/v1/teams/gardener/pipelines/etcd-steward-master/jobs/master-head-update-job)
[![Go Report Card](https://goreportcard.com/badge/github.com/gardener/etcd-steward)](https://goreportcard.com/report/github.com/gardener/etcd-steward)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

`etcd-steward` is a sidecar agent that manages the lifecycle of an etcd member. It is the successor to [`etcd-backup-restore`](https://github.com/gardener/etcd-backup-restore) and is deployed by [etcd-druid](https://github.com/gardener/etcd-druid).

## What it does

- **Initializes** the etcd member following the [DEP-04 lifecycle](docs/concepts/etcd-member-lifecycle.md) — handles fresh bootstrap, learner join, restore from snapshot, and re-join after corruption
- **Snapshots** the etcd DB (full + delta) to configurable object storage on a schedule or on-demand
- **Restores** the etcd DB from snapshots on data loss
- **Garbage collects** old snapshots according to retention policies
- **Monitors alarms** and automatically remediates NOSPACE by compacting and defragmenting
- **Updates EtcdMember** status with real-time member state for etcd-druid to consume
- **Renews K8s Leases** as heartbeats for member liveness detection
- **Exposes HTTP endpoints** compatible with etcd-wrapper: `/initialization/start`, `/initialization/status`, `/config`, `/snapshot/full`, `/snapshot/delta`, `/snapshot/latest`, `/healthz`, `/metrics`

## Documentation

| Audience | Document |
|----------|----------|
| Users / integrators | [Getting Started](docs/usage/getting-started.md) |
| Users / integrators | [Configuration Reference](docs/usage/configuration.md) |
| Operators | [Operator Guide](docs/operator/getting-started.md) |
| Developers | [Developer Guide](docs/development/getting-started.md) |
| Concepts | [Architecture](docs/concepts/architecture.md) |
| Concepts | [etcd Member Lifecycle](docs/concepts/etcd-member-lifecycle.md) |

## Quick start

etcd-steward is deployed automatically by etcd-druid when the `UseEtcdSteward` feature gate is enabled. See the [Getting Started guide](docs/usage/getting-started.md).

## Building

```bash
make build   # produces ./bin/etcd-steward
make test    # runs all unit + integration tests
make check   # runs golangci-lint
```

## License

[Apache-2.0](LICENSE)
