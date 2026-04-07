<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Phase 3: Production-Ready — Observations

**Branch**: `ai/etcd-steward/claude/production-ready`
**Date**: 2026-04-07

---

## Summary

Phase 3 hardened the etcd-steward implementation for production use. All `make check` and `make test` gates pass cleanly.

---

## Tasks Completed

### 3.1 — Graceful shutdown / clean exit marker

- `server.Run()` closes `shutdownCh` and sets `isShutdown` when context is cancelled.
- `/healthz` returns `503 Service Unavailable` once shutdown begins, preventing new requests.
- Clean exit marker is not written to the data directory in this phase (etcd-wrapper handles lifecycle signalling via its own mechanism). The shutdown hook is in place for etcd-wrapper integration.

### 3.2 — TLS support for HTTP server

- `server.NewServerWithTLS()` added, accepting `certFile`/`keyFile` parameters.
- `server.NewServer()` delegates to `NewServerWithTLS` with empty strings (HTTP fallback).
- `cmd/etcd-steward/main.go` parses `--tls-cert-file` and `--tls-key-file` flags and passes them through.
- Mounts path: `/var/etcd/ssl/` (same as etcd-backup-restore).

### 3.3 — Config endpoint reads live file

- `configFn` in `main.go` reads `/var/etcd/config/etcd.conf.yaml` on every request (not cached).
- Returns YAML bytes directly; `Content-Type: application/x-yaml`.

### 3.4 — Integration tests for HTTP lifecycle

Two test suites:
- `TestHTTPLifecycle_PathC`: 9 sub-tests covering full lifecycle for fresh single-node (no DB, no snapshots).
  - `/healthz` before init → 200 "ok"
  - `/initialization/status` before start → "New"
  - `/config` → 200 with YAML
  - POST `/initialization/start?mode=sanity` → blocks, returns "Successful"
  - `/initialization/status` after start → "Successful"
  - Data dir exists after path C
  - Idempotency: second POST returns "Successful" immediately
  - `/metrics` reachable → prometheus output
  - Wrong method (DELETE) → 405
- `TestHTTPLifecycle_PathA`: path A with pre-created valid bbolt DB → "Successful"
- `TestServerReachableBeforeInit`: verifies server responds to `/healthz` before init goroutine starts (etcd-wrapper contract)

### 3.5 — EtcdMember CRD integration (etcd-druid fork)

- `UseEtcdSteward` feature gate added to etcd-druid fork branch `ai/etcd-steward-phase1/claude/etcd-member-crd`.
- EtcdMember CRD created per-member by etcd-druid when gate is enabled.
- etcd-steward writes `Status` sub-resource; ownership boundary respected.
- Verified in e2e: all 6 `TestBasic` variants pass with `UseEtcdSteward=true` enabled by default in the test configmap.

### 3.6 — Multi-node data-loss recovery: TCP pre-check

**Critical fix**: gRPC `MemberList` / `WasMemberInCluster` calls hang indefinitely when the etcd endpoint is unreachable. The gRPC client does not respect context cancellation during connection establishment phase.

Solution: `isEtcdReachable()` does a fast TCP dial (3s timeout) before any gRPC call. If TCP fails → skip gRPC entirely → return `false` (not data-loss). This unblocks 3-node fresh bootstrap where etcd is not yet running.

Implementation:
- `tcpDialFn` field on `Initializer` struct (injectable for tests).
- Default: `(&net.Dialer{}).DialContext`.
- `isEtcdReachable(ctx)` → TCP dial with 3s timeout.
- `etcdTCPAddr()` strips scheme and path from endpoint URL.
- TCP check at the very top of `needsDataLossRecovery()`, before both data-dir-empty and non-empty paths.

### 3.7 — Lint and quality fixes

All `errcheck` violations suppressed with `//nolint:errcheck` where appropriate:
- `conn.Close()` in `isEtcdReachable()`
- `ln.Close()` / `db.Close()` / `resp.Body.Close()` in test files

`make check` (golangci-lint v2.6.2) reports 0 issues.

### 3.8–3.11 — E2e verification

All 6 `TestBasic` variants verified against KIND cluster `etcd-druid-e2e`:
- `TestBasicEtcdWithNoBackup` (0-replica, 1-replica, 3-replica)
- `TestBasicEtcdWithLocalBackup` (0-replica, 1-replica, 3-replica)

All variants complete within 78s. `UseEtcdSteward` gate enabled. EtcdMember CRs created and reflect correct initialization status.

---

## Known Limitations

1. **Cloud backup backends** (S3, GCS, Azure Blob) are not tested in Phase 3. The snapstore abstraction is in place but only the local file-system provider is exercised in e2e.
2. **Delta snapshot replay** is stubbed. `pkg/snapshotter` triggers full snapshots only; delta replay from WAL is deferred to a follow-up task.
3. **Learner promotion timeout** is fixed at 10s in `promoteAfterSync`. This should be configurable via flag before general availability.
4. **No metrics for backup/restore operations** yet. The `InitializationDurationSeconds` histogram and `StateTransitionsTotal` counter are emitted, but snapshotter metrics are not yet wired.

---

## Test Results

```
make check:  0 issues (golangci-lint v2.6.2)
make test:   all packages ok
  pkg/alarm            ok
  pkg/compression      ok
  pkg/config           ok
  pkg/etcdclient       ok
  pkg/gc               ok
  pkg/initializer      ok (5 tests)
  pkg/integration      ok (3 test functions, 9+1+1 sub-tests)
  pkg/leaderwatch      ok
  pkg/lease            ok
  pkg/lock             ok
  pkg/member           ok
  pkg/restoration      ok
  pkg/server           ok
  pkg/snapshotlease    ok
  pkg/snapshotter      ok
  pkg/snapstore        ok
  pkg/statemachine     ok
  pkg/validator        ok
```

---

## HTTP Contract Compliance

All endpoints from the etcd-wrapper contract remain intact:

| Endpoint                          | Method     | Status |
|-----------------------------------|------------|--------|
| `/config`                         | GET        | ✅     |
| `/initialization/start`           | GET / POST | ✅     |
| `/initialization/status`          | GET        | ✅     |
| `/snapshot/full`                  | POST       | ✅     |
| `/snapshot/delta`                 | POST       | ✅     |
| `/snapshot/latest`                | GET        | ✅     |
| `/healthz`                        | GET        | ✅     |
| `/metrics`                        | GET        | ✅     |

---

## Files Changed in Phase 3

```
cmd/etcd-steward/main.go             — TLS flags, configFn, processEtcdConfig
cmd/etcd-steward/yaml.go             — YAML marshal/unmarshal helpers (new)
pkg/initializer/initializer.go       — TCP pre-check, tcpDialFn, isEtcdReachable, etcdTCPAddr
pkg/initializer/initializer_test.go  — DataLoss test uses real TCP listener
pkg/integration/integration_test.go  — Full HTTP lifecycle tests (new)
pkg/server/handlers.go               — TLS, GET /initialization/start, shutdown
Makefile                             — golangci-lint v2 target
hack/tools.mk                        — golangci-lint v2 version pin
```
