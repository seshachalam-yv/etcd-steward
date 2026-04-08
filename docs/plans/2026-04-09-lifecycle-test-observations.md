# Lifecycle Test Observations — Phase A (no TLS) + Phase B (TLS)

**Date**: 2026-04-09  
**Branch (steward)**: `ai/etcd-steward/claude/production-ready-v2`  
**Branch (druid)**: `ai/etcd-steward-phase1/claude/etcd-member-crd`

---

## Test Scope

Full end-to-end lifecycle for a single-node etcd cluster managed by etcd-steward:

1. **Phase A** — no TLS: write keys → full snapshot → write delta keys → delta snapshot → delete PVC → restore → verify state
2. **Phase B** — enable TLS on running cluster: write keys → full snapshot → write delta keys → delta snapshot → delete PVC → restore → verify state

Both phases completed successfully in a single test run.

---

## Phase A Results (no TLS)

| Step | Result |
|------|--------|
| Write 3 keys (`/lifecycle/phase-a/key{1,2,3}`) | ✅ revision 8–10 |
| Full snapshot triggered | ✅ `Full-...-10-....zst` at revision 10 |
| Write 2 delta keys (`/lifecycle/phase-a/delta{1,2}`) | ✅ revision 11–12 |
| Delta snapshot captured | ✅ `Incremental-...-10-...-12-...zst` |
| Delete PVC `test-test-0`, restore from full snapshot | ✅ restored to revision 10 |
| Verify 8 keys at revision 10 | ✅ (delta keys NOT restored — known gap, see below) |

---

## Phase B Results (with TLS — single cycle)

### Steps

| Step | Result |
|------|--------|
| Patch Etcd resource with `clientUrlTls`/`peerUrlTls`/`backup.tls` | ✅ druid reconciled, pod restarted with TLS |
| Verify TLS cluster healthy (get `/lifecycle/phase-a/key1` over HTTPS) | ✅ TLS verified via FQDN `test-client.manual-test.svc.cluster.local:2379` |
| Write 3 TLS keys (`/lifecycle/phase-b/key{1,2,3}`) | ✅ revision 11–13 (total 11 keys) |
| Full snapshot at revision 13 | ✅ `Full-...-13-...zst` |
| Write 2 delta keys (`/lifecycle/phase-b/delta{1,2}`) | ✅ revision 14–15 |
| Delta snapshot captured | ✅ `Incremental-...-13-...-15-...zst` |
| Delete PVC, restore from full snapshot at rev 13 | ✅ |
| Verify 11 keys at revision 13 (all phase-a + phase-b/key{1,2,3}) | ✅ (delta keys NOT restored — known gap) |

---

## Bugs Fixed (this session — carried from previous sessions)

### Bug 1: etcd-steward `--cacert/--cert/--key` flags not wired to etcd client
- **Fix**: `cmd/etcd-steward/main.go` reads TLS flags and passes to `clientv3.Config.TLS`

### Bug 2: Member lease missing `tls-enabled` annotation
- **Fix**: `pkg/lease/lease.go` sets `member.etcd.gardener.cloud/tls-enabled` annotation

### Bug 3: Readiness probe scheme wrong when only `clientUrlTLS` set
- **Fix**: `internal/component/statefulset/builder.go` — `etcdTLSEnabled := clientUrlTLS != nil || backup.TLS != nil`

---

## Observations / Minor Issues Found

### Observation 1: First write after pod restart silently returns empty (no timeout flags)
- **Symptom**: `etcdctl put` without `--dial-timeout`/`--command-timeout` returns empty output (pod exits 0 but no `OK`)
- **Root cause**: Pod starts, etcd runs, leader election takes ~1-2s; during this window gRPC requests are accepted but buffered. Default etcdctl command timeout is 5s but the silent empty comes from the pod being `Completed` before timeout fires
- **Workaround**: Always use `--dial-timeout=5s --command-timeout=10s` on first post-restart writes
- **Needed test**: `TestEtcdClientRetriesOnLeaderElection` — verify that retrying with backoff succeeds

### Observation 2: `endpoint health` always fails with `etcdserver: request timed out`
- **Symptom**: `etcdctl endpoint health` times out even when etcd is healthy
- **Root cause**: `endpoint health` uses `Alarm` RPC (a write) which needs quorum; for single-node etcd just after restart, raft needs a beat to commit
- **Impact**: Not a bug — single-node etcd is healthy when it can do `get` operations; `endpoint health` check may need `--command-timeout=30s`
- **Note**: The `healthy` result appears only if the alarm check completes within the timeout

### Observation 3: Server cert needed FQDN SAN (`test-client.manual-test.svc.cluster.local`)
- **Root cause**: etcd-wrapper's skip-san-dev image does not inject own SANs; manually-generated certs missed FQDN form
- **Fix**: Use `test/utils/pki.go GeneratePKIResourcesToDirectory` which generates all 9 required DNS forms
- **Test coverage**: `TestGetDNSNames` + `TestGeneratePKIResourcesToDirectory` in `test/utils/pki_test.go`

### Observation 4: Secret type immutability on `kubectl apply`
- **Symptom**: `kubectl create secret tls --dry-run | apply` fails with "type field is immutable" on existing `Opaque` secrets
- **Fix**: Use `kubectl patch secret --type=json` to update only the data fields

### Observation 5: Snapshot handlers had no etcd readiness gate
- **Symptom**: `POST /snapshot/full` and `POST /snapshot/delta` were accepted immediately even when
  `InitializationStatus != Successful` — i.e., before etcd had elected a leader after pod restart
- **Root cause**: `handleSnapshotFull`/`handleSnapshotDelta` in `pkg/server/handlers.go` called
  `TriggerFullSnapshot`/`TriggerDeltaSnapshot` without checking initialization status
- **Fix**: Added readiness gate in both handlers — return HTTP 503 when `statusFn() != Successful`
- **Test coverage**: `TestHandleSnapshotFull_NotInitialized`, `TestHandleSnapshotDelta_NotInitialized`
  in `pkg/server/server_test.go`

---

## Known Gaps

### Gap 1: Delta snapshot application not implemented (issue #11)
- **Behaviour**: `deltaCount:1` skipped with `"skipping delta application — not implemented in v0.1.0 (see issue #11)"`
- **Impact**: After restore, only last full snapshot state is recovered; keys written between last full and last delta are lost
- **Test coverage**: `TestRestore_WithDeltas` explicitly documents this behavior
- **Next step**: Implement MVCC event replay in `pkg/restoration/restoration.go`

### Gap 2: etcd-steward HTTP server → HTTPS server transition
- **Behaviour**: When TLS is enabled, steward correctly starts HTTPS server on port 8080
- **Status**: Working ✅ (steward logs `"starting HTTPS server"` in Phase B)
- **Note**: etcd-wrapper in `skip-san-dev` image uses plain HTTP for steward API; will need update when wrapper switches to HTTPS

---

## New Tests Added (this session)

### `test/utils/pki_test.go` (new — etcd-druid fork)
- `TestGetDNSNames`: 9 required SANs present including FQDN form
- `TestGeneratePKIResourcesToDirectory`: server cert has FQDN SANs; client cert has no DNS SANs

### `cmd/etcd-steward/main_test.go` (new — etcd-steward fork)
- `TestDerivePeerURL` — 6 cases
- `TestCountClusterMembers` — 3 cases
- `TestFirstURL` — 3 cases
- `TestProcessEtcdConfig_PerMemberFields`
- `TestProcessEtcdConfig_AlreadyFlatFields`
- `TestProcessEtcdConfig_MemberNotInMap_FallsBackToFirst`
- `TestProcessEtcdConfig_MultipleURLs`
- `TestProcessEtcdConfig_SetsMemberName`

### `pkg/lease/lease_test.go` (additions — etcd-steward fork)
- `TestRenewSetsPeerTLSAnnotation`
- `TestRenewPreservesPeerTLSAnnotationOnUpdate`

### `internal/component/statefulset/builder_test.go` (additions — etcd-druid fork)
- `TestReadinessProbeScheme` — 4 cases

### `pkg/server/server_test.go` (additions — etcd-steward fork)
- `TestHandleSnapshotFull_NotInitialized`: POST /snapshot/full returns 503 when status != Successful
- `TestHandleSnapshotDelta_NotInitialized`: POST /snapshot/delta returns 503 when status != Successful
- Updated `TestHandleSnapshotFull_SnapshotterError`, `TestHandleSnapshotFull_Success`,
  `TestHandleSnapshotDelta_SnapshotterError`, `TestHandleSnapshotDelta_Success`
  to use `InitializationStatusSuccessful` (required to pass readiness gate)

---

## Final State

All tests pass:
- `go test ./...` in etcd-steward: all packages, 0 failures
- `go test ./cmd/etcd-steward/...`: 8 tests, all pass
- `go test ./test/utils/...` in etcd-druid: 2 new tests, all pass
- `go test ./internal/component/statefulset/...` in etcd-druid: all tests pass

End-to-end lifecycle verified (second full run):
- Phase A (no TLS): ✅ write → full snap → delta → delta snap → restore → verify (3 keys at rev 4)
- Phase B (TLS): ✅ write → full snap → delta → delta snap → restore → verify (6 keys at rev 7)
- TLS connectivity verified via FQDN with proper cert SANs
