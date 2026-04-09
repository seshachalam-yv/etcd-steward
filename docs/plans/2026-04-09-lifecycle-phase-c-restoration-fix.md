# Lifecycle Phase C: Restoration Fix — WAL+Snap & Delta Application

**Date**: 2026-04-09  
**Branch**: `ai/etcd-steward/claude/production-ready-v2`

---

## Summary

This session completed the full lifecycle test (Phase A no-TLS + Phase B TLS from previous session)
by diagnosing and fixing two critical restoration bugs:

1. **Write operations timed out after restore** — root cause: missing WAL/snap files
2. **Delta keys not restored** — root cause: delta application not implemented (now fixed)

Both issues are now fixed and verified end-to-end with a TLS cluster.

---

## Phase C Test Scenario

| Step | Before Fix | After Fix |
|------|-----------|-----------|
| Write 13 keys (phase-a, phase-b, phase-c prefix) | ✅ | ✅ |
| Full snapshot at rev 11 | ✅ | ✅ |
| Write 3 delta keys (phase-c/delta{1,2,3}) | ✅ | ✅ |
| Delta snapshot rev 11→14 | ✅ | ✅ |
| Delete PVC (simulate data loss) | ✅ | ✅ |
| Restore from full+delta | ✅ restored 10 keys, delta LOST | ✅ all 13 keys restored |
| Write-after-restore (`PUT`) | ❌ timeout | ✅ `OK` |

---

## Bug 1: Write Timeout After Restoration

### Symptom
After `kubectl delete pvc test-test-0` → restore, all 10 keys (from full snapshot) were readable
but every `etcdctl put` timed out with raft `"spent 7s"` warnings.

### Root Cause
Original `restoration.go` (step 5 of tryRestore) only copied `member/snap/db` from the
decompressed snapshot into `dataDir/member/snap/db`. etcd's raft subsystem requires:
- `member/wal/` — write-ahead log for raft consistency
- `member/snap/` — snapshot metadata including term/index

Without WAL, etcd can read MVCC data (bbolt reads directly) but cannot commit writes
because raft has no initialized starting state.

### Fix
Rewrote `pkg/restoration/restoration.go` to use `go.etcd.io/etcd/etcdutl/v3/snapshot.v3Manager.Restore()`:

```
1. Fetch + decompress snapshot → tmpSnapshotPath = tempDir/snapshot.db
2. os.RemoveAll(dataDir)  (idempotent clear)
3. etcdutl snapshot.Restore(RestoreConfig{
       SnapshotPath:   tmpSnapshotPath,
       OutputDataDir: tempDir/restore-out,
       Name:          memberName,
       PeerURLs:      []string{peerURL},
       InitialCluster: initialCluster,
       InitialClusterToken: "etcd-cluster-<first-member>",
   })
   → creates restore-out/member/{wal/, snap/db}
4. Apply delta snapshots to restore-out/member/snap/db
5. os.Rename(restore-out, dataDir)
```

Added `go.etcd.io/etcd/etcdutl/v3 v3.5.27` to go.mod + vendor.

---

## Bug 2: Delta Snapshot Application Was Incomplete (Gap 1 from prev session)

### Symptom
After restore from full snapshot (rev 11), keys written between full snapshot and PVC deletion
(phase-c/delta{1,2,3} at rev 12-14) were not present.

### Root Cause
Previous session noted this as "Gap 1 — not implemented in v0.1.0". The delta application
code (`applyDeltas`, `writeDeltaEventsToDB`) was present but the restoration flow only
applied deltas to the wrong target — the delta was read but the bbolt DB path was
`restoreOut/member/snap/db` which hadn't been created yet (because we weren't using etcdutl).

### Fix
With the etcdutl-based approach:
- `etcdutl.Restore()` creates `restoreOut/member/snap/db` with proper raft metadata
- Delta snapshots are applied to this DB path BEFORE moving it to `dataDir`
- The `applyDeltas` + `writeDeltaEventsToDB` code now correctly adds MVCC entries

Result: After restore, `GET /prefix` returns all 13 keys (10 from full snapshot + 3 delta keys).

---

## Test Fixes

### `pkg/restoration/restoration_test.go`
- Added `TestMain` to set `skipHashCheck = true` (etcdutl requires valid hash; tests use synthetic data)
- All 17 tests pass after fix

### `pkg/initializer/initializer_test.go`
- Added `TestMain` calling `restoration.SetSkipHashCheckForTests(true)`
- Added `makeMinimalEtcdSnapshotBytes` helper that creates a real bbolt DB with "key"+"meta" buckets
- Fixed `TestInitializer_SingleNode_EmptyDataDir_WithSnapshots` to use real bbolt DB instead of `[]byte("fake-etcd-db-content")`
- All initializer tests pass

---

## Bug 3: `--compression-policy` Flag Ignored

### Symptom
`--compression-policy=gzip` was parsed but never assigned to `cfg.CompressionPolicy`.
All snapshots were always `.zst` (default "zstd").

### Fix
`cmd/etcd-steward/main.go`:
```go
if compressEnabled, err := fs.GetBool("compress-snapshots"); err == nil && compressEnabled {
    if policy, err := fs.GetString("compression-policy"); err == nil && policy != "" {
        cfg.CompressionPolicy = policy
    }
} else if !compressEnabled {
    cfg.CompressionPolicy = "none"
}
```

---

## E2E Verification

```
kubectl logs test-0 -c backup-restore -n manual-test | grep -E "(restoring|restoration|applying delta)"

{"msg":"restoring from full snapshot","snapshot":"Backup-1775693804/Full-...-11-...zst","revision":11}
{"msg":"restoring snapshot","path":"...snapshot.db","wal-dir":"...restore-out/member/wal",...}
{"msg":"restored snapshot","path":"...snapshot.db",...}
{"msg":"applying delta snapshots","deltaCount":2,"fromRevision":11}
{"msg":"applied delta snapshot","snapshot":"Incremental-...-11-...-14-...zst","events":3,"lastRevision":14}
{"msg":"applied delta snapshot","snapshot":"Incremental-...-11-...-14-...zst","events":3,"lastRevision":14}
{"msg":"restoration completed","dataDir":"/var/etcd/data/new.etcd","revision":11}
{"msg":"full snapshot saved","revision":14,...}
```

After restore: `etcdctl get / --prefix --keys-only` returns all 13 keys.
Write-after-restore: `etcdctl put /lifecycle/phase-c/write-after-restore "test"` returns `OK`.

---

## Final State

All tests pass: `go test ./... -count=1` — 20 packages, 0 failures.

Commit: `5af3fa3 Fix restoration: use etcdutl for proper WAL+snap creation, apply deltas`
