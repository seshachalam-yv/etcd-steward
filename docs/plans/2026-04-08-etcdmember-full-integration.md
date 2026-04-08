<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# etcd-steward: Full EtcdMember CRD Integration

**Date:** 2026-04-08  
**Branch (etcd-steward):** `ai/etcd-steward/claude/production-ready-v2`  
**Branch (etcd-druid fork):** `ai/etcd-steward-phase1/claude/etcd-member-crd`  
**Gate 1:** Pre-approved  
**Gate 2:** Requires human approval before push

---

## Issue

**Link:** https://github.com/gardener/etcd-steward/issues/1  
**Summary:** Rewrite etcd-backup-restore as etcd-steward with full EtcdMember CRD integration — the per-member sidecar updates EtcdMember status for all operational events (snapshots, defrag, DB size, transitions, conditions).

---

## Context

### What exists on `production-ready-v2`

The branch contains a fully working Phase 1–2 implementation:

| Package | What it does | EtcdMember wired? |
|---------|-------------|-------------------|
| `pkg/initializer/` | DEP-04 4-path init (new single-node, learner join, existing-data, restore) | ✅ transitions + lastRestoration |
| `pkg/statemachine/recorder.go` | Writes `EtcdMember.status.transitions` via dynamic client | ✅ used by initializer + leaderwatch |
| `pkg/member/client.go` | Patches `EtcdMember.status` (id, clusterID, lastRestoration) | ✅ used by initializer |
| `pkg/leaderwatch/` | Polls etcd status, records Leader/Follower transitions | ✅ transitions only |
| `pkg/snapshotter/` | Full + delta snapshot pipeline | ❌ no snapshot status update |
| `pkg/alarm/` | NOSPACE/CORRUPT defrag + disarm | ❌ no lastDefragmentation update |
| `pkg/gc/` | Snapshot garbage collection | — n/a |
| `pkg/lease/` | K8s member lease renewal | — n/a |
| `pkg/lock/` | Distributed etcd lock for snapshotter coordination | — n/a |
| `pkg/server/` | HTTP server (all 8 etcd-wrapper endpoints) | — n/a |
| `pkg/metrics/` | Prometheus metrics | — n/a |

### What exists on `etcd-member-crd` (etcd-druid fork)

- `EtcdMember` CRD fully defined (`api/core/v1alpha1/types_etcdmember.go`)
- `EtcdMemberResourceStatus` has: `id`, `clusterID`, `peerTLSEnabled`, `dbSize`, `dbSizeInUse`, `snapshots`, `lastDefragmentation`, `transitions`, `volumeMismatches`, `lastRestoration`
- Component creates/deletes EtcdMember CRs; sets `druid.gardener.cloud/create-as-learner` annotation on scale-up
- `UseEtcdSteward` feature gate wired

### Gap analysis

The following EtcdMember status fields are **not yet populated** by etcd-steward:

| Field | Owner | Gap |
|-------|-------|-----|
| `status.snapshots.lastFull` | snapshotter | Not written after full snapshot |
| `status.snapshots.lastDelta` | snapshotter | Not written after delta snapshot |
| `status.snapshots.accumulatedDeltaSize` | snapshotter | Not tracked or written |
| `status.dbSize` / `status.dbSizeInUse` | leaderwatch or periodic loop | Not written anywhere |
| `status.peerTLSEnabled` | config at startup | Not written |
| `status.lastDefragmentation` | alarm handler | Not written after defrag |
| `status.conditions[DataVolumeReadOnly]` | initializer/server | Not implemented |

Additionally, the current `member.Client` only supports **direct synchronous patches**. The notes specify an **InfoProvider pattern**: background goroutine that periodically collects from registered providers and makes one batched patch call — avoiding N concurrent patch races.

---

## Chosen Approach

### Approach A: InfoProvider background reconciler (chosen)

Add a `MemberStatusReconciler` to `pkg/member/` that:
1. Accepts synchronous `RecordTransition(ctx, transition)` calls (mandatory, blocking, for init path)
2. Accepts `RegisterProvider(id, InfoProvider)` — periodic async info collection
3. Runs a background goroutine that ticks every `StatusUpdateInterval` (default 30s), queries all providers, and patches EtcdMember status in one batched merge-patch

Each operational component (snapshotter, alarm, leaderwatch) implements `InfoProvider` and provides its latest known values.

### Why not Approach B (per-call patches from each component)

Per-call patches from multiple goroutines cause concurrent status update races — last-writer wins, and one component can clobber another's fields. The InfoProvider pattern serializes all writes through one goroutine.

### Why not Approach C (one big status struct passed around)

A shared status struct requires global locking across all components and creates tight coupling. InfoProvider is looser: each component owns its own memory.

---

## Change Type

- [x] New package methods (`pkg/member/`)
- [x] Wiring changes (`pkg/snapshotter/`, `pkg/alarm/`, `pkg/leaderwatch/`, `cmd/etcd-steward/main.go`)
- [x] New conditions infrastructure (`pkg/member/conditions.go`)
- [ ] API change (no — EtcdMember CRD already has all fields in druid fork)
- [ ] Test only

---

## Tasks

### Task 1 — `pkg/member/`: InfoProvider pattern + MemberStatusReconciler

**Scope:** `pkg/member/reconciler.go`, `pkg/member/reconciler_test.go`  
**Model:** sonnet  
**Tests:** unit  
**API generation:** no

**What to build:**

```go
// InfoProvider supplies a snapshot of operational status to MemberStatusReconciler.
// Each component that needs to update EtcdMember.status implements this interface.
type InfoProvider interface {
    // ProvideInfo returns the current status contribution from this provider.
    // Nil fields in the returned StatusInfo are ignored (not patched).
    // Called by MemberStatusReconciler on every reconcile tick; must be fast and non-blocking.
    ProvideInfo() StatusInfo
}

// StatusInfo is the aggregated status contribution from all InfoProviders.
// Only non-nil fields are written to EtcdMember.status.
type StatusInfo struct {
    DBSize              *resource.Quantity
    DBSizeInUse         *resource.Quantity
    PeerTLSEnabled      *bool
    Snapshots           *SnapshotInfo  // replaces EtcdMember.status.snapshots
    LastDefragmentation *DefragInfo
}

// SnapshotInfo mirrors EtcdMemberSnapshots from the druid API but is local to etcd-steward.
type SnapshotInfo struct {
    LastFull             *SnapshotEntry
    LastDelta            *SnapshotEntry
    AccumulatedDeltaSize *resource.Quantity
}

// SnapshotEntry mirrors EtcdMemberSnapshotInfo from the druid API.
type SnapshotEntry struct {
    Name          string
    Timestamp     metav1.Time
    StartRevision int64
    EndRevision   int64
    Size          *resource.Quantity
}

// DefragInfo mirrors EtcdMemberDefragmentation from the druid API.
type DefragInfo struct {
    StartTime     metav1.Time
    EndTime       *metav1.Time
    InitialDBSize *resource.Quantity
    FinalDBSize   *resource.Quantity
    Reason        *string
    Message       *string
}

// MemberStatusReconciler batches status updates from multiple InfoProviders.
type MemberStatusReconciler struct { ... }

// New creates a MemberStatusReconciler.
func New(client Client, memberName, namespace string, interval time.Duration, logger *zap.Logger) *MemberStatusReconciler

// RegisterProvider registers an InfoProvider under a unique ID.
func (r *MemberStatusReconciler) RegisterProvider(id string, p InfoProvider)

// Run starts the periodic reconcile loop until ctx is cancelled.
func (r *MemberStatusReconciler) Run(ctx context.Context) error
```

**Acceptance criteria:**
- `MemberStatusReconciler.Run()` ticks every `interval`, collects all providers, and calls `Client.UpdateStatus()` with merged info
- Non-nil fields from ANY provider appear in the patch; nil fields are omitted
- Multiple providers can register; their outputs are merged (last-wins per field)
- If `Client.UpdateStatus()` returns an error, it is logged + metric set to 0, but loop continues
- `ComponentHealth` metric for `"member-status-reconciler"` tracks health per tick
- Test: table-driven, covers: zero providers, one provider with all fields, two providers with overlapping fields (merge), client error (loop continues), ctx cancellation stops loop

---

### Task 2 — `pkg/member/`: Conditions support (DataVolumeReadOnly)

**Scope:** `pkg/member/conditions.go`, `pkg/member/conditions_test.go`  
**Model:** sonnet  
**Tests:** unit  
**API generation:** no

**What to build:**

The notes specify an explicit need for `DataVolumeReadOnly` condition in `EtcdMember.status.conditions`:

```go
// Condition mirrors metav1.Condition for EtcdMember.
type Condition struct {
    Type               string
    Status             string   // "True" | "False" | "Unknown"
    Reason             string
    Message            string
    LastTransitionTime metav1.Time
}

const ConditionDataVolumeReadOnly = "DataVolumeReadOnly"
```

Add to `Client` interface:
```go
// SetCondition patches a single condition in EtcdMember.status.conditions.
SetCondition(ctx context.Context, memberName, namespace string, condition Condition) error
```

The condition check is triggered by the `server` package when the `/initialization/start` response includes a read-only filesystem error, or by the `initializer` when it detects the data volume is read-only.

**Implementation:** `SetCondition` uses JSON merge patch on `status` subresource. The conditions slice must be read-modify-write (same pattern as transitions recorder) to avoid clobbering other conditions.

**Acceptance criteria:**
- `SetCondition("DataVolumeReadOnly", "True", "ReadOnlyFileSystem", "...")` creates or updates the condition entry in the slice
- Re-calling with the same Type+Status does NOT update `lastTransitionTime` (idempotent)
- Re-calling with different Status DOES update `lastTransitionTime`
- `NoopClient` implements `SetCondition` (returns nil)
- Test: table-driven covering: set new condition, update existing (same status → no time change), update existing (different status → time changes), client error propagated

---

### Task 3 — `pkg/snapshotter/`: InfoProvider impl + snapshot status tracking

**Scope:** `pkg/snapshotter/snapshotter.go`, `pkg/snapshotter/snapshotter_test.go`  
**Model:** sonnet  
**Tests:** unit  
**API generation:** no

**What to build:**

Add `InfoProvider` implementation to `Snapshotter`:

```go
// Add to Snapshotter struct:
mu            sync.Mutex
lastRevision  int64           // already exists
snapshotInfo  member.SnapshotInfo  // NEW: tracks latest snapshot info for InfoProvider

// Snapshotter implements member.InfoProvider.
func (s *Snapshotter) ProvideInfo() member.StatusInfo {
    s.mu.Lock()
    defer s.mu.Unlock()
    snap := s.snapshotInfo  // copy
    return member.StatusInfo{Snapshots: &snap}
}
```

After each successful full or delta snapshot, update `s.snapshotInfo`:
- Full snapshot: set `LastFull`, reset `AccumulatedDeltaSize` to zero, set `LastDelta = nil`
- Delta snapshot: set `LastDelta`, increment `AccumulatedDeltaSize` by size of delta

The `size` for `SnapshotEntry` is the **uncompressed** size (read from `len(data)` before compression).

**Acceptance criteria:**
- After `TriggerFullSnapshot()` succeeds: `ProvideInfo().Snapshots.LastFull.EndRevision == currentRev`
- After `TriggerFullSnapshot()` succeeds: `ProvideInfo().Snapshots.AccumulatedDeltaSize` is zero
- After `TriggerDeltaSnapshot()` succeeds: `ProvideInfo().Snapshots.LastDelta.StartRevision == lastRevision + 1`
- After `TriggerDeltaSnapshot()` succeeds: `ProvideInfo().Snapshots.AccumulatedDeltaSize` increments by delta size
- After second `TriggerFullSnapshot()`: `AccumulatedDeltaSize` resets to zero (full snapshot resets delta accumulation)
- `ProvideInfo()` is safe to call concurrently with `TriggerFullSnapshot()` / `TriggerDeltaSnapshot()`
- If snapshot fails, `snapshotInfo` is NOT updated
- Tests: table-driven, cover full-then-delta, full-resets-delta, concurrent read safety

---

### Task 4 — `pkg/alarm/`: InfoProvider impl + defrag status tracking

**Scope:** `pkg/alarm/alarm.go`, `pkg/alarm/alarm_test.go`  
**Model:** sonnet  
**Tests:** unit  
**API generation:** no

**What to build:**

Add `InfoProvider` implementation to the alarm handler:

```go
// Add to Handler struct:
mu              sync.Mutex
lastDefrag      *member.DefragInfo  // nil until first defrag

// Handler implements member.InfoProvider.
func (h *Handler) ProvideInfo() member.StatusInfo {
    h.mu.Lock()
    defer h.mu.Unlock()
    if h.lastDefrag == nil {
        return member.StatusInfo{}
    }
    d := *h.lastDefrag // copy
    return member.StatusInfo{LastDefragmentation: &d}
}
```

After every defragmentation (both NOSPACE-triggered and scheduled), populate `lastDefrag`:
- `StartTime`, `EndTime`, `Reason` (`"NSPACEAlarm"` or `"Scheduled"` or `"DBSizeThreshold"`)
- `InitialDBSize` — from etcd status before defrag
- `FinalDBSize` — from etcd status after defrag
- `Message` — optional summary

**Acceptance criteria:**
- `ProvideInfo().LastDefragmentation` is nil before first defrag
- After NOSPACE-triggered defrag: `LastDefragmentation.Reason == "NSPACEAlarm"`
- After successful defrag: `EndTime` is non-nil
- `ProvideInfo()` is safe to call concurrently with defrag
- Tests: nil before defrag, NOSPACE-triggered, success path sets EndTime, concurrent safety

---

### Task 5 — `pkg/leaderwatch/`: DBSize + DBSizeInUse InfoProvider

**Scope:** `pkg/leaderwatch/leaderwatch.go`, `pkg/leaderwatch/leaderwatch_test.go`  
**Model:** sonnet  
**Tests:** unit  
**API generation:** no

**What to build:**

The etcd `Status()` RPC already returns `DbSize` and `DbSizeInUse`. LeaderWatcher already calls `Status()` on every poll tick. Add:

```go
// Add to LeaderWatcher struct:
mu          sync.RWMutex  // already exists
currentRole Role          // already exists
dbSize      int64         // NEW
dbSizeInUse int64         // NEW

// LeaderWatcher implements member.InfoProvider.
func (w *LeaderWatcher) ProvideInfo() member.StatusInfo {
    w.mu.RLock()
    defer w.mu.RUnlock()
    dbSize := resource.NewMilliQuantity(w.dbSize*1000, resource.BinarySI)
    dbSizeInUse := resource.NewMilliQuantity(w.dbSizeInUse*1000, resource.BinarySI)
    return member.StatusInfo{
        DBSize:      dbSize,
        DBSizeInUse: dbSizeInUse,
    }
}
```

After each `checkLeadership()` call, update `w.dbSize` and `w.dbSizeInUse` from the status response.

**Acceptance criteria:**
- `ProvideInfo().DBSize` reflects the value returned by most recent `Status()` call
- Zero before first successful poll
- `ProvideInfo()` is safe to call concurrently with `Run()`
- If `Status()` returns error, DBSize/DBSizeInUse are NOT updated (retain last good value)
- Tests: first poll sets values, error does not update, concurrent safety

---

### Task 6 — `pkg/member/`: PeerTLSEnabled startup write

**Scope:** `pkg/member/client.go`  
**Model:** haiku  
**Tests:** unit (extend existing)  
**API generation:** no

**What to build:**

Add `PeerTLSEnabled` to `UpdateStatusOpts`:

```go
type UpdateStatusOpts struct {
    MemberID        *string
    ClusterID       *string
    LastTransition  *statemachine.Transition
    LastRestoration *LastRestorationStatus
    PeerTLSEnabled  *bool   // NEW
}
```

And in `UpdateStatus()`, include it in the patch when non-nil:
```go
if opts.PeerTLSEnabled != nil {
    statusFields["peerTLSEnabled"] = *opts.PeerTLSEnabled
}
```

**Wiring:** In `cmd/etcd-steward/main.go`, immediately after successful initialization, write `PeerTLSEnabled` based on config:
```go
peerTLS := cfg.Etcd.PeerTLSEnabled // bool from config
if err := memberClient.UpdateStatus(ctx, memberName, namespace, member.UpdateStatusOpts{
    PeerTLSEnabled: &peerTLS,
}); err != nil {
    logger.Warn("failed to set peerTLSEnabled on EtcdMember", zap.Error(err))
}
```

**Acceptance criteria:**
- `UpdateStatus()` with `PeerTLSEnabled: ptr(true)` includes `"peerTLSEnabled": true` in patch
- `UpdateStatus()` with `PeerTLSEnabled: nil` does NOT include `peerTLSEnabled` in patch (unchanged)
- Test: extend `TestK8sMemberClient_UpdateStatus` table to cover PeerTLSEnabled nil and non-nil

---

### Task 7 — `cmd/etcd-steward/main.go`: Wire MemberStatusReconciler + all providers

**Scope:** `cmd/etcd-steward/main.go`  
**Model:** sonnet  
**Tests:** integration smoke test extension  
**API generation:** no

**What to build:**

Wire `MemberStatusReconciler` as the central status update hub:

```go
// After all components are constructed:
reconciler := member.NewStatusReconciler(
    memberClient,
    memberName, namespace,
    30*time.Second, // StatusUpdateInterval
    logger,
)
reconciler.RegisterProvider("snapshotter", snapshotter)
reconciler.RegisterProvider("leaderwatch", leaderWatcher)
reconciler.RegisterProvider("alarm", alarmHandler)

// Start in goroutine alongside other long-running components:
errGroup.Go(func() error {
    return reconciler.Run(ctx)
})
```

Also wire PeerTLSEnabled write immediately after initialization completes (Task 6 wiring).

**Acceptance criteria:**
- `MemberStatusReconciler` is started as a goroutine in main.go
- All three providers (snapshotter, leaderwatch, alarm) are registered
- If `--enable-snapshots=false`, snapshotter provider is NOT registered (avoid nil pointer)
- `reconciler.Run()` error propagates to errGroup and causes clean shutdown
- Smoke test: extend `pkg/integration/integration_test.go` to verify MemberStatusReconciler starts without error with noop providers

---

### Task 8 — `pkg/snapshotter/`: Snapshot hash via etcdutl metadata

**Scope:** `pkg/snapshotter/snapshotter.go`  
**Model:** haiku  
**Tests:** unit  
**API generation:** no

**What to build:**

The notes say:  
> _"Save the snapshot to a file. Use etcd's snapshot status API to get the Snapshot status information which contains the revision. Use this revision and make it part of the file name."_

Currently `TriggerFullSnapshot()` calls `s.getCurrentRevision()` BEFORE taking the snapshot. This can be a different revision from what ended up in the snapshot file. Fix:

1. Save snapshot bytes to a temp file (or memory buffer)
2. Use `bbolt.Open` + read the `meta` key or use `go.etcd.io/etcd/etcdutl/snapshot.Status()` to read the actual revision from the snapshot file
3. Use that revision for `snap.LastRevision` and the file name

Since etcd-steward's `go.mod` doesn't include `etcdutl`, the simpler approach is to **read the revision from the bbolt snapshot directly using the existing `go.etcd.io/bbolt` dependency**:

```go
// After writing snapshot to buffer, read actual revision:
// The snapshot is a bbolt DB file. The revision is stored in the "meta" bucket.
// Use bbolt.Open in read-only mode on a temp file to extract it.
actualRev, err := readRevisionFromSnapshot(tmpFile)
```

**Implementation note:** If reading the revision is too complex (bbolt snapshot format is internal), use the simpler alternative from the notes: pass `--snapshot-revision` via the `/snapshot/latest` endpoint metadata rather than the file name — but do add the actual revision from bbolt, not the pre-snapshot revision.

**Acceptance criteria:**
- The `LastRevision` in the saved snapshot name comes from the snapshot content, not a pre-snapshot etcd status call
- If revision extraction fails, fall back to current behavior (log warning + use pre-snapshot revision)
- Tests: mock snapshot bytes; verify that extracted revision matches expected value

---

### Task 9 — Cleanup: Remove snapshot lease package

**Scope:** `pkg/snapshotlease/` — **delete this package**  
**Model:** haiku  
**Tests:** n/a  
**API generation:** no

Per the design notes:
> _"Stop creating snapshot leases; delete existing snapshot leases"_ (task-list.md)

The `pkg/snapshotlease/` package implements K8s snapshot leases, which are being replaced by `EtcdMember.status.snapshots`. This package:
- Has no callers in the current codebase (confirmed by grep)
- Is a vestigial concept from etcd-backup-restore

**What to do:**
1. Confirm no callers: `grep -r "snapshotlease" --include="*.go"` 
2. Delete `pkg/snapshotlease/`
3. Remove from any imports

**Acceptance criteria:**
- `pkg/snapshotlease/` directory does not exist
- `make check && make test` pass after deletion
- No references to `snapshotlease` in any `.go` file

---

### Task 10 — Tests: Integration smoke test for full EtcdMember status flow

**Scope:** `pkg/integration/integration_test.go`  
**Model:** sonnet  
**Tests:** integration  
**API generation:** no

**What to build:**

Extend the existing integration smoke test to cover the full EtcdMember status update path using noop/fake components. The test must verify:

1. `MemberStatusReconciler.Run()` with registered providers produces a merged `UpdateStatusOpts` containing values from all providers
2. The `K8sRecorder.Record()` + `K8sMemberClient.UpdateStatus()` calls are made with correct field values

Use the existing `FakeDynamicClient` or a new minimal fake that captures patch calls.

**Acceptance criteria:**
- Test: `TestMemberStatusIntegration` exists and passes
- Test verifies that after one reconcile tick, snapshots info from snapshotter provider and dbSize from leaderwatch provider BOTH appear in a single patch call
- Test verifies that a transition recorded via `K8sRecorder` appears in the patch
- Test is table-driven where possible
- `make test` passes including this test

---

## Commit Strategy

Each task maps to **one commit** (plus any generated code commits if API changes — none here).

Commit messages follow the pattern: `<verb>(<scope>): <subject> (#1)`

| Task | Commit message |
|------|---------------|
| Task 1 | `feat(member): add InfoProvider pattern and MemberStatusReconciler (#1)` |
| Task 2 | `feat(member): add Conditions support and DataVolumeReadOnly condition (#1)` |
| Task 3 | `feat(snapshotter): implement InfoProvider for snapshot status tracking (#1)` |
| Task 4 | `feat(alarm): implement InfoProvider for defragmentation status tracking (#1)` |
| Task 5 | `feat(leaderwatch): implement InfoProvider for DBSize and DBSizeInUse tracking (#1)` |
| Task 6 | `feat(member): add PeerTLSEnabled to UpdateStatusOpts and write on startup (#1)` |
| Task 7 | `feat(cmd): wire MemberStatusReconciler with all providers in main.go (#1)` |
| Task 8 | `fix(snapshotter): use snapshot content revision instead of pre-snapshot etcd status (#1)` |
| Task 9 | `chore: delete snapshotlease package (replaced by EtcdMember.status.snapshots) (#1)` |
| Task 10 | `test(integration): add smoke test for full EtcdMember status update flow (#1)` |

---

## PR Structure (etcd-steward)

All 10 tasks land in **one PR** on the etcd-steward repo against `main`. The PR is logically divided into:

- **Commits 1–2:** Core infrastructure (InfoProvider + Conditions)
- **Commits 3–5:** Per-component InfoProvider impls (no main.go changes)
- **Commit 6:** PeerTLSEnabled startup write
- **Commit 7:** Main.go wiring (all pieces come together)
- **Commits 8–9:** Cleanup (snapshot revision accuracy + snapshotlease removal)
- **Commit 10:** Integration tests

This order means the PR can be reviewed bottom-up: reviewers verify the interface first (commits 1–2), then verify each component provides the right data (commits 3–5), then verify the wiring is correct (commit 7).

---

## PR Checklist

- [ ] `make check` (golangci-lint) passes
- [ ] `make test` passes
- [ ] All 10 acceptance criteria sections have passing tests
- [ ] No snapshot lease references in any `.go` file
- [ ] `EtcdMember.status.snapshots` is written after every full/delta snapshot
- [ ] `EtcdMember.status.lastDefragmentation` is written after every defrag
- [ ] `EtcdMember.status.dbSize` / `dbSizeInUse` updated on every LeaderWatcher poll
- [ ] `EtcdMember.status.peerTLSEnabled` written once at startup
- [ ] `MemberStatusReconciler` registered in main.go errGroup
- [ ] E2e verification: not required for this PR (no controller changes; sidecar tested via integration smoke test)

---

## Rollback

If this PR causes problems:
1. Revert the PR: all changes are additive (new methods) or cleanups (snapshotlease removal)
2. The snapshotlease package can be restored from git history if needed: `git checkout HEAD~N -- pkg/snapshotlease/`
3. MemberStatusReconciler can be disabled by simply not calling `reconciler.Run()` in main.go

---

## Notes for Reviewer

- The `InfoProvider` interface is deliberately simple (`ProvideInfo() StatusInfo`). Components store their own state; the reconciler just collects and patches.
- Concurrent safety: each component owns a `sync.Mutex` for its local info. The reconciler does NOT hold a lock when calling `ProvideInfo()` — providers must be safe to call without external synchronization.
- The `SnapshotEntry.Size` field stores the **uncompressed** size intentionally — druid's compaction controller uses accumulated delta size for quota decisions, so compressed size would undercount.
- Conditions (Task 2) are implemented as a separate `SetCondition()` call (not via InfoProvider) because conditions are event-driven (set when detected) not periodic.
