# etcd-steward: Phased Implementation Plan

**Date:** 2026-04-06  
**Author:** Seshachalam Yv  
**Goal:** Replace etcd-backup-restore with etcd-steward — a clean-room, modular rewrite

---

## Context and Starting Point

The `phase1-etcd-member-lifecycle` worktree contains significant AI-generated work from a previous session.
Before building on it, each phase begins with an **audit step** that evaluates what was generated,
scores its quality, and records observations. Code that passes audit becomes the foundation;
code that doesn't is rewritten before moving forward.

**Repos involved:**
- `github.com/gardener/etcd-steward` — the sidecar being built (primary)
- `github.com/gardener/etcd-druid` (fork: `seshachalam-yv/etcd-druid`) — operator, owns EtcdMember CRD + feature gate
- `github.com/gardener/etcd-wrapper` — calls steward's HTTP API to start embedded etcd

**etcd-wrapper HTTP contract (must be preserved):**
- `GET  /config` — returns etcd startup config
- `POST /initialization/start` — triggers initialization; wrapper blocks until ready
- `GET  /initialization/status` — returns current initialization state
- `POST /snapshot/full` — triggers on-demand full snapshot
- `POST /snapshot/delta` — triggers on-demand delta snapshot
- `GET  /snapshot/latest` — returns latest snapshot metadata
- `GET  /healthz` — health check
- `GET  /metrics` — Prometheus metrics

---

## Phase 1: No-Backup Lifecycle (EtcdMember CRD + Feature Gate + Core Sidecar)

**Scope:** The minimal thing that can replace etcd-backup-restore in the no-backup case.
etcd-wrapper can start etcd through steward; etcd-druid can create/delete EtcdMember resources;
steward updates its own EtcdMember status. No cloud storage dependencies.

**Repos touched:** etcd-steward, etcd-druid (fork)

---

### Phase 1 — Audit Existing Work

Before any new development, evaluate the `phase1-etcd-member-lifecycle` worktree.

**Audit checklist:**

| Area | What to check | Pass criteria |
|------|--------------|---------------|
| EtcdMember CRD (etcd-druid) | Types, validation markers, generated CRD YAML | Types match design doc; CRD generated correctly; two-commit rule followed |
| `UseEtcdSteward` feature gate | Gate wiring in etcd-druid | Gate exists, disabled by default, wired through reconciler |
| Initialization state machine | `pkg/initializer/` — state transitions, error handling | Covers all 4 paths (new single-node, new multi-node learner, existing-data, restore-from-snapshot); no data loss risk |
| HTTP server | `pkg/server/` — endpoint registration, response format | All required endpoints present; responses match etcd-wrapper's expected format |
| Member lease renewal | `pkg/lease/` | Renews K8s lease on schedule; updates EtcdMember status |
| Metrics | `pkg/metrics/` | Component health metrics, state transition metrics, initialization duration |
| EtcdMember status updates | State transitions written to K8s | Transitions recorded with reason, timestamp, message |
| Configuration | `pkg/config/` | Config file + CLI flags both work; all components can be independently enabled/disabled |
| Tests | Coverage across packages | Unit tests present; table-driven; no ginkgo |
| Build/lint/CI | `make check`, `make test` | Both pass with no warnings |

**Audit output:** Write `docs/plans/2026-04-06-phase1-audit.md` recording:
- Score per area (pass / partial / fail)
- What needs rewriting
- What can be kept
- Observations and lessons

---

### Phase 1 — Task 1.1: EtcdMember CRD in etcd-druid (fork)

**Repo:** `seshachalam-yv/etcd-druid`  
**Branch:** `ai/TASK-etcd-steward-phase1/claude/etcd-member-crd`

**What:** Add the `EtcdMember` resource to etcd-druid's API — the per-member status object
that etcd-steward writes to and etcd-druid's controllers read from.

**Acceptance criteria:**
- `api/core/v1alpha1/types_etcdmember.go` defines:
  - `EtcdMember` (no Spec, Status-only) with `EtcdMemberResourceStatus`
  - State machine states: `New → Initializing → Starting → Started`
  - Sub-states: `DBValidationSanity`, `DBValidationFull`, `Restoration`, `PendingLearner`, `Learner`, `Follower`, `Leader`
  - `EtcdMemberTransition` struct (State, SubState, Reason, TransitionTime, Message)
  - `EtcdMemberRestoration` struct (Type, Status, StartTime, EndTime)
  - `EtcdMemberVolumeMismatch` struct
- CEL validation: `Transitions` max 100 items (field-scoped)
- `api/core/v1alpha1/register.go` registers `EtcdMember` and `EtcdMemberList`
- Two commits: hand-written types first, `cd api && make generate` second
- CRD YAML generated under `api/core/v1alpha1/crds/`
- `api/core/v1alpha1/crds/crd_test.go` covers EtcdMember CRD
- Label convention: `druid.gardener.cloud/owned-by: <etcd-name>` on each EtcdMember

**Verification:** `cd api && make check-generate` produces no diff

---

### Phase 1 — Task 1.2: UseEtcdSteward Feature Gate (etcd-druid fork)

**Repo:** `seshachalam-yv/etcd-druid`

**What:** Add `UseEtcdSteward` feature gate, disabled by default.
When enabled, etcd-druid deploys etcd-steward instead of etcd-backup-restore.

**Acceptance criteria:**
- `internal/features/features.go` defines `UseEtcdSteward` as an alpha gate (default off)
- Etcd reconciler: when gate enabled, uses steward image from imagevector instead of etcd-backup-restore image
- Etcd reconciler: when gate enabled, creates/deletes EtcdMember resources for each replica
- StatefulSet component: sidecar container name, command, and flags differ when gate enabled
- Gate off = existing behavior unchanged (no regression)
- Unit tests cover both gate-on and gate-off paths

---

### Phase 1 — Task 1.3: EtcdMember Lifecycle in etcd-druid (create/delete)

**Repo:** `seshachalam-yv/etcd-druid`

**What:** etcd-druid creates one EtcdMember per replica on cluster creation/scale-up and deletes
them on cluster deletion/hibernation/scale-down.

**Acceptance criteria:**
- On `Etcd` reconcile with `spec.replicas > 0` and gate enabled: one `EtcdMember` per replica created with `druid.gardener.cloud/owned-by` label
- On `Etcd` reconcile with `spec.replicas == 0` or deletion: all EtcdMember resources for that Etcd deleted
- EtcdMember name format: `<etcd-name>-<ordinal>` (e.g., `etcd-main-0`)
- Owner reference set so EtcdMember GC'd when Etcd is deleted
- `CreateAsLearner` annotation set on EtcdMember when ordinal > 0 and cluster is already running
- Etcd reconciler reads EtcdMember status to determine cluster readiness (replaces snapshot-lease-based readiness)
- Unit tests: table-driven, cover scale-up, scale-down, deletion

---

### Phase 1 — Task 1.4: etcd-steward Core — Initialization and HTTP Server

**Repo:** `github.com/gardener/etcd-steward`

**What:** The heart of Phase 1. etcd-wrapper will call `/initialization/start` → steward runs the
state machine → signals ready → wrapper starts etcd.

**Acceptance criteria (initialization state machine):**
- Path A — fresh pod, single-node: creates new etcd data directory, transitions to `Started/Leader`
- Path B — fresh pod, multi-node (`CreateAsLearner` annotation present on EtcdMember): adds self as learner via member API, waits for promotion, transitions to `Started/Follower`
- Path C — data dir exists, passes validation: starts as-is, transitions to `Started/{Leader|Follower}`
- Path D — (Phase 2) snapshot found: restore — stub/error in Phase 1 to force explicit choice
- Clean exit marker written to data dir on graceful shutdown; read on restart to pick sanity vs full validation
- `POST /initialization/start` blocks until state reaches `Started` or fails with error
- `GET /initialization/status` returns current state machine state (JSON, plain-text compatible with etcd-wrapper)
- All state transitions written to `EtcdMember.Status.Transitions` in K8s
- `GET /config` returns valid etcd startup config (initial-cluster, advertise-peer-urls, etc.)

**Acceptance criteria (HTTP server):**
- Server starts before initialization begins (always reachable)
- `GET /healthz` returns 200 when running, 503 during fatal shutdown
- `GET /metrics` serves Prometheus metrics (always registered)
- Graceful shutdown: drain in-flight requests on SIGTERM before exiting
- No goroutine leaks (context cancellation propagates)

**Acceptance criteria (configuration):**
- Config file and CLI flags both accepted; CLI flags override config file
- All components independently enable/disable via flag
- Required config validated at startup with clear error messages

---

### Phase 1 — Task 1.5: Member Lease Renewal

**Repo:** `github.com/gardener/etcd-steward`

**What:** Renew the K8s lease for this member on a heartbeat interval. etcd-druid reads
these leases to detect stale/dead members for remediation.

**Acceptance criteria:**
- Lease name: `<etcd-name>-<ordinal>` in same namespace
- Heartbeat interval: `--member-lease-renewal-interval` (default: 10s)
- Lease TTL: 3× heartbeat interval
- On startup: create lease if not exists, else renew
- On SIGTERM: stop renewing (do not delete — druid watches for expiry)
- Lease holder identity: pod name
- EtcdMember status updated with last-renewed timestamp
- Flag `--enable-member-lease-renewal` (default true)

---

### Phase 1 — Task 1.6: Metrics

**Repo:** `github.com/gardener/etcd-steward`

**What:** Prometheus metrics for observability. Focus on the no-backup Phase 1 scope.

**Acceptance criteria:**
- `etcd_steward_component_health` gauge per component (1=healthy, 0=degraded)
  - Labels: `namespace`, `name`, `component` (initializer, http_server, member_lease, leader_watch)
- `etcd_steward_initialization_duration_seconds` histogram (labels: `namespace`, `name`, `path` [A/B/C/D])
- `etcd_steward_state_transitions_total` counter (labels: `namespace`, `name`, `state`, `sub_state`, `reason`)
- All metrics registered at startup regardless of which components are enabled
- Metrics endpoint always available (even before init completes)

---

### Phase 1 — Gate: Evaluation and Observations

After all Phase 1 tasks pass their acceptance criteria:

1. Run `make check && make test` in etcd-steward — must pass clean
2. Run `make ci-checks && make test-unit` in etcd-druid fork — must pass clean
3. Manual smoke test: deploy KIND cluster, enable `UseEtcdSteward`, create single-node Etcd resource,
   verify etcd comes up and EtcdMember transitions to `Started/Leader`
4. **Write `docs/plans/2026-04-06-phase1-observations.md`** covering:
   - What the AI session generated that was correct vs what needed fixing
   - Design decisions that emerged during implementation
   - Technical debt items deferred to later phases
   - Any surprises in etcd-wrapper's HTTP contract

---

## Phase 2: Backup, Restore, Snapshotting

**Scope:** Cloud storage integration. Full snapshot + delta snapshot pipeline. Restoration from
snapshot on fresh pod. Garbage collection. etcd-backup-restore becomes fully replaceable for
backed-up clusters.

**Repos touched:** etcd-steward, etcd-druid (fork), etcd-backup-restore (reference only)

---

### Phase 2 — Audit Checkpoint

Before starting Phase 2 code, audit Phase 1 worktree for:
- Any snapstore, compression, or GC code already present (the worktree has these — evaluate quality)
- `compact` and `copy-backups` subcommands already present — audit against spec
- Decide: rewrite from scratch or build on existing

Record findings in `docs/plans/2026-04-06-phase2-audit.md`.

---

### Phase 2 — Task 2.1: Snapstore Abstraction and Local Implementation

**What:** The `Snapstore` interface and a local filesystem implementation used in unit tests
and in environments without cloud storage.

**Acceptance criteria:**
- `pkg/snapstore/snapstore.go` defines `Snapstore` interface: `Save`, `Fetch`, `List`, `Delete`
- `pkg/snapstore/local.go` implements local filesystem backend
- Snapshot filename format: `{Kind}-{StartRevision:016d}-{LastRevision:016d}-{UnixNano}`
- Atomic write: write to `.tmp`, rename to final path
- Hash stored in snapshot metadata (not in blob contents)
- `pkg/compression/` provides pluggable compression: none, gzip, zstd (use zstd as default — faster than gzip)
- `List()` returns snapshots sorted oldest-first
- Unit tests cover: save+fetch round-trip, atomic write failure, list sorting

---

### Phase 2 — Task 2.2: Cloud Provider Snapstore Backends

**What:** S3, GCS, ABS (Azure Blob Storage) backends. Delegate to etcd-backup-restore's
vendor implementations until native implementations are added (acceptable for Phase 2).

**Acceptance criteria:**
- `pkg/snapstore/s3.go`, `gcs.go`, `abs.go` implement `Snapstore`
- Backend selected by `--snapstore-provider` flag (values: `local`, `s3`, `gcs`, `abs`)
- Credentials via environment variables (standard provider SDK conventions)
- `--snapstore-container` (bucket/container name), `--snapstore-prefix` (path prefix)
- `StoreSpec.EndpointOverride` respected — if set, overrides the cloud provider endpoint URL
- Unit tests use local backend; integration tests note that provider tests require real/fake cloud
- Note in docs which tests require `TEST_S3=true` / `TEST_GCS=true` etc

---

### Phase 2 — Task 2.3: Full Snapshot Pipeline

**What:** Take a full etcd DB snapshot, compress it, upload to snapstore.

**Acceptance criteria:**
- `POST /snapshot/full` triggers a full snapshot on demand; returns 202 while in progress, 200 on complete
- Schedule: `--full-snapshot-schedule` (cron expression, default: `0 */24 * * *`)
- Snapshot taken via etcd `Snapshot()` API (not `etcdctl` subprocess)
- Compressed before upload using configured compressor
- Hash computed from uncompressed bytes; stored in snapshot metadata
- EtcdMember status: `LastFullSnapshot` updated with revision, timestamp, size
- Only the leader takes full snapshots (gate on `IsLeader` from leader watcher)
- `GET /snapshot/latest` returns metadata of most recent full+delta pair
- Unit tests: mock snapstore, verify upload called with correct metadata
- Metric: `etcd_steward_snapshot_duration_seconds{kind="full"}` histogram

---

### Phase 2 — Task 2.4: Delta Snapshot Pipeline

**What:** Incremental event-based snapshots since last full snapshot.

**Acceptance criteria:**
- `POST /snapshot/delta` triggers delta snapshot on demand
- Schedule: `--delta-snapshot-period` (default: `20s`)
- Delta snapshot watches etcd event stream from `LastRevision + 1`
- Max delta size: `--max-delta-snapshot-size` (default: `100MiB`)
- Max total events: `--max-delta-events` (default: `1_000_000`) — trigger compaction if exceeded
- Delta snapshots taken by leader only
- Metric: `etcd_steward_snapshot_duration_seconds{kind="delta"}` histogram
- Unit tests: mock etcd watch, verify delta written on size/event threshold

---

### Phase 2 — Task 2.5: Restoration from Snapshot

**What:** When a fresh pod starts and no valid local data exists but snapshots are in the snapstore,
restore from the most recent full + delta chain.

**Acceptance criteria:**
- Initialization Path D: snapstore has at least one full snapshot → download + restore
- Download full snapshot, decompress, write to data dir
- Apply all delta snapshots in order up to latest
- Verify hash of downloaded full snapshot
- State machine transitions: `Initializing/Restoration` → `Started/{Leader|Follower}`
- `EtcdMember.Status.LastRestoration` updated with type, status, timing
- Multi-node: restoration only runs on pod 0; pods 1-N join as learners (sync from leader)
- `--enable-restoration` flag (default: true)
- Unit tests: mock snapstore with known snapshot sequence, verify data dir state
- Metric: `etcd_steward_restoration_duration_seconds` histogram

---

### Phase 2 — Task 2.6: Snapshot Garbage Collection

**What:** Bound disk/cloud-storage usage by deleting old snapshots.

**Acceptance criteria:**
- Retention policy: retain last N full snapshot sets (default: 7)
- A "snapshot set" = one full snapshot + all deltas taken before the next full snapshot
- GC never deletes the latest set
- `--garbage-collection-period` enables GC (default: `12h`; 0 disables)
- Only the leader runs GC
- `--garbage-collection-policy` selects policy: `Exponential` (default), `LimitBased`
- Unit tests: construct snapshot list, verify correct set deleted, latest set always preserved

---

### Phase 2 — Task 2.7: E2E Verification

**What:** End-to-end test proving backup + restore works in a KIND cluster.

**Acceptance criteria:**
- KIND cluster with `UseEtcdSteward` enabled in etcd-druid
- Single-node Etcd resource with local snapstore
- Write data to etcd → full snapshot taken → delete data dir → pod restart → restoration runs → data present
- Multi-node: 3-node Etcd resource, write data, scale to 0 (hibernation snapshot), scale back to 3, verify data
- Test runs via `make ci-e2e-kind` in etcd-druid fork
- Observations from e2e written to `docs/plans/2026-04-06-phase2-observations.md`

---

### Phase 2 — Gate: Evaluation and Observations

After Phase 2 tasks pass:

1. `make check && make test` in etcd-steward
2. `make ci-e2e-kind PROVIDERS="local"` in etcd-druid fork
3. Write `docs/plans/2026-04-06-phase2-observations.md` covering:
   - Backup/restore correctness issues found during testing
   - Performance characteristics (snapshot size, upload time)
   - Differences from etcd-backup-restore behavior that need etcd-wrapper changes
   - Security gaps deferred to Phase 3

---

## Phase 3: Production Ready

**Scope:** Everything needed for a real shoot cluster. Multi-cloud backup, defragmentation,
alarm handling, distributed coordination, security hardening, full compaction integration
with etcd-druid, multi-site backup, etcd-wrapper leadership notification.

---

### Phase 3 — Tasks

#### 3.1: Defragmentation (with distributed lock)
- `pkg/lock/` etcd-backed distributed lock: grant lease → atomic Txn → watch on contention
- Lock key: `/druid/lock/{namespace}/{name}-defrag`
- Defrag schedule: `--defragmentation-schedule` (cron expression)
- Space alarm triggers defrag immediately (bypasses schedule)
- `EtcdMember.Status.LastDefragmentation` updated (startTime, endTime, initialDBSize, finalDBSize)
- Metric: `etcd_steward_defragmentation_duration_seconds{namespace, name, status_code}`

#### 3.2: Alarm Handling
- `pkg/alarm/` polls `AlarmList()` on configurable interval
- NOSPACE: compact history to `currentRevision - compactRevisionLag`, defrag, disarm
- CORRUPT: log error, do not auto-disarm, set component health metric to 0
- `--alarm-check-interval` (default: `30s`)

#### 3.3: etcd-wrapper Leadership Notification
- etcd-wrapper needs to know when steward's init is done and whether this member is leader/follower
- Two options evaluated in Phase 1 observations; implement chosen option here
- Steward writes to dedicated etcd key watched by wrapper, OR steward pushes HTTP notification to wrapper

#### 3.4: Compact and CopyBackups Subcommands
- `etcd-steward compact --revision=N` — called by etcd-druid compaction controller via `kubectl exec`
- `etcd-steward copy-backups` — copies snapshots between two storespecs (for disaster recovery)
- Both subcommands must parse same config format as the default command

#### 3.5: Compaction Controller Integration (etcd-druid)
- etcd-druid compaction controller watches EtcdMember instead of snapshot leases
- Uses `AccumulatedDeltaSize` from EtcdMember status to trigger compaction
- `kubectl exec` call updated to use `etcd-steward compact` instead of `etcdbrctl compact`

#### 3.6: Multi-Site Backup (Dual Target)
- Secondary backup target via second `StoreSpec`
- Same snapshot uploaded to both primary and secondary
- GC runs independently per target
- Failure of secondary does not block primary

#### 3.7: EtcdMember Info Provider
- HTTP endpoint exposing: MemberID, ClusterID, State, DBSize, DBSizeInUse
- etcd-druid reads this for status aggregation
- Replaces etcd-backup-restore's `GET /config` info payload

#### 3.8: Calendar-Based GC Policy
- Hierarchical retention: Hour/Day/Week/Month with max-per-unit and number-of-units
- Only implement after time-based and count-based are proven correct

#### 3.9: Scale-Up Correctness (observedClusterSize)
- `observedClusterSize` field in EtcdMember or Etcd status
- etcd-druid writes `initial-cluster` config based on `observedClusterSize`, not `spec.replicas`
- Prevents data corruption when a new member starts with wrong initial-cluster peers

#### 3.10: Security Hardening
- Separate IAM roles per operation (Snapshotter: read/write; GC: delete/lock; Compactor: read/write)
- Document threat model S-1 through S-3 (see `etcd-steward-notes/security.md`)
- Evaluate AWS S3 Object Lock for immutability of snapshots

#### 3.11: Performance and Load Tests
- Load test: write 10k keys/sec, verify delta snapshots keep up
- Restore performance: snapshot size vs restore time correlation
- GC sweep performance with 1000+ snapshots

---

### Phase 3 — Gate: Evaluation and Observations

1. Full CI passes: `make ci-e2e-kind PROVIDERS="none,local,s3"` in etcd-druid fork
2. etcd-backup-restore parity checklist: every HTTP endpoint, every flag, every behavior documented
   in ebr-notes.md verified to exist in steward or explicitly deferred
3. Write `docs/plans/2026-04-06-phase3-observations.md` — final lessons learned

---

## Cross-Cutting Invariants (all phases)

| Invariant | Detail |
|-----------|--------|
| HTTP contract | etcd-wrapper's `/config`, `/initialization/start`, `/initialization/status`, `/snapshot/full`, `/snapshot/delta`, `/snapshot/latest`, `/healthz`, `/metrics` endpoints must always respond in etcd-wrapper's expected format |
| No Ginkgo | Use standard `testing` package + table-driven tests throughout |
| No inline code in skills | Plugin stays thin; conventions go in `docs/development/` |
| Two-commit API rule | Any etcd-druid API change: types first, `cd api && make generate` second |
| Feature gate default off | `UseEtcdSteward` stays alpha/disabled-by-default until Phase 3 complete |
| EtcdMember status is steward-owned | Only etcd-steward writes to `EtcdMember.Status`; etcd-druid only reads it |
| Clean exit marker | Steward writes marker to data dir on graceful shutdown for sanity-vs-full validation decision |

---

## Sequencing Summary

```
Phase 1 Audit
    → 1.1 EtcdMember CRD (etcd-druid)
    → 1.2 UseEtcdSteward Feature Gate (etcd-druid)
    → 1.3 EtcdMember Lifecycle — create/delete (etcd-druid)
    → 1.4 Core sidecar — initialization + HTTP server (etcd-steward)
    → 1.5 Member lease renewal (etcd-steward)
    → 1.6 Metrics (etcd-steward)
    → Phase 1 Gate + Observations

Phase 2 Audit
    → 2.1 Snapstore abstraction + local backend
    → 2.2 Cloud provider backends
    → 2.3 Full snapshot pipeline
    → 2.4 Delta snapshot pipeline
    → 2.5 Restoration from snapshot
    → 2.6 Snapshot GC
    → 2.7 E2E verification
    → Phase 2 Gate + Observations

Phase 3
    → 3.1 Defragmentation
    → 3.2 Alarm handling
    → 3.3 Leadership notification (etcd-wrapper)
    → 3.4 Compact + CopyBackups subcommands
    → 3.5 Compaction controller integration (etcd-druid)
    → 3.6 Multi-site backup
    → 3.7 EtcdMember info provider
    → 3.8–3.11 (calendar GC, scale-up, security, perf)
    → Phase 3 Gate + Observations
```

---

## Decision Log

| Decision | Rationale |
|----------|-----------|
| No ginkgo | Notes explicitly say "use standard testing, avoid ginkgo" |
| zstd default compression over gzip | Notes say gzip is slower and less efficient; zstd is better default |
| Local snapstore first | Decouples Phase 1/2 from cloud credential setup; unblocks all other work |
| Delegate to ebr vendor for cloud backends (Phase 2) | Shipping over re-inventing; replace with native later |
| EtcdMember has no Spec | Status-only resource — steward owns status, druid owns lifecycle |
| Audit before building | Previous AI session generated code of unknown quality; inspect before trusting |
| `UseEtcdSteward` alpha, disabled by default | No production impact until Phase 3 complete |
