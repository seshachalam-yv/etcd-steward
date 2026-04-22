> **For agentic workers:** Gate 1 is pre-approved for all etcd-steward sessions.
> Execute sessions in order. Each session is self-contained with its own tasks.

## Issue
Link: https://github.com/gardener/etcd-steward/issues/1
Summary: Production roadmap for etcd-steward — Sessions 2-6 covering all remaining capabilities.

## Fork Root
Path: /Users/I568019/go/src/github.com/seshachalam-yv/etcd-steward

## Change Type
[x] Enhancement to existing packages
[x] New packages for cloud providers and multi-node coordination

## Current State (after Session 1)
- 17 internal packages, 130+ unit tests passing with -race
- Full snapshots: working (etcdutl Snapshot API)
- Delta snapshots: event-based via Watch API (Session 1)
- Delta restoration: event replay via KV (Session 1)
- Feature gate: UseEtcdSteward alpha, all 40 druid e2e pass
- Local snapstore: working
- Cloud providers: NONE
- Multi-node: lifecycle passes e2e but no learner-join logic
- Compaction: base snapshot only, no delta application cycle
- EtcdMember CRD: not created

---

## Session 2: Cloud Provider Snapstore (Issues #7, #8)

### Design
Implement S3, GCS, and ABS providers for the snapstore interface. Each provider implements Upload/Download/GetInfo/List/Delete. Compression is transparent (already handled by the compression package wrapping the snapstore).

### Tasks

- [ ] Task 1: Snapstore provider factory
      Files: internal/snapstore/factory.go, internal/snapstore/factory_test.go
      Tests: unit
      Acceptance criteria:
      - `TestNewSnapStore_Local`: factory returns *LocalSnapStore for provider "Local"
      - `TestNewSnapStore_S3`: factory returns *S3SnapStore for provider "S3"
      - `TestNewSnapStore_GCS`: factory returns *GCSSnapStore for provider "GCS"
      - `TestNewSnapStore_ABS`: factory returns *ABSSnapStore for provider "ABS"
      - `TestNewSnapStore_Unknown`: returns error for unknown provider

- [ ] Task 2: S3 snapstore provider
      Files: internal/snapstore/s3.go, internal/snapstore/s3_test.go
      Tests: unit (mock S3 client)
      Deps: github.com/aws/aws-sdk-go-v2
      Acceptance criteria:
      - `TestS3SnapStore_Upload`: uploads object to S3 with correct key
      - `TestS3SnapStore_Download`: downloads object from S3
      - `TestS3SnapStore_List`: lists objects with prefix, returns sorted SnapInfo
      - `TestS3SnapStore_Delete`: deletes object
      - Compile-time interface assertion: `var _ SnapStore = (*S3SnapStore)(nil)`

- [ ] Task 3: GCS snapstore provider
      Files: internal/snapstore/gcs.go, internal/snapstore/gcs_test.go
      Tests: unit (mock GCS client)
      Deps: cloud.google.com/go/storage
      Acceptance criteria:
      - Same 5 test patterns as S3
      - Compile-time interface assertion

- [ ] Task 4: ABS snapstore provider
      Files: internal/snapstore/abs.go, internal/snapstore/abs_test.go
      Tests: unit (mock ABS client)
      Deps: github.com/Azure/azure-sdk-for-go/sdk/storage/azblob
      Acceptance criteria:
      - Same 5 test patterns as S3
      - Compile-time interface assertion

- [ ] Task 5: Wire factory into daemon + compact + copy-backups
      Files: cmd/etcdsteward/main.go, cmd/etcdsteward/compact/compact.go, cmd/etcdsteward/copybackups/copybackups.go, internal/config/config.go
      Tests: unit
      Acceptance criteria:
      - Config has StoreProvider, StorePrefix, StoreContainer fields
      - Daemon creates snapstore via factory based on config
      - Compact subcommand uses factory
      - Copy-backups supports cross-provider copy (e.g., Local→S3)

---

## Session 3: Multi-Node Coordination (Issues #3, #4, #5, #14)

### Design
Implement learner-join, learner-promote, member-remove, and peer TLS coordination. This is the most complex session — it touches bootstrapper, statemachine, and defrag.

### Tasks

- [ ] Task 1: Learner join flow in bootstrapper
      Files: internal/bootstrapper/bootstrapper.go, internal/bootstrapper/bootstrapper_test.go
      Tests: unit
      Acceptance criteria:
      - `TestBootstrapper_MultiNode_LearnerJoin`: when isLearner=true and cluster exists, calls ClusterClient.MemberAddAsLearner with peer URLs
      - `TestBootstrapper_MultiNode_LearnerJoin_ClusterDown`: when cluster unreachable, retries with backoff
      - State machine transitions: Unknown→PendingLearner→Learner (recorded synchronously)

- [ ] Task 2: Learner promotion
      Files: internal/bootstrapper/bootstrapper.go, internal/bootstrapper/bootstrapper_test.go
      Tests: unit
      Acceptance criteria:
      - `TestBootstrapper_LearnerPromotion`: after learner catches up, calls ClusterClient.MemberPromote
      - State machine transition: Learner→Follower (recorded synchronously)
      - Promotion gated on member being in sync (revision check)

- [ ] Task 3: Member remove on corrupt data
      Files: internal/bootstrapper/bootstrapper.go
      Tests: unit
      Acceptance criteria:
      - `TestBootstrapper_CorruptData_RemovesMember`: on corrupt data, calls ClusterClient.MemberRemove before deleting data dir
      - Order verified: remove first, then delete (per design notes — prevents orphaned member)

- [ ] Task 4: Peer TLS coordination
      Files: internal/bootstrapper/bootstrapper.go, new internal/peertls/peertls.go
      Tests: unit
      Acceptance criteria:
      - `TestPeerTLS_DetectChange`: detects peer TLS config change in ConfigMap
      - `TestPeerTLS_UpdateMember`: calls etcd MemberUpdate with new peer URLs
      - No pod restart needed — steward handles TLS change dynamically

- [ ] Task 5: Defrag follower-first with real status keys
      Files: internal/defrag/defrag.go, internal/defrag/defrag_test.go
      Tests: unit
      Acceptance criteria:
      - `TestDefrag_StatusKeys_Written`: leader writes /steward/defrag/{member} keys
      - `TestDefrag_FollowersFirst`: followers complete defrag before leader starts
      - `TestDefrag_StatusKeys_Cleaned`: keys deleted after all members complete

---

## Session 4: Full Compaction Cycle + Cross-Provider Copy (Issues #12)

### Design
The compact subcommand must: download latest snapshot set → start temp embedded etcd → apply deltas → defrag → take new full snapshot → upload. Copy-backups must support cross-provider (e.g., S3→GCS).

### Tasks

- [ ] Task 1: Embedded etcd for compaction
      Files: internal/compactor/compactor.go, internal/compactor/compactor_test.go
      Tests: unit
      Acceptance criteria:
      - `TestCompactor_StartEmbeddedEtcd`: starts embedded etcd with restored data
      - `TestCompactor_ApplyDeltas`: applies delta snapshots to embedded etcd
      - `TestCompactor_DefragAndSnapshot`: defrags, takes new full snapshot

- [ ] Task 2: Full compact subcommand
      Files: cmd/etcdsteward/compact/compact.go
      Tests: unit
      Acceptance criteria:
      - `TestCompact_EndToEnd`: full cycle with local snapstore — input 1 full + 3 deltas → output 1 new compacted full

- [ ] Task 3: Cross-provider copy-backups
      Files: cmd/etcdsteward/copybackups/copybackups.go
      Tests: unit
      Acceptance criteria:
      - `TestCopyBackups_LocalToLocal`: existing test
      - `TestCopyBackups_CrossProvider`: source=Local dest=mock-S3, all snapshots copied

---

## Session 5: EtcdMember CRD + Druid Integration (Issues #15, #16)

### Design
Define EtcdMember CRD types in etcd-druid, create the CRD, and wire etcd-steward to update EtcdMember status with real data (MemberID, ClusterID, DBSize, snapshots, transitions).

### Tasks

- [ ] Task 1: EtcdMember API types (in etcd-druid)
      Files: api/core/v1alpha1/etcdmember_types.go
      Tests: unit + CRD validation
      API generation: yes
      Acceptance criteria:
      - EtcdMember CRD with Status.Transitions, Status.Snapshots, Status.Conditions
      - `make generate` produces clean CRD YAML

- [ ] Task 2: EtcdMember component in druid
      Files: internal/component/etcdmember/
      Tests: unit
      Acceptance criteria:
      - Component creates EtcdMember CRs during Etcd reconciliation
      - Component deletes EtcdMember CRs on Etcd deletion

- [ ] Task 3: Wire steward to update real EtcdMember data
      Files: internal/member/updater.go (steward)
      Tests: unit
      Acceptance criteria:
      - MemberInfoProvider returns real MemberID, ClusterID, DBSize, DBSizeInUse
      - Snapshot info provider returns last full/delta snapshot revisions
      - Async update loop patches EtcdMember status every syncInterval

---

## Session 6: Hardening + TLS e2e + Data Validation (Issues #6, #10, #13)

### Tasks

- [ ] Task 1: Data validation (Issue #13)
      Files: internal/validator/validator.go, internal/validator/validator_test.go
      Tests: unit
      Acceptance criteria:
      - Sanity check: WAL + DB exist, DB opens without error
      - Full check: bbolt Verify + revision consistency
      - Single-node vs multi-node logic (per design notes)

- [ ] Task 2: Time-based + calendar-based GC (Issue #10)
      Files: internal/gc/gc.go, internal/gc/policy.go, internal/gc/gc_test.go
      Tests: unit
      Acceptance criteria:
      - Time-based: retains snapshot sets within configured duration
      - Calendar-based: hour/day/week/month granularity per design notes
      - Count-based: already working

- [ ] Task 3: TLS e2e tests
      Files: test/e2e/ (steward)
      Tests: e2e
      Acceptance criteria:
      - Client TLS enabled: steward connects to etcd via TLS
      - Peer TLS enabled: multi-node with mutual TLS
      - Backup-restore TLS: steward serves TLS on HTTP server

- [ ] Task 4: Alarm handler hardening (Issue #6)
      Files: internal/alarm/alarm.go
      Tests: unit
      Acceptance criteria:
      - NOSPACE: compact + defrag + disarm (already working)
      - Multiple consecutive alarms handled without crash
      - Alarm metrics published

- [ ] Task 5: Error handling audit
      Files: all internal packages
      Tests: none (code review)
      Acceptance criteria:
      - All errors use internal/errors package with appropriate codes
      - No panics in production paths
      - All goroutines have proper context cancellation

## PR Checklist (per session)
- [ ] make build passes
- [ ] make test-unit passes with -race
- [ ] make test-e2e passes (steward e2e)
- [ ] druid e2e passes with UseEtcdSteward=true (for sessions touching druid)

## Rollback
Each session is a separate commit. Revert the session's commit to roll back.
