> **Gate 1 pre-approved. Autonomous execution. Do not push code or update upstream issues.**
> Use parallel agents where possible. Launch monitoring agent for e2e tests.

## Issue
Summary: Redesign etcd-steward state machine to match DEP-04, fix AllMembersReady, pass all druid e2e tests.

## Repos
- **Steward**: /Users/I568019/go/src/github.com/gardener/etcd-steward/.worktrees/etcd-steward-v2
- **Druid**: /Users/I568019/go/src/github.com/gardener/etcd-druid (main repo with worktree changes copied)

## Problem Statement
1. Steward's state machine uses flat states (Unknown/New/PendingLearner/Learner/Follower/Leader) instead of DEP-04's hierarchical states (New → Initializing → Starting → Started with sub-states)
2. Member lease HolderIdentity format doesn't match what druid expects → AllMembersReady=False
3. EtcdMember status fields (state, subState, snapshots, lastDefrag, lastRestore) not populated

## DEP-04 State Machine (exact)

```
States:        New, Initializing, Starting, Started
SubStates:
  Initializing: DBValidationSanity, DBValidationFull, Restoration
  Starting:     PendingLearner, Learner
  Started:      Follower, Leader

Transition table:
  nil                        → New                              (ClusterScaledUp | NewSingleNodeClusterCreated)
  New                        → Initializing/DBValidationSanity  (DetectedPreviousCleanExit)
  New                        → Initializing/DBValidationFull    (DetectedPreviousUncleanExit)
  Initializing/DBV-S|DBV-F  → Initializing/Restoration         (DBValidationFailed, single-node)
  Initializing/DBV-S|DBV-F  → New                              (DBValidationFailed, multi-node → remove+clean)
  Initializing/DBV-S|DBV-F  → Started/Leader|Follower          (DBValidationSucceeded)
  Initializing/Restoration   → Started/Leader                   (RestorationSucceeded)
  New                        → Starting/PendingLearner          (WaitingToJoinAsLearner)
  Starting/PendingLearner    → Starting/Learner                 (JoinedAsLearner)
  Starting/Learner           → Started/Follower                 (PromotedAsVotingMember)
  Started/Follower           → Started/Leader                   (GainedClusterLeadership)
  Started/Leader             → Started/Follower                 (LostClusterLeadership)
```

## Tasks

### Wave 1: Steward state machine + member lease (parallel-safe: different packages)

- [ ] **Task 1: Rewrite internal/statemachine/ to match DEP-04**
      Files: internal/statemachine/types.go, statemachine.go, statemachine_test.go
      Acceptance:
      - States: New, Initializing, Starting, Started (4 top-level)
      - SubStates: DBValidationSanity, DBValidationFull, Restoration, PendingLearner, Learner, Follower, Leader (7)
      - Transition struct: State, SubState, Reason, TransitionTime, Message
      - All 11 reason codes from proposal defined as constants
      - All transition rules from the table above implemented
      - Tests: single-node bootstrap (New→Initializing/DBV-F→Started/Leader), scale-up (New→Starting/PL→Starting/Learner→Started/Follower), restart with corruption (Started/Follower→New→Initializing/DBV-F→Initializing/Restoration→Started/Leader)

- [ ] **Task 2: Fix member lease HolderIdentity format**
      Files: internal/lease/lease.go
      Acceptance:
      - HolderIdentity format: `<memberID>:<role>` (e.g. `128088275939295631:Leader`)
      - This matches what druid's etcdmember readyCheck expects
      - Test: verify HolderIdentity format

### Wave 2: Wire state machine into daemon + populate EtcdMember status (parallel-safe: steward vs druid)

- [ ] **Task 3: Wire DEP-04 transitions into daemon runDaemon()**
      Files: cmd/etcdsteward/main.go
      Acceptance:
      - Daemon records transitions at each step:
        1. Pod starts → New (NewSingleNodeClusterCreated)
        2. Validation starts → Initializing/DBValidationFull (DetectedPreviousUncleanExit)
        3. If valid → Started/Leader or Started/Follower (DBValidationSucceeded)
        4. If corrupt + single-node → Initializing/Restoration → Started/Leader
        5. If corrupt + multi-node → New (member removed)
        6. After etcd ready + leader watch → Started/Leader or Started/Follower
      - Transitions written to EtcdMember.Status.Transitions via K8s patch

- [ ] **Task 4: Populate EtcdMember.Status fields from steward**
      Files: internal/member/updater.go, cmd/etcdsteward/main.go
      Acceptance:
      - EtcdMember.Status.ID = memberID (from etcd Status)
      - EtcdMember.Status.ClusterID = clusterID
      - EtcdMember.Status.State = current top-level state
      - EtcdMember.Status.SubState = current sub-state
      - EtcdMember.Status.DBSize = dbSize
      - EtcdMember.Status.DBSizeInUse = dbSizeInUse
      - EtcdMember.Status.Snapshots = last full/delta snapshot info (from SnapshotInfoProvider)

- [ ] **Task 5: Fix druid status reconciler to use EtcdMember state**
      Files (druid): internal/controller/etcd/reconcile_status.go, internal/health/status/check.go
      Acceptance:
      - When UseEtcdSteward enabled: read EtcdMember.Status.State + SubState
      - Map to member readiness: Started/Leader or Started/Follower → Ready
      - AllMembersReady=True when all EtcdMember resources have State=Started

### Wave 3: Build, deploy, e2e test with monitoring

- [ ] **Task 6: Rebuild steward image + redeploy druid + run e2e**
      - Rebuild steward from worktree
      - Copy druid changes to main repo
      - Deploy druid with UseEtcdSteward=true
      - Run TestBasic/basic-no-tls-1-none first
      - Launch monitoring agent to check pod logs immediately
      - If passes, run full TestBasic suite
      - If fails, monitoring agent diagnoses from logs

## Execution Strategy

- Wave 1: Launch Task 1 + Task 2 in parallel (different packages)
- Wave 2: Launch Task 3 + Task 5 in parallel (different repos: steward vs druid)
  - Task 4 sequential after Task 3 (same file)
- Wave 3: Build → deploy → test with monitoring agent

## Rollback
Revert commits on respective branches.
