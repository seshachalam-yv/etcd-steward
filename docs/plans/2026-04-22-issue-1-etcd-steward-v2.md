> **For agentic workers:** Gate 1 is pre-approved. Proceed directly to implementation.
> Use parallel wave execution — launch all tasks within a wave concurrently.
> Apply ALL 12 learnings from the previous session (documented below).

## Issue
Link: https://github.com/gardener/etcd-steward/issues/1
Summary: Re-write etcd-backup-restore as etcd-steward with modular, toggleable components and introduce UseEtcdSteward feature gate in etcd-druid.

## Repositories
- **etcd-steward fork:** /Users/I568019/go/src/github.com/seshachalam-yv/etcd-steward (branch: feat/issue-1/etcd-steward-v2)
- **etcd-druid worktree:** /Users/I568019/go/src/github.com/gardener/etcd-druid/.worktrees/etcd-steward-v2 (branch: feat/etcd-steward-v2)

## Change Type
[x] New binary — etcd-steward sidecar
[x] Feature gate — UseEtcdSteward in etcd-druid

## Critical Learnings (MUST apply)

| # | Learning | Where to apply |
|---|----------|---------------|
| L1 | Go nil interface: `var store SnapStore; localStore, err := NewLocal(); if err == nil { store = localStore }` | daemon wiring (main.go) |
| L2 | Wrapper contract: register `/initialization/status`, `/initialization/start`, `/config` on HTTP server | server package + daemon wiring |
| L3 | Readiness probe: when UseEtcdSteward, change to `/healthz` on backupPort | druid builder.go |
| L4 | Pod identity: `--pod-name=$(POD_NAME) --pod-namespace=$(POD_NAMESPACE)` using K8s env substitution | druid builder.go steward args |
| L5 | Distroless: e2e tests use etcdctl, port-forward, K8s API — no shell commands | e2e tests |
| L6 | Feature gate files: features.go, constants.go, image.go, images.yaml, builder.go, test utils | druid feature gate task |
| L7 | Dockerfile.e2e: build binary locally, COPY into distroless | steward Dockerfile.e2e |
| L8 | Per-test namespaces: `e2e-` + sanitized t.Name(), max 63 chars, no underscores | e2e helpers |
| L9 | etcdctl --count-only needs --write-out=fields | e2e tests |
| L10 | etcd peer URLs: use 0.0.0.0 not localhost | e2e pod specs |
| L11 | Naming: all file names lowercase ASCII, no `-` or `_` | all steward files |
| L12 | No Ginkgo: Go native testing.T only in etcd-steward | all steward tests |

## Design Summary

### etcd-steward architecture
- **cobra CLI**: daemon (default), `compact`, `copy-backups`
- **17 internal packages**: errors, config, server, etcdclient, statemachine, validator, snapstore, compression, snapshotter, gc, restorer, compactor, defrag, alarm, bootstrapper, member, lease, leaderwatch, metrics
- **zap** structured JSON logger
- **pflag + viper**: config file + CLI flags, flags override file
- **Custom errors**: sentinel codes + structured Error{code,subCode,cause,message,operation}
- **Wrapper compatibility**: /initialization/status + /initialization/start + /config (L2)

### Design decisions from notes
- **Defrag Option #3**: leader-coordinated via etcd keys + lease, leader defrags last
- **Init Option #1**: steward validates data dir + downloads snapshots; wrapper starts embedded etcd
- **Leadership Option #1**: watch `/steward/leader` key in etcd DB
- **EtcdMember updates**: sync mandatory transitions until voting-member, then async via registered InfoProviders
- **Snapshotter**: etcd lease for mutual exclusion across leadership changes
- **GC**: snapshot-set based (full + subsequent deltas), count retention, never deletes latest set

### UseEtcdSteward feature gate (etcd-druid)
- Alpha, disabled by default, not locked
- When enabled: image selection returns `etcd-steward` key; container args use steward CLI flags; readiness probe → `/healthz` on backupPort (L3); pod identity via `$(POD_NAME)` (L4)
- Works with UpgradeEtcdVersion (both combinations)

---

## Tasks

### Wave 1: Steward scaffold (no deps)

- [ ] **Task 1: Project scaffold — go.mod, cobra CLI, Makefile, Dockerfile**
      **depends-on:** —
      Files: go.mod, cmd/etcdsteward/main.go, cmd/etcdsteward/compact/compact.go, cmd/etcdsteward/copybackups/copybackups.go, Makefile, Dockerfile, Dockerfile.e2e (L7), hack/build.sh, .golangci.yaml, internal/doc.go, hack/tools/tools.go
      Tests: build only
      API generation: no
      #### Requirement: Binary builds and CLI works
      - WHEN `make build` is run THEN `bin/etcd-steward --help` lists compact and copy-backups subcommands

- [ ] **Task 2: Custom error types**
      **depends-on:** Task 1
      Files: internal/errors/errors.go, internal/errors/errors_test.go
      Tests: unit (8 tests)
      #### Requirement: Error wrapping chain preserved
      - WHEN `errors.Wrap(sentinel, cause, "op", "msg")` THEN `errors.Is(wrapped, cause)` returns true

### Wave 2: Core infrastructure (deps: T1-2)

- [ ] **Task 3: Config — struct, flags, file binding, validation**
      **depends-on:** T1, T2
      Files: internal/config/config.go, internal/config/flags.go, internal/config/validate.go, internal/config/config_test.go
      Tests: unit (7 tests)

- [ ] **Task 4: HTTP server + /metrics + /healthz + wrapper compat endpoints (L2)**
      **depends-on:** T1
      Files: internal/server/server.go, internal/server/server_test.go
      Tests: unit (5+ tests)
      #### Requirement: Wrapper compatibility endpoints registered
      - WHEN server starts THEN GET /initialization/status returns 200 with JSON status
      - WHEN GET /healthz THEN returns 200 "ok"
      - WHEN GET /metrics THEN returns 200 with prometheus metrics

- [ ] **Task 5: etcdclient — KV, Maintenance, Cluster interfaces + distributed lock**
      **depends-on:** T1
      Files: internal/etcdclient/client.go, internal/etcdclient/lock.go, internal/etcdclient/client_test.go
      Tests: unit (4 tests)

### Wave 3: Components A (deps: T2-5)

- [ ] **Task 6: State machine — member transitions, K8s recorder**
      **depends-on:** T2, T5
      Files: internal/statemachine/types.go, internal/statemachine/statemachine.go, internal/statemachine/recorder.go, internal/statemachine/statemachine_test.go, internal/statemachine/recorder_test.go
      Tests: unit (10 tests)

- [ ] **Task 7: Snapstore — local provider**
      **depends-on:** T2
      Files: internal/snapstore/types.go, internal/snapstore/local.go, internal/snapstore/snapstore_test.go
      Tests: unit (7 tests)

- [ ] **Task 8: Compression + Task 9: Leader watch**
      **depends-on:** T5, T7
      Files: internal/compression/compression.go, internal/compression/compression_test.go, internal/leaderwatch/leaderwatch.go, internal/leaderwatch/leaderwatch_test.go
      Tests: unit (9 tests)
      #### Requirement: leaderwatch mock is race-free
      - WHEN mock value changes concurrently THEN no data race (mockKV uses sync.RWMutex + SetValue method)

- [ ] **Task 10: Metrics — per-component registration**
      **depends-on:** T4
      Files: internal/metrics/metrics.go, internal/metrics/metrics_test.go
      Tests: unit (11 tests)

### Wave 4: Components B (deps: T6-10)

- [ ] **Task 11: Snapshotter — full+delta with etcd lease**
      **depends-on:** T5, T7, T8, T10
      Files: internal/snapshotter/snapshotter.go, internal/snapshotter/snapshotter_test.go
      Tests: unit (9 tests)

- [ ] **Task 12: GC — count-based retention + Task 13: Restorer**
      **depends-on:** T7, T8
      Files: internal/gc/gc.go, internal/gc/gc_test.go, internal/restorer/restorer.go, internal/restorer/restorer_test.go
      Tests: unit (12 tests)

- [ ] **Task 14: Defrag + Task 15: Bootstrapper**
      **depends-on:** T5, T6, T7, T8
      Files: internal/defrag/defrag.go, internal/defrag/defrag_test.go, internal/bootstrapper/bootstrapper.go, internal/bootstrapper/wrapperapi.go, internal/bootstrapper/bootstrapper_test.go
      Tests: unit (17 tests)
      #### Requirement: bootstrapper handles nil restorer (L1)
      - WHEN `b.restorer == nil` THEN skips restore and starts as new member

- [ ] **Task 16: Member updater + lease renewal**
      **depends-on:** T5, T6, T10
      Files: internal/member/updater.go, internal/member/infoprovider.go, internal/member/updater_test.go, internal/lease/lease.go, internal/lease/lease_test.go
      Tests: unit (12 tests)

### Wave 5: Wiring + subcommands (deps: T3-16)

- [ ] **Task 17: Alarm handler + compact + copy-backups subcommands**
      **depends-on:** T5, T7, T8, T13
      Files: internal/alarm/alarm.go, internal/alarm/alarm_test.go, cmd/etcdsteward/compact/compact.go, cmd/etcdsteward/compact/compact_test.go, cmd/etcdsteward/copybackups/copybackups.go, cmd/etcdsteward/copybackups/copybackups_test.go
      Tests: unit (9 tests)

- [ ] **Task 18: Daemon wiring — full main.go implementation**
      **depends-on:** T3-T17
      Files: cmd/etcdsteward/main.go, cmd/etcdsteward/main_test.go
      Tests: unit (7 tests)
      #### Requirement: nil interface gotcha handled (L1)
      - WHEN NewLocal fails THEN `store` interface remains nil (use `localStore, err` pattern)
      #### Requirement: wrapper compat endpoints wired (L2)
      - WHEN steward starts THEN /initialization/status returns InitStatusNew, after bootstrap returns InitStatusSuccessful

### Wave 6: Steward e2e tests

- [ ] **Task 19: Steward e2e suite**
      **depends-on:** T18
      Files: test/e2e/helpers_test.go, test/e2e/steward_test.go, test/e2e/snapshotter_test.go, hack/kind-e2e.sh
      Tests: e2e (8 tests)
      #### Applies: L5 (distroless), L7 (Dockerfile.e2e), L8 (per-test ns), L9 (etcdctl fields), L10 (peer URLs)

### Wave 7: Feature gate in etcd-druid

- [ ] **Task 20: UseEtcdSteward feature gate**
      **depends-on:** T18 (steward image must exist)
      Files (in druid worktree):
      - api/config/v1alpha1/features.go — add UseEtcdSteward alpha gate
      - internal/common/constants.go — add ImageKeyEtcdSteward
      - internal/utils/image.go — return steward key when gate enabled
      - internal/images/images.yaml — add etcd-steward default entry
      - internal/component/statefulset/builder.go — getStewardContainerCommandArgs() + readiness probe change (L3, L4)
      - test/utils/constants.go — ETCDStewardImageTag
      - test/utils/imagevector.go — add etcd-steward to test vector
      - hack/e2e-imagevector-overwrite.yaml — override for e2e
      Tests: make test-unit, make test-integration
      #### Requirement: Feature gate file checklist complete (L6)
      - WHEN UseEtcdSteward=true THEN getEtcdImageKeys returns (etcd-wrapper, etcd-steward, alpine)
      #### Requirement: Steward CLI args include pod identity (L4)
      - WHEN container args generated THEN --pod-name=$(POD_NAME) and --pod-namespace=$(POD_NAMESPACE) present
      #### Requirement: Readiness probe updated (L3)
      - WHEN UseEtcdSteward=true THEN readiness probe is /healthz on backupPort

### Wave 8: Run druid e2e with steward

- [ ] **Task 21: Run druid e2e tests with UseEtcdSteward**
      **depends-on:** T19, T20
      Files: none (runtime verification)
      Tests: e2e (40+ tests: TestBasic, TestScaleOut, TestClusterUpdate, TestSnapshotCompaction, TestSecretFinalizers)
      #### Requirement: All existing e2e tests pass
      - WHEN `make ci-e2e-kind` with UseEtcdSteward=true THEN 0 failures

## PR Checklist (pre-submission)

### etcd-steward
- [ ] `make build` passes
- [ ] `make check` (golangci-lint) passes
- [ ] `make test-unit` passes with -race (127+ tests)
- [ ] `make test-e2e` passes (8+ tests in KIND)
- [ ] No Ginkgo imports (L12)
- [ ] All filenames lowercase ASCII, no `-`/`_` (L11)
- [ ] License headers on all .go files

### etcd-druid
- [ ] `make test-unit` passes
- [ ] `make test-integration` passes
- [ ] e2e: all 40+ tests pass with UseEtcdSteward=true
- [ ] Feature gate doesn't break default behavior (UseEtcdSteward=false)

## Rollback
- etcd-steward: delete branch `feat/issue-1/etcd-steward-v2` in fork
- etcd-druid: delete worktree `.worktrees/etcd-steward-v2` and branch `feat/etcd-steward-v2`
