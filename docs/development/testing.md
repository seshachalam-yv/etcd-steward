<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Testing Guide

## Test categories

etcd-steward uses three levels of testing:

| Level | Location | Framework | Requirement |
|-------|----------|-----------|-------------|
| Unit | `pkg/<component>/*_test.go` | Go `testing` | No external services |
| Integration | `pkg/integration/*_test.go` | Go `testing` | No external services (in-process) |
| e2e | etcd-druid fork `test/e2e/` | Go `testing` | KIND cluster |

**No Ginkgo.** All tests use standard `testing.T` and table-driven patterns.

---

## Running tests

### All unit + integration tests

```bash
make test
# equivalent to: go test ./... -count=1
```

### Single package

```bash
go test ./pkg/alarm/... -v -count=1
```

### With coverage

```bash
go test ./pkg/... -cover -count=1
```

### Lint

```bash
make check
# runs golangci-lint v2 with .golangci.yaml config
```

---

## Unit test conventions

### No real etcd or Kubernetes

Every component depends only on narrow interfaces. Tests provide fake implementations:

```go
// Example: testing alarm.Handler without a real etcd
type fakeMaintenance struct {
    alarms       []*pb.AlarmMember
    statusRev    int64
    defragCalled bool
    disarmed     []*clientv3.AlarmMember
}

func (f *fakeMaintenance) AlarmList(_ context.Context) (*clientv3.AlarmResponse, error) { ... }
func (f *fakeMaintenance) Defragment(_ context.Context, _ string) (*clientv3.DefragmentResponse, error) { ... }
// ...

h := alarm.New(fakeMaintenance, fakeKV, "http://localhost:2379", ...)
err := h.check(context.Background())
```

### Table-driven tests

Use table-driven tests for any function with multiple input/output combinations:

```go
func TestDetermineMode(t *testing.T) {
    cases := []struct {
        name    string
        setup   func(dir string)
        want    validator.ValidationMode
    }{
        {"no exit marker", func(dir string) {}, validator.Full},
        {"clean exit marker", func(dir string) { writeCleanMarker(dir) }, validator.Sanity},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            // ...
        })
    }
}
```

### No `time.Sleep`

Use channels or condition loops instead:

```go
// Bad
time.Sleep(100 * time.Millisecond)
if !m.defragCalled { t.Error(...) }

// Good
deadline := time.After(2 * time.Second)
for {
    select {
    case <-deadline:
        t.Fatal("timeout")
    default:
        if m.defragCalled { return }
        time.Sleep(time.Millisecond)
    }
}
```

---

## Integration tests

`pkg/integration` tests the full HTTP lifecycle without external services. Tests wire the real `server.Server` + `initializer.Initializer` together using a real bbolt database on a temp directory.

```bash
go test ./pkg/integration/... -v -count=1 -timeout 60s
```

Key test functions:

| Function | What it tests |
|----------|---------------|
| `TestHTTPLifecycle_PathC` | Full lifecycle for fresh single-node: status, start, config, metrics, idempotency |
| `TestHTTPLifecycle_PathA` | Path A: existing valid DB → `Successful` immediately |
| `TestServerReachableBeforeInit` | `/healthz` responds before `Start()` is called |

---

## Coverage targets

| Package | Current | Target |
|---------|---------|--------|
| alarm | 97.8% | ≥ 90% |
| config | 100% | ≥ 90% |
| gc | 97.8% | ≥ 90% |
| lock | 90.9% | ≥ 90% |
| member | 92.3% | ≥ 90% |
| server | 84.7% | ≥ 80% |
| restoration | 84.5% | ≥ 80% |
| validator | 87.1% | ≥ 80% |
| statemachine | 81.6% | ≥ 80% |
| initializer | 80.7% | ≥ 80% |
| leaderwatch | 82.5% | ≥ 80% |
| compression | 80.8% | ≥ 80% |

---

## Adding tests for a new component

1. Create `pkg/<component>/<component>_test.go` in the same package (`package <component>`).
2. Define fake implementations for any interfaces the component depends on.
3. Write a test for each exported function/method, covering:
   - Happy path
   - Each error path (what happens if a dependency returns an error)
   - Context cancellation
4. For components with a `Run(ctx)` loop, test both:
   - That `Run` exits when the context is cancelled
   - That `Run` invokes the expected behaviour on each tick
5. Run `go test ./pkg/<component>/... -cover` and verify ≥ 80% coverage.

---

## e2e tests

e2e tests run against a real KIND cluster managed by etcd-druid.

### Prerequisites

- `kind` and `kubectl` installed
- `KUBECONFIG` set to the KIND cluster
- etcd-steward image built and pushed to a registry accessible by the cluster

### Running

```bash
cd /path/to/etcd-druid-fork
make test-e2e PROVIDERS=local
```

### etcd-steward-specific e2e tests

The standard etcd-druid e2e suite does not yet include `UseEtcdSteward`-specific test scenarios. The following can be verified manually:

| Scenario | How to verify |
|----------|---------------|
| Single-node no-backup bootstrap | Deploy `Etcd` with 1 replica, no backup store; verify pod reaches `Running` state |
| Multi-node bootstrap | Deploy `Etcd` with 3 replicas; verify all pods reach `Running` |
| Initialization status | `curl http://<pod-ip>:8080/initialization/status` returns `Successful` |
| EtcdMember CRs created | `kubectl get etcdmembers -n <namespace>` shows one CR per member |
| Graceful shutdown | `kubectl delete pod <pod>` — steward writes exit marker; next start uses Sanity validation |
