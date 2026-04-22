<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Testing Guide

etcd-steward uses **Go native `testing.T` only**. Ginkgo is not used in this project. All tests follow standard Go conventions with table-driven patterns and interface-based mocking.

## Running Tests

### Unit Tests

```bash
go test -count=1 -race ./internal/...
```

Or via the Makefile:

```bash
make test-unit
```

The `-count=1` flag disables test caching so every run executes fresh. The `-race` flag enables the Go race detector.

### Integration Tests

```bash
go test -count=1 -race -tags=integration ./test/integration/...
```

### End-to-End Tests

```bash
go test -count=1 -race -tags=e2e ./test/e2e/...
```

Or via the Makefile:

```bash
make test-e2e
```

## Writing Unit Tests

### File and Function Naming

Test files are placed alongside the code they test in the same package:

```
internal/defrag/defrag.go        # production code
internal/defrag/defrag_test.go   # tests
```

Test function names follow the `TestFunctionName_Scenario` convention:

```go
func TestDefragment_FollowersFirstLeaderLast(t *testing.T) { ... }
func TestDefragment_NoEndpointsReturnsError(t *testing.T) { ... }
func TestSortEndpoints_LeaderLast(t *testing.T) { ... }
```

### Table-Driven Tests

Use table-driven tests when verifying multiple scenarios for the same function:

```go
func TestValidate_MissingFields(t *testing.T) {
    tests := []struct {
        name    string
        cfg     *Config
        wantErr bool
    }{
        {
            name:    "missing pod-name",
            cfg:     &Config{PodNamespace: "default", EtcdEndpoints: []string{"localhost:2379"}},
            wantErr: true,
        },
        {
            name:    "missing pod-namespace",
            cfg:     &Config{PodName: "etcd-0", EtcdEndpoints: []string{"localhost:2379"}},
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := Validate(tt.cfg)
            if (err != nil) != tt.wantErr {
                t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### Temporary Directories

Use `t.TempDir()` for any test that needs filesystem access. The directory is automatically cleaned up when the test finishes:

```go
func TestRestorer_WritesToDataDir(t *testing.T) {
    dataDir := t.TempDir()
    // ... use dataDir as the target for restore operations
}
```

### Mock Interfaces

Production code defines small interfaces (e.g. `DistributedLock`, `MaintenanceClient`, `RoleProvider`). Tests provide mock implementations in the test file:

```go
type mockLock struct {
    acquireErr error
    releaseErr error
    acquired   int
    released   int
}

func (m *mockLock) Acquire(_ context.Context) error {
    if m.acquireErr != nil {
        return m.acquireErr
    }
    m.acquired++
    return nil
}

func (m *mockLock) Release(_ context.Context) error {
    if m.releaseErr != nil {
        return m.releaseErr
    }
    m.released++
    return nil
}
```

Mocks are defined per-test-file, not shared across packages. This keeps each package self-contained and avoids mock coupling.

### Assertions

Use standard `t.Fatal`, `t.Fatalf`, `t.Error`, and `t.Errorf`:

```go
if err != nil {
    t.Fatalf("unexpected error: %v", err)
}
if got != want {
    t.Errorf("got %v, want %v", got, want)
}
```

### Logger in Tests

Use `zap.NewNop()` for a silent logger in unit tests:

```go
logger := zap.NewNop()
d := defrag.New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)
```

### Metrics in Tests

Use `metrics.RegisterComponentWith` with a per-test `prometheus.NewRegistry()` for isolation:

```go
reg := prometheus.NewRegistry()
m := metrics.RegisterComponentWith(metrics.Snapshotter, reg)
```

Call `metrics.ResetForTesting()` if you need to clear the global registry between tests.

## End-to-End Tests

### Build Tags

E2e tests carry the `//go:build e2e` tag so they are excluded from `go test ./...`:

```go
//go:build e2e

package e2e
```

### KIND Cluster

E2e tests assume a running KIND cluster with:

- etcd-steward deployed as a pod with an etcd sidecar
- The distroless container image loaded into KIND

### Distroless Image Caveats

The production image is based on `gcr.io/distroless/static-debian12:nonroot`. This means:

- No shell is available inside the steward container. Use the etcd container for `etcdctl` commands.
- Debugging requires `kubectl exec` into a different container in the same pod, or ephemeral debug containers.
- File paths inside the container are limited to what the binary itself creates.

### Test Helpers

Common e2e helpers live in `test/e2e/helpers_test.go`:

- `setupNamespace(t)` -- creates a unique test namespace
- `createEtcdStewardPod(t, ns, name, args)` -- deploys a steward pod
- `waitForPodReady(t, ns, name, timeout)` -- blocks until the pod is ready
- `waitForStewardHealthy(t, ns, name, timeout)` -- polls the `/healthz` endpoint
- `execInPod(t, ns, pod, container, cmd)` -- runs a command in a container
- `portForwardPod(t, ns, pod, port, path)` -- port-forwards and issues an HTTP GET
