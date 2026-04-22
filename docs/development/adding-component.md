<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Adding a New Component

This guide walks through the steps to add a new operational component to etcd-steward. The project follows a consistent pattern: each component lives in its own package under `internal/`, exposes a `New()` constructor, and integrates with the daemon wiring in `cmd/etcdsteward/main.go`.

## Step 1: Create the Package

Create a new directory under `internal/`:

```
internal/mycomponent/
├── mycomponent.go       # Core logic
└── mycomponent_test.go  # Unit tests
```

Add the SPDX header to every `.go` file:

```go
// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0
```

## Step 2: Define the Component

Follow the constructor pattern used by all existing components. Accept dependencies as interfaces and configuration as explicit parameters:

```go
package mycomponent

import (
    "context"
    "time"

    "go.uber.org/zap"
)

// SomeDependency abstracts an external system so it can be mocked in tests.
type SomeDependency interface {
    DoWork(ctx context.Context) error
}

// MyComponent does something useful.
type MyComponent struct {
    interval time.Duration
    dep      SomeDependency
    logger   *zap.Logger
}

// New creates a MyComponent with the given dependencies.
func New(interval time.Duration, dep SomeDependency, logger *zap.Logger) *MyComponent {
    return &MyComponent{
        interval: interval,
        dep:      dep,
        logger:   logger,
    }
}
```

Key conventions:

- **Small interfaces**: Define only the methods your component needs, not the full client surface.
- **`*zap.Logger`**: All components accept a zap logger for structured logging.
- **No global state**: Dependencies are passed via the constructor.

## Step 3: Implement the Run Loop

Components that operate continuously implement a `Run(ctx context.Context)` method that blocks until the context is cancelled:

```go
// Run starts the component's main loop. It blocks until ctx is cancelled.
func (c *MyComponent) Run(ctx context.Context) {
    ticker := time.NewTicker(c.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            c.logger.Info("mycomponent stopped")
            return
        case <-ticker.C:
            if err := c.dep.DoWork(ctx); err != nil {
                c.logger.Error("work failed", zap.Error(err))
            }
        }
    }
}
```

The pattern across the codebase is:

1. Create a `time.Ticker` with the component's interval.
2. `select` on `ctx.Done()` and the ticker channel.
3. On context cancellation, log and return.
4. On tick, execute the work and log errors.

## Step 4: Register in the Daemon Wiring

Edit `cmd/etcdsteward/main.go` to wire and start your component in the root command's `RunE` function:

```go
import "github.com/gardener/etcd-steward/internal/mycomponent"

// Inside the root RunE function, after config loading:
if cfg.EnableMyComponent {
    comp := mycomponent.New(cfg.MyComponentInterval, someDep, logger)
    go comp.Run(ctx)
}
```

Components are started as goroutines. The parent context is cancelled on shutdown, which propagates to all components.

## Step 5: Add the Enable Flag

Add a boolean enable flag and any interval/timeout flags to the config:

In `internal/config/config.go`, add the field to the `Config` struct:

```go
// EnableMyComponent enables the new component.
EnableMyComponent bool
// MyComponentInterval is the interval between runs.
MyComponentInterval time.Duration
```

Set a default in `DefaultConfig()`:

```go
EnableMyComponent:    false,
MyComponentInterval:  5 * time.Minute,
```

In `internal/config/flags.go`, bind the flag:

```go
fs.BoolVar(&cfg.EnableMyComponent, "enable-mycomponent", cfg.EnableMyComponent,
    "enable the new component")
fs.DurationVar(&cfg.MyComponentInterval, "mycomponent-interval", cfg.MyComponentInterval,
    "interval between mycomponent runs")
```

And add the corresponding `applyXxxIfUnset` calls in `LoadFromFile` so config file values are respected.

## Step 6: Add Metrics (Optional)

If the component needs Prometheus metrics, register it in `internal/metrics/metrics.go`:

1. Add a new `Component` constant:

```go
const MyComponent Component = "mycomponent"
```

2. Add component-specific metrics in `registerComponentSpecific`:

```go
case MyComponent:
    m.MyCustomGauge = prometheus.NewGauge(prometheus.GaugeOpts{
        Namespace: ns, Subsystem: sub, Name: "some_gauge",
        Help: "Description of the gauge.",
    })
    registerer.MustRegister(m.MyCustomGauge)
```

3. Use `metrics.RecordOperation` in the component for standard operation tracking.

## Step 7: Write Unit Tests

Create `internal/mycomponent/mycomponent_test.go` with mock implementations of all interfaces:

```go
package mycomponent

import (
    "context"
    "testing"
    "time"

    "go.uber.org/zap"
)

type mockDep struct {
    err    error
    called int
}

func (m *mockDep) DoWork(_ context.Context) error {
    m.called++
    return m.err
}

func TestNew(t *testing.T) {
    dep := &mockDep{}
    c := New(time.Minute, dep, zap.NewNop())
    if c == nil {
        t.Fatal("expected non-nil component")
    }
}

func TestMyComponent_DoesWork(t *testing.T) {
    dep := &mockDep{}
    c := New(time.Minute, dep, zap.NewNop())

    // Test the core logic directly (not the Run loop)
    // ...
}
```

Conventions:

- Use `zap.NewNop()` for the logger.
- Test the core business logic methods directly, not the `Run` loop.
- Use `t.TempDir()` for any filesystem needs.
- Name tests `TestFunctionName_Scenario`.

## Component Lifecycle Summary

1. **Initialization**: The daemon creates the component after config is loaded and dependencies are resolved.
2. **Start**: The component's `Run` method is launched in a goroutine. It begins work after any initial setup (e.g. acquiring a lock, taking an initial snapshot).
3. **Steady state**: The ticker fires at the configured interval. Each tick executes the component's work.
4. **Shutdown**: The parent context is cancelled. The component detects `ctx.Done()`, cleans up resources, and returns.

## Checklist

- [ ] Package created under `internal/` with SPDX headers
- [ ] Constructor follows `New(...)` pattern with dependency injection
- [ ] `Run(ctx context.Context)` method blocks until context cancellation
- [ ] Enable flag added to `Config`, `DefaultConfig()`, `BindFlags()`, and `LoadFromFile()`
- [ ] Wired in `cmd/etcdsteward/main.go` behind the enable flag
- [ ] Unit tests with mock interfaces in `_test.go`
- [ ] Metrics registered (if applicable)
