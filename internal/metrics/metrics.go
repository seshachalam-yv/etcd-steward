// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Component identifies an etcd-steward subsystem that registers its own metrics.
type Component string

const (
	// Snapshotter is the snapshot component.
	Snapshotter Component = "snapshotter"
	// GC is the garbage collection component.
	GC Component = "gc"
	// Defrag is the defragmentation component.
	Defrag Component = "defrag"
	// Alarm is the alarm management component.
	Alarm Component = "alarm"
	// Restorer is the restore component.
	Restorer Component = "restorer"
	// Bootstrap is the bootstrap component.
	Bootstrap Component = "bootstrap"
	// Member is the member management component.
	Member Component = "member"
	// Lease is the lease management component.
	Lease Component = "lease"
)

// Metrics holds the Prometheus metrics for a single component.
type Metrics struct {
	// Common metrics (available for every component).
	OperationDuration *prometheus.HistogramVec
	OperationErrors   *prometheus.CounterVec
	OperationTotal    *prometheus.CounterVec

	// Snapshotter-specific metrics.
	SnapshotRevision   prometheus.Gauge
	SnapshotDuration   prometheus.Histogram
	SnapshotSize       prometheus.Histogram
	DeltaSnapshotCount prometheus.Counter

	// Defrag-specific metrics.
	DefragDuration    prometheus.Histogram
	DefragAttempts    prometheus.Counter
	DefragDBSizeBefore prometheus.Gauge
	DefragDBSizeAfter  prometheus.Gauge

	// GC-specific metrics.
	GCDeletedSnapshots prometheus.Counter
	GCRunDuration      prometheus.Histogram

	// Alarm-specific metrics.
	AlarmActive prometheus.Gauge
}

var (
	mu        sync.Mutex
	registry  = make(map[Component]*Metrics)
)

// RegisterComponent registers the standard and component-specific metrics for
// the given component using the default prometheus registerer.
func RegisterComponent(comp Component) *Metrics {
	return RegisterComponentWith(comp, prometheus.DefaultRegisterer)
}

// RegisterComponentWith registers the standard and component-specific metrics
// for the given component with the supplied registerer. This is useful for test
// isolation where each test can use its own prometheus.Registry.
func RegisterComponentWith(comp Component, registerer prometheus.Registerer) *Metrics {
	mu.Lock()
	defer mu.Unlock()

	if m, ok := registry[comp]; ok {
		return m
	}

	ns := "etcd_steward"
	sub := string(comp)

	m := &Metrics{
		OperationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "operation_duration_seconds",
			Help:      "Duration of operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"operation"}),
		OperationErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "operation_errors_total",
			Help:      "Total number of failed operations.",
		}, []string{"operation"}),
		OperationTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "operation_total",
			Help:      "Total number of operations.",
		}, []string{"operation"}),
	}

	registerer.MustRegister(m.OperationDuration, m.OperationErrors, m.OperationTotal)

	registerComponentSpecific(comp, m, ns, sub, registerer)

	registry[comp] = m
	return m
}

func registerComponentSpecific(comp Component, m *Metrics, ns, sub string, registerer prometheus.Registerer) {
	switch comp {
	case Snapshotter:
		m.SnapshotRevision = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "snapshot_revision",
			Help: "The latest snapshot revision.",
		})
		m.SnapshotDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "snapshot_duration_seconds",
			Help: "Duration of snapshot operations.", Buckets: prometheus.DefBuckets,
		})
		m.SnapshotSize = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "snapshot_size_bytes",
			Help:    "Size of snapshot in bytes.",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
		})
		m.DeltaSnapshotCount = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "delta_snapshot_total",
			Help: "Total number of delta snapshots taken.",
		})
		registerer.MustRegister(m.SnapshotRevision, m.SnapshotDuration, m.SnapshotSize, m.DeltaSnapshotCount)

	case Defrag:
		m.DefragDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "defrag_duration_seconds",
			Help: "Duration of defragmentation.", Buckets: prometheus.DefBuckets,
		})
		m.DefragAttempts = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "defrag_attempts_total",
			Help: "Total defragmentation attempts.",
		})
		m.DefragDBSizeBefore = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "defrag_db_size_before_bytes",
			Help: "DB size before defragmentation.",
		})
		m.DefragDBSizeAfter = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "defrag_db_size_after_bytes",
			Help: "DB size after defragmentation.",
		})
		registerer.MustRegister(m.DefragDuration, m.DefragAttempts, m.DefragDBSizeBefore, m.DefragDBSizeAfter)

	case GC:
		m.GCDeletedSnapshots = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "gc_deleted_snapshots_total",
			Help: "Total snapshots deleted by GC.",
		})
		m.GCRunDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "gc_run_duration_seconds",
			Help: "Duration of GC runs.", Buckets: prometheus.DefBuckets,
		})
		registerer.MustRegister(m.GCDeletedSnapshots, m.GCRunDuration)

	case Alarm:
		m.AlarmActive = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "alarm_active",
			Help: "Whether an alarm is currently active (1=active, 0=inactive).",
		})
		registerer.MustRegister(m.AlarmActive)
	}
}

// RecordOperation is a safe helper that records a completed operation. If m is
// nil the call is a no-op, which allows callers to use metrics before
// registration.
func RecordOperation(m *Metrics, operation string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.OperationTotal.WithLabelValues(operation).Inc()
	m.OperationDuration.WithLabelValues(operation).Observe(duration.Seconds())
	if err != nil {
		m.OperationErrors.WithLabelValues(operation).Inc()
	}
}

// ResetForTesting clears the internal registry so that tests start with a
// clean state. It does NOT unregister metrics from prometheus.DefaultRegisterer;
// use RegisterComponentWith with a per-test registry for full isolation.
func ResetForTesting() {
	mu.Lock()
	defer mu.Unlock()
	registry = make(map[Component]*Metrics)
}
