// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package metrics registers Prometheus metrics for etcd-steward.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// ComponentHealth tracks the health status of etcd-steward components.
	ComponentHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "etcd_steward",
			Name:      "component_health",
			Help:      "Health status of etcd-steward components (1=healthy, 0=unhealthy).",
		},
		[]string{"namespace", "name", "component"},
	)

	// StateTransitionsTotal counts state transitions by state, sub_state, and reason.
	StateTransitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd_steward",
			Name:      "state_transitions_total",
			Help:      "Total number of etcd member state transitions recorded.",
		},
		[]string{"namespace", "name", "state", "sub_state", "reason"},
	)

	// InitializationDurationSeconds measures initialization duration by path.
	// path label is one of: "A" (restart with existing DB), "B" (scale-up learner),
	// "C" (fresh single-node), "D" (snapshot restore), or "unknown" if classification failed.
	InitializationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "etcd_steward",
			Name:      "initialization_duration_seconds",
			Help:      "Duration of etcd member initialization in seconds, by initialization path.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"namespace", "name", "path"},
	)

	// ValidationTotal counts DB validation operations by mode and result.
	// mode is "sanity" or "full"; result is "success" or "failure".
	ValidationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd_steward",
			Name:      "validation_total",
			Help:      "Total number of etcd DB validation operations, by mode and result.",
		},
		[]string{"namespace", "name", "mode", "result"},
	)

	// SnapshotDurationSeconds measures snapshot duration by kind.
	SnapshotDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "etcd_steward",
			Name:      "snapshot_duration_seconds",
			Help:      "Duration of snapshot operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"kind"},
	)

	// RestorationDurationSeconds measures the duration of etcd data restoration.
	RestorationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "etcd_steward",
			Name:      "restoration_duration_seconds",
			Help:      "Duration of etcd data restoration from snapshot in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"namespace", "name"},
	)

	// DefragmentationDurationSeconds measures the duration of defragmentation operations.
	// status_code is "success" or "failure". reason is "NSPACEAlarm", "Scheduled", or "Manual".
	DefragmentationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "etcd_steward",
			Name:      "defragmentation_duration_seconds",
			Help:      "Duration of etcd defragmentation operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"namespace", "name", "status_code", "reason"},
	)

	// GCSnapshotsDeletedTotal counts snapshots deleted during garbage collection.
	GCSnapshotsDeletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd_steward",
			Name:      "gc_snapshots_deleted_total",
			Help:      "Total number of snapshots deleted by the garbage collector.",
		},
		[]string{"namespace", "name", "kind", "policy"},
	)
)

func init() {
	prometheus.MustRegister(
		ComponentHealth,
		StateTransitionsTotal,
		InitializationDurationSeconds,
		ValidationTotal,
		SnapshotDurationSeconds,
		RestorationDurationSeconds,
		DefragmentationDurationSeconds,
		GCSnapshotsDeletedTotal,
	)
}
