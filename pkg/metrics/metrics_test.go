// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gardener/etcd-steward/pkg/metrics"
)

func TestComponentHealth_SetAndCollect(t *testing.T) {
	metrics.ComponentHealth.WithLabelValues("default", "etcd-main-0", "snapshotter").Set(1)

	gathered, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range gathered {
		if mf.GetName() == "etcd_steward_component_health" {
			found = true
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "component" && lp.GetValue() == "snapshotter" {
						if m.GetGauge().GetValue() != 1 {
							t.Errorf("expected gauge=1, got %v", m.GetGauge().GetValue())
						}
					}
				}
			}
		}
	}
	if !found {
		t.Error("etcd_steward_component_health metric not found in registry")
	}
}

func TestStateTransitionsTotal_Inc(t *testing.T) {
	metrics.StateTransitionsTotal.WithLabelValues("default", "etcd-main-0", "Started", "", "JoinedAsLearner").Inc()

	gathered, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range gathered {
		if mf.GetName() == "etcd_steward_state_transitions_total" {
			found = true
			break
		}
	}
	if !found {
		t.Error("etcd_steward_state_transitions_total metric not found in registry")
	}
}

func TestInitializationDurationSeconds_Observe(_ *testing.T) {
	// Observe must not panic.
	metrics.InitializationDurationSeconds.WithLabelValues("default", "etcd-main-0", "NewSingleNode").Observe(1.5)
}

func TestSnapshotDurationSeconds_Observe(t *testing.T) {
	metrics.SnapshotDurationSeconds.WithLabelValues("full").Observe(10.0)
	metrics.SnapshotDurationSeconds.WithLabelValues("delta").Observe(0.5)

	gathered, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range gathered {
		if mf.GetName() == "etcd_steward_snapshot_duration_seconds" {
			if len(mf.GetMetric()) < 2 {
				t.Errorf("expected at least 2 histogram series (full+delta), got %d", len(mf.GetMetric()))
			}
		}
	}
}

func TestRestorationDurationSeconds_Observe(t *testing.T) {
	metrics.RestorationDurationSeconds.WithLabelValues("default", "etcd-main-0").Observe(30.0)
	gathered, _ := prometheus.DefaultGatherer.Gather()
	found := false
	for _, mf := range gathered {
		if mf.GetName() == "etcd_steward_restoration_duration_seconds" {
			found = true
		}
	}
	if !found {
		t.Error("etcd_steward_restoration_duration_seconds not found")
	}
}

func TestDefragmentationDurationSeconds_Observe(t *testing.T) {
	metrics.DefragmentationDurationSeconds.WithLabelValues("default", "etcd-main-0", "success", "NSPACEAlarm").Observe(5.0)
	gathered, _ := prometheus.DefaultGatherer.Gather()
	found := false
	for _, mf := range gathered {
		if mf.GetName() == "etcd_steward_defragmentation_duration_seconds" {
			found = true
		}
	}
	if !found {
		t.Error("etcd_steward_defragmentation_duration_seconds not found")
	}
}
