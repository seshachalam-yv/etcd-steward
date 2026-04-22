// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func newTestRegistry() prometheus.Registerer {
	return prometheus.NewRegistry()
}

func TestRegisterComponent_Snapshotter(t *testing.T) {
	ResetForTesting()
	reg := newTestRegistry()
	m := RegisterComponentWith(Snapshotter, reg)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
	if m.OperationDuration == nil {
		t.Fatal("OperationDuration should not be nil")
	}
	if m.SnapshotRevision == nil {
		t.Fatal("SnapshotRevision should not be nil for Snapshotter")
	}
	if m.SnapshotDuration == nil {
		t.Fatal("SnapshotDuration should not be nil for Snapshotter")
	}
	if m.SnapshotSize == nil {
		t.Fatal("SnapshotSize should not be nil for Snapshotter")
	}
	if m.DeltaSnapshotCount == nil {
		t.Fatal("DeltaSnapshotCount should not be nil for Snapshotter")
	}
}

func TestRegisterComponent_Defrag(t *testing.T) {
	ResetForTesting()
	reg := newTestRegistry()
	m := RegisterComponentWith(Defrag, reg)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
	if m.DefragDuration == nil {
		t.Fatal("DefragDuration should not be nil for Defrag")
	}
	if m.DefragAttempts == nil {
		t.Fatal("DefragAttempts should not be nil for Defrag")
	}
	if m.DefragDBSizeBefore == nil {
		t.Fatal("DefragDBSizeBefore should not be nil for Defrag")
	}
	if m.DefragDBSizeAfter == nil {
		t.Fatal("DefragDBSizeAfter should not be nil for Defrag")
	}
}

func TestRegisterComponent_GC(t *testing.T) {
	ResetForTesting()
	reg := newTestRegistry()
	m := RegisterComponentWith(GC, reg)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
	if m.GCDeletedSnapshots == nil {
		t.Fatal("GCDeletedSnapshots should not be nil for GC")
	}
	if m.GCRunDuration == nil {
		t.Fatal("GCRunDuration should not be nil for GC")
	}
}

func TestRegisterComponent_Alarm(t *testing.T) {
	ResetForTesting()
	reg := newTestRegistry()
	m := RegisterComponentWith(Alarm, reg)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
	if m.AlarmActive == nil {
		t.Fatal("AlarmActive should not be nil for Alarm")
	}
}

func TestRegisterComponent_DoesNotPanic(t *testing.T) {
	ResetForTesting()
	reg := newTestRegistry()
	// Components without specific metrics should still register common ones.
	for _, comp := range []Component{Restorer, Bootstrap, Member, Lease} {
		m := RegisterComponentWith(comp, reg)
		if m == nil {
			t.Fatalf("expected non-nil Metrics for component %q", comp)
		}
		if m.OperationDuration == nil {
			t.Fatalf("OperationDuration should not be nil for %q", comp)
		}
		if m.OperationErrors == nil {
			t.Fatalf("OperationErrors should not be nil for %q", comp)
		}
		if m.OperationTotal == nil {
			t.Fatalf("OperationTotal should not be nil for %q", comp)
		}
	}
}

func TestRegisterComponent_Idempotent(t *testing.T) {
	ResetForTesting()
	reg := newTestRegistry()
	m1 := RegisterComponentWith(Snapshotter, reg)
	m2 := RegisterComponentWith(Snapshotter, reg)
	if m1 != m2 {
		t.Fatal("RegisterComponentWith should return the same Metrics on repeated calls")
	}
}

func TestUnregisteredMetric_IsNil(t *testing.T) {
	ResetForTesting()
	reg := newTestRegistry()
	// Register GC, then verify that Snapshotter-specific fields are nil.
	m := RegisterComponentWith(GC, reg)
	if m.SnapshotRevision != nil {
		t.Fatal("SnapshotRevision should be nil for GC component")
	}
	if m.DefragDuration != nil {
		t.Fatal("DefragDuration should be nil for GC component")
	}
	if m.AlarmActive != nil {
		t.Fatal("AlarmActive should be nil for GC component")
	}
}

func TestRecordOperation_NilMetricsNoPanic(t *testing.T) {
	// Must not panic when Metrics is nil.
	RecordOperation(nil, "test-op", time.Second, nil)
}

func TestRecordOperation_Success(t *testing.T) {
	ResetForTesting()
	reg := prometheus.NewRegistry()
	m := RegisterComponentWith(Snapshotter, reg)

	RecordOperation(m, "full-snapshot", 500*time.Millisecond, nil)

	// Gather and verify that metrics were recorded.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "etcd_steward_snapshotter_operation_total" {
			found = true
			if len(f.GetMetric()) == 0 {
				t.Fatal("expected at least one metric sample")
			}
			val := f.GetMetric()[0].GetCounter().GetValue()
			if val != 1 {
				t.Fatalf("expected operation_total=1, got %f", val)
			}
		}
	}
	if !found {
		t.Fatal("etcd_steward_snapshotter_operation_total metric not found")
	}
}

func TestRecordOperation_Error(t *testing.T) {
	ResetForTesting()
	reg := prometheus.NewRegistry()
	m := RegisterComponentWith(Defrag, reg)

	RecordOperation(m, "defrag", 2*time.Second, errForTest)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "etcd_steward_defrag_operation_errors_total" {
			found = true
			if len(f.GetMetric()) == 0 {
				t.Fatal("expected at least one metric sample")
			}
			val := f.GetMetric()[0].GetCounter().GetValue()
			if val != 1 {
				t.Fatalf("expected operation_errors_total=1, got %f", val)
			}
		}
	}
	if !found {
		t.Fatal("etcd_steward_defrag_operation_errors_total metric not found")
	}
}

func TestComponentSpecificMetrics_Isolated(t *testing.T) {
	ResetForTesting()
	regSnap := prometheus.NewRegistry()
	regDefrag := prometheus.NewRegistry()

	mSnap := RegisterComponentWith(Snapshotter, regSnap)
	mDefrag := RegisterComponentWith(Defrag, regDefrag)

	// Record on snapshotter only.
	mSnap.SnapshotRevision.Set(100)
	mDefrag.DefragAttempts.Inc()

	// Verify snapshotter registry has snapshot_revision but not defrag_attempts.
	snapFamilies, _ := regSnap.Gather()
	for _, f := range snapFamilies {
		if f.GetName() == "etcd_steward_defrag_defrag_attempts_total" {
			t.Fatal("snapshotter registry should not contain defrag metrics")
		}
	}

	// Verify defrag registry has defrag_attempts but not snapshot_revision.
	defragFamilies, _ := regDefrag.Gather()
	for _, f := range defragFamilies {
		if f.GetName() == "etcd_steward_snapshotter_snapshot_revision" {
			t.Fatal("defrag registry should not contain snapshotter metrics")
		}
	}

	// Confirm each registry has the expected metric.
	foundSnap := false
	for _, f := range snapFamilies {
		if f.GetName() == "etcd_steward_snapshotter_snapshot_revision" {
			foundSnap = true
		}
	}
	if !foundSnap {
		t.Fatal("snapshotter registry should contain snapshot_revision")
	}

	foundDefrag := false
	for _, f := range defragFamilies {
		if f.GetName() == "etcd_steward_defrag_defrag_attempts_total" {
			foundDefrag = true
		}
	}
	if !foundDefrag {
		t.Fatal("defrag registry should contain defrag_attempts_total")
	}
}

// errForTest is a sentinel error used in tests.
var errForTest = errTestSentinel{}

type errTestSentinel struct{}

func (errTestSentinel) Error() string { return "test error" }
