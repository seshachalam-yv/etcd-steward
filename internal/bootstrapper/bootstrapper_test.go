// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package bootstrapper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

// --- Mock implementations ---

type mockRestorer struct {
	findResult string
	findErr    error
	restoreErr error
	restored   string
}

func (m *mockRestorer) FindLatestFullSnapshot(_ context.Context) (string, error) {
	return m.findResult, m.findErr
}

func (m *mockRestorer) Restore(_ context.Context, name string) error {
	m.restored = name
	return m.restoreErr
}

type mockWrapper struct {
	startErr error
	readyErr error
	started  bool
	checked  bool
}

func (m *mockWrapper) StartEmbeddedEtcd(_ context.Context) error {
	m.started = true
	return m.startErr
}

func (m *mockWrapper) CheckReady(_ context.Context) error {
	m.checked = true
	return m.readyErr
}

type mockClusterOp struct {
	removeErr error
	removed   string
}

func (m *mockClusterOp) RemoveMember(_ context.Context, name string) error {
	m.removed = name
	return m.removeErr
}

type mockRecorder struct {
	states []string
}

func (m *mockRecorder) RecordBootstrapState(_ context.Context, _, _, state string) error {
	m.states = append(m.states, state)
	return nil
}

type mockStateMachine struct {
	startedAsNew      bool
	startedAsFollower bool
	triggerErr        error
}

func (m *mockStateMachine) TriggerStartAsNew() error {
	m.startedAsNew = true
	return m.triggerErr
}

func (m *mockStateMachine) TriggerStartAsFollower() error {
	m.startedAsFollower = true
	return m.triggerErr
}

// --- Tests ---

func TestNew(t *testing.T) {
	logger := zap.NewNop()
	b := New("pod-0", "ns", "/tmp/data", true, false, nil, &mockStateMachine{}, &mockRecorder{}, &mockWrapper{}, &mockClusterOp{}, logger)

	if b == nil {
		t.Fatal("expected non-nil Bootstrapper")
	}
	if b.podName != "pod-0" {
		t.Fatalf("expected podName %q, got %q", "pod-0", b.podName)
	}
	if b.namespace != "ns" {
		t.Fatalf("expected namespace %q, got %q", "ns", b.namespace)
	}
	if !b.isSingleNode {
		t.Fatal("expected isSingleNode true")
	}
}

func TestInitialize_EmptyDir_NilRestorer_StartsAsNew(t *testing.T) {
	dataDir := t.TempDir()
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	// Restorer is nil — this is the CRITICAL L1 test.
	b := New("pod-0", "ns", dataDir, true, false, nil, sm, rec, wrapper, &mockClusterOp{}, logger)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sm.startedAsNew {
		t.Fatal("expected state machine to trigger StartAsNew")
	}
	if sm.startedAsFollower {
		t.Fatal("did not expect state machine to trigger StartAsFollower")
	}
	if !wrapper.started {
		t.Fatal("expected wrapper to start etcd")
	}
	if !wrapper.checked {
		t.Fatal("expected wrapper ready check")
	}

	// Verify recorder tracked the states.
	if len(rec.states) < 2 {
		t.Fatalf("expected at least 2 recorded states, got %d", len(rec.states))
	}
	if rec.states[0] != "Initializing" {
		t.Fatalf("expected first state %q, got %q", "Initializing", rec.states[0])
	}
}

func TestInitialize_EmptyDir_WithRestorer_NoSnapshot_StartsAsNew(t *testing.T) {
	dataDir := t.TempDir()
	restorer := &mockRestorer{findResult: ""}
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	b := New("pod-0", "ns", dataDir, true, false, restorer, sm, rec, wrapper, &mockClusterOp{}, logger)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sm.startedAsNew {
		t.Fatal("expected StartAsNew when no snapshot available")
	}
}

func TestInitialize_EmptyDir_WithRestorer_HasSnapshot_Restores(t *testing.T) {
	dataDir := t.TempDir()
	restorer := &mockRestorer{findResult: "Full-1-100-1234567890"}
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	b := New("pod-0", "ns", dataDir, true, false, restorer, sm, rec, wrapper, &mockClusterOp{}, logger)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if restorer.restored != "Full-1-100-1234567890" {
		t.Fatalf("expected restore with snapshot %q, got %q", "Full-1-100-1234567890", restorer.restored)
	}
	if !sm.startedAsFollower {
		t.Fatal("expected StartAsFollower after restore")
	}
}

func TestInitialize_NonEmptyDir_StartsWithExistingState(t *testing.T) {
	dataDir := t.TempDir()
	// Create a file so the directory is not empty.
	if err := os.WriteFile(filepath.Join(dataDir, "member"), []byte("data"), 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	b := New("pod-0", "ns", dataDir, false, false, nil, sm, rec, wrapper, &mockClusterOp{}, logger)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sm.startedAsFollower {
		t.Fatal("expected StartAsFollower for non-empty data dir")
	}
	if sm.startedAsNew {
		t.Fatal("did not expect StartAsNew for non-empty data dir")
	}
}

func TestInitialize_CorruptDir_CleansUpAndStartsNew(t *testing.T) {
	dataDir := t.TempDir()
	// Create CORRUPT marker file.
	if err := os.WriteFile(filepath.Join(dataDir, "CORRUPT"), []byte("1"), 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	clusterOp := &mockClusterOp{}
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	b := New("pod-0", "ns", dataDir, false, false, nil, sm, rec, wrapper, clusterOp, logger)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Member should have been removed from cluster.
	if clusterOp.removed != "pod-0" {
		t.Fatalf("expected member %q to be removed, got %q", "pod-0", clusterOp.removed)
	}

	// After cleanup, data dir is empty -> starts as new.
	if !sm.startedAsNew {
		t.Fatal("expected StartAsNew after corrupt cleanup")
	}
}

func TestInitialize_RestoreFailure_ReturnsError(t *testing.T) {
	dataDir := t.TempDir()
	restorer := &mockRestorer{
		findResult: "Full-1-100-1234567890",
		restoreErr: fmt.Errorf("restore failed"),
	}
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	b := New("pod-0", "ns", dataDir, true, false, restorer, sm, rec, wrapper, &mockClusterOp{}, logger)

	err := b.Initialize(context.Background())
	if err == nil {
		t.Fatal("expected error when restore fails")
	}
}

func TestInitialize_WrapperStartFailure_ReturnsError(t *testing.T) {
	dataDir := t.TempDir()
	wrapper := &mockWrapper{startErr: fmt.Errorf("wrapper start failed")}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	b := New("pod-0", "ns", dataDir, true, false, nil, sm, rec, wrapper, &mockClusterOp{}, logger)

	err := b.Initialize(context.Background())
	if err == nil {
		t.Fatal("expected error when wrapper start fails")
	}
}

func TestInitialize_WrapperReadyFailure_ReturnsError(t *testing.T) {
	dataDir := t.TempDir()
	wrapper := &mockWrapper{readyErr: fmt.Errorf("etcd not ready")}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	logger := zap.NewNop()

	b := New("pod-0", "ns", dataDir, true, false, nil, sm, rec, wrapper, &mockClusterOp{}, logger)

	err := b.Initialize(context.Background())
	if err == nil {
		t.Fatal("expected error when etcd not ready")
	}
}

func TestIsDataDirEmpty_NonexistentDir(t *testing.T) {
	logger := zap.NewNop()
	b := New("pod-0", "ns", "/nonexistent/path/that/should/not/exist", true, false, nil, &mockStateMachine{}, &mockRecorder{}, &mockWrapper{}, &mockClusterOp{}, logger)

	if !b.IsDataDirEmpty() {
		t.Fatal("expected nonexistent directory to be reported as empty")
	}
}

func TestIsDataDirCorrupt_NoMarker(t *testing.T) {
	dataDir := t.TempDir()
	logger := zap.NewNop()
	b := New("pod-0", "ns", dataDir, true, false, nil, &mockStateMachine{}, &mockRecorder{}, &mockWrapper{}, &mockClusterOp{}, logger)

	if b.IsDataDirCorrupt() {
		t.Fatal("expected clean directory to not be reported as corrupt")
	}
}
