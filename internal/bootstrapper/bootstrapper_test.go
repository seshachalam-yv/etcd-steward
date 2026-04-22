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
	removeErr          error
	removed            string
	addLearnerErr      error
	addLearnerID       uint64
	addLearnerPeerURLs []string
	promoteErr         error
	promotedID         uint64
	removeByIDErr      error
	removedByID        uint64
}

func (m *mockClusterOp) RemoveMember(_ context.Context, name string) error {
	m.removed = name
	return m.removeErr
}

func (m *mockClusterOp) MemberAddAsLearner(_ context.Context, peerURLs []string) (uint64, error) {
	m.addLearnerPeerURLs = peerURLs
	if m.addLearnerErr != nil {
		return 0, m.addLearnerErr
	}
	return m.addLearnerID, nil
}

func (m *mockClusterOp) MemberPromote(_ context.Context, memberID uint64) error {
	m.promotedID = memberID
	return m.promoteErr
}

func (m *mockClusterOp) MemberRemoveByID(_ context.Context, memberID uint64) error {
	m.removedByID = memberID
	return m.removeByIDErr
}

type mockRecorder struct {
	states []string
}

func (m *mockRecorder) RecordBootstrapState(_ context.Context, _, _, state string) error {
	m.states = append(m.states, state)
	return nil
}

type mockStateMachine struct {
	startedAsNew            bool
	startedAsFollower       bool
	startedAsPendingLearner bool
	learnerJoined           bool
	promoted                bool
	triggerErr              error
}

func (m *mockStateMachine) TriggerStartAsNew() error {
	m.startedAsNew = true
	return m.triggerErr
}

func (m *mockStateMachine) TriggerStartAsFollower() error {
	m.startedAsFollower = true
	return m.triggerErr
}

func (m *mockStateMachine) TriggerStartAsPendingLearner() error {
	m.startedAsPendingLearner = true
	return m.triggerErr
}

func (m *mockStateMachine) TriggerLearnerJoined() error {
	m.learnerJoined = true
	return m.triggerErr
}

func (m *mockStateMachine) TriggerPromoted() error {
	m.promoted = true
	return m.triggerErr
}

// --- Helpers ---

// newBootstrapper creates a Bootstrapper with common defaults for tests.
func newBootstrapper(
	podName, dataDir string,
	isSingleNode, isLearner bool,
	memberID uint64,
	peerURLs []string,
	restorer RestoreProvider,
	sm StateMachineProvider,
	rec Recorder,
	wrapper WrapperClient,
	cluster ClusterOperator,
) *Bootstrapper {
	return New(podName, "ns", dataDir, isSingleNode, isLearner, memberID, peerURLs, restorer, sm, rec, wrapper, cluster, zap.NewNop())
}

// --- Tests ---

func TestNew(t *testing.T) {
	b := New("pod-0", "ns", "/tmp/data", true, false, 0, nil, nil, &mockStateMachine{}, &mockRecorder{}, &mockWrapper{}, &mockClusterOp{}, zap.NewNop())

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

	// Restorer is nil — this is the CRITICAL L1 test.
	b := newBootstrapper("pod-0", dataDir, true, false, 0, nil, nil, sm, rec, wrapper, &mockClusterOp{})

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

	b := newBootstrapper("pod-0", dataDir, true, false, 0, nil, restorer, sm, rec, wrapper, &mockClusterOp{})

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

	b := newBootstrapper("pod-0", dataDir, true, false, 0, nil, restorer, sm, rec, wrapper, &mockClusterOp{})

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

	b := newBootstrapper("pod-0", dataDir, false, false, 0, nil, nil, sm, rec, wrapper, &mockClusterOp{})

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

	// Single-node (memberID=0) falls back to RemoveMember by name.
	b := newBootstrapper("pod-0", dataDir, true, false, 0, nil, nil, sm, rec, wrapper, clusterOp)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Member should have been removed from cluster by name.
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

	b := newBootstrapper("pod-0", dataDir, true, false, 0, nil, restorer, sm, rec, wrapper, &mockClusterOp{})

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

	b := newBootstrapper("pod-0", dataDir, true, false, 0, nil, nil, sm, rec, wrapper, &mockClusterOp{})

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

	b := newBootstrapper("pod-0", dataDir, true, false, 0, nil, nil, sm, rec, wrapper, &mockClusterOp{})

	err := b.Initialize(context.Background())
	if err == nil {
		t.Fatal("expected error when etcd not ready")
	}
}

func TestIsDataDirEmpty_NonexistentDir(t *testing.T) {
	b := New("pod-0", "ns", "/nonexistent/path/that/should/not/exist", true, false, 0, nil, nil, &mockStateMachine{}, &mockRecorder{}, &mockWrapper{}, &mockClusterOp{}, zap.NewNop())

	if !b.IsDataDirEmpty() {
		t.Fatal("expected nonexistent directory to be reported as empty")
	}
}

func TestIsDataDirCorrupt_NoMarker(t *testing.T) {
	dataDir := t.TempDir()
	b := New("pod-0", "ns", dataDir, true, false, 0, nil, nil, &mockStateMachine{}, &mockRecorder{}, &mockWrapper{}, &mockClusterOp{}, zap.NewNop())

	if b.IsDataDirCorrupt() {
		t.Fatal("expected clean directory to not be reported as corrupt")
	}
}

// --- Task 1: Learner join flow tests ---

func TestBootstrapper_LearnerJoin(t *testing.T) {
	dataDir := t.TempDir()
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	clusterOp := &mockClusterOp{addLearnerID: 12345}
	peerURLs := []string{"https://pod-1.etcd:2380"}

	b := newBootstrapper("pod-1", dataDir, false, true, 0, peerURLs, nil, sm, rec, wrapper, clusterOp)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify MemberAddAsLearner was called with the correct peer URLs.
	if len(clusterOp.addLearnerPeerURLs) != 1 || clusterOp.addLearnerPeerURLs[0] != peerURLs[0] {
		t.Fatalf("expected MemberAddAsLearner called with %v, got %v", peerURLs, clusterOp.addLearnerPeerURLs)
	}

	// Verify state transitions: Unknown -> PendingLearner -> Learner.
	if !sm.startedAsPendingLearner {
		t.Fatal("expected TriggerStartAsPendingLearner to be called")
	}
	if !sm.learnerJoined {
		t.Fatal("expected TriggerLearnerJoined to be called")
	}

	// Verify etcd was started.
	if !wrapper.started {
		t.Fatal("expected wrapper to start etcd")
	}
	if !wrapper.checked {
		t.Fatal("expected wrapper ready check")
	}

	// Verify memberID was stored.
	if b.memberID != 12345 {
		t.Fatalf("expected memberID %d, got %d", 12345, b.memberID)
	}

	// Verify recorder states.
	expectedStates := []string{"Initializing", "JoiningAsLearner", "Running", "Learner"}
	if len(rec.states) != len(expectedStates) {
		t.Fatalf("expected %d recorded states, got %d: %v", len(expectedStates), len(rec.states), rec.states)
	}
	for i, want := range expectedStates {
		if rec.states[i] != want {
			t.Fatalf("state[%d]: expected %q, got %q", i, want, rec.states[i])
		}
	}
}

func TestBootstrapper_LearnerJoin_ClusterDown(t *testing.T) {
	dataDir := t.TempDir()
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	clusterOp := &mockClusterOp{addLearnerErr: fmt.Errorf("connection refused")}
	peerURLs := []string{"https://pod-1.etcd:2380"}

	b := newBootstrapper("pod-1", dataDir, false, true, 0, peerURLs, nil, sm, rec, wrapper, clusterOp)

	err := b.Initialize(context.Background())
	if err == nil {
		t.Fatal("expected error when cluster is unreachable")
	}

	// Wrapper must not have been started.
	if wrapper.started {
		t.Fatal("wrapper should not start when learner add fails")
	}

	// State machine should not have transitioned.
	if sm.startedAsPendingLearner {
		t.Fatal("PendingLearner transition should not occur when cluster is unreachable")
	}
}

// --- Task 2: Learner promotion tests ---

func TestBootstrapper_PromoteLearner(t *testing.T) {
	dataDir := t.TempDir()
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	clusterOp := &mockClusterOp{}

	b := newBootstrapper("pod-1", dataDir, false, false, 0, nil, nil, sm, rec, wrapper, clusterOp)

	err := b.PromoteLearner(context.Background(), 12345)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify MemberPromote was called with the correct ID.
	if clusterOp.promotedID != 12345 {
		t.Fatalf("expected MemberPromote called with ID %d, got %d", 12345, clusterOp.promotedID)
	}

	// Verify state transition: Learner -> Follower.
	if !sm.promoted {
		t.Fatal("expected TriggerPromoted to be called")
	}

	// Verify recorder captured the Follower state.
	if len(rec.states) != 1 || rec.states[0] != "Follower" {
		t.Fatalf("expected recorded state [\"Follower\"], got %v", rec.states)
	}
}

func TestBootstrapper_PromoteLearner_Error(t *testing.T) {
	dataDir := t.TempDir()
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}
	clusterOp := &mockClusterOp{promoteErr: fmt.Errorf("learner not ready")}

	b := newBootstrapper("pod-1", dataDir, false, false, 0, nil, nil, sm, rec, wrapper, clusterOp)

	err := b.PromoteLearner(context.Background(), 99999)
	if err == nil {
		t.Fatal("expected error when MemberPromote fails")
	}

	// State machine should NOT have transitioned.
	if sm.promoted {
		t.Fatal("TriggerPromoted should not be called when MemberPromote fails")
	}
}

// --- Task 3: Corrupt data multi-node member removal tests ---

func TestBootstrapper_CorruptData_MultiNode_RemovesMember(t *testing.T) {
	dataDir := t.TempDir()
	// Create CORRUPT marker file.
	if err := os.WriteFile(filepath.Join(dataDir, "CORRUPT"), []byte("1"), 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	clusterOp := &mockClusterOp{}
	wrapper := &mockWrapper{}
	sm := &mockStateMachine{}
	rec := &mockRecorder{}

	// Multi-node: isSingleNode=false, memberID=42.
	b := newBootstrapper("pod-0", dataDir, false, false, 42, nil, nil, sm, rec, wrapper, clusterOp)

	err := b.Initialize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// MemberRemoveByID should have been called with the member's own ID.
	if clusterOp.removedByID != 42 {
		t.Fatalf("expected MemberRemoveByID called with ID %d, got %d", 42, clusterOp.removedByID)
	}

	// The name-based RemoveMember should NOT have been called.
	if clusterOp.removed != "" {
		t.Fatalf("expected name-based RemoveMember not to be called, but got %q", clusterOp.removed)
	}

	// After cleanup, data dir is empty -> starts as new.
	if !sm.startedAsNew {
		t.Fatal("expected StartAsNew after corrupt cleanup")
	}
}
