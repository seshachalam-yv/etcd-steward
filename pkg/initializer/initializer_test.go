// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package initializer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/etcdclient"
	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/statemachine"
)

// mockRecorder records transitions for verification.
type mockRecorder struct {
	transitions []statemachine.Transition
}

func (r *mockRecorder) Record(_ context.Context, _, _ string, t statemachine.Transition) error {
	r.transitions = append(r.transitions, t)
	return nil
}

// mockClusterClient implements etcdclient.ClusterClient for testing.
type mockClusterClient struct {
	members          []etcdclient.Member
	addLearnerID     uint64
	addLearnerErr    error
	promotedID       uint64
	promoteErr       error
	removedPeerURL   string
	wasMemberResult  bool
	wasMemberErr     error
}

func (m *mockClusterClient) AddLearner(_ context.Context, _ string) (uint64, error) {
	return m.addLearnerID, m.addLearnerErr
}

func (m *mockClusterClient) PromoteMember(_ context.Context, memberID uint64) error {
	m.promotedID = memberID
	return m.promoteErr
}

func (m *mockClusterClient) RemoveMember(_ context.Context, _ uint64) error {
	return nil
}

func (m *mockClusterClient) ListMembers(_ context.Context) ([]etcdclient.Member, error) {
	return m.members, nil
}

func (m *mockClusterClient) WasMemberInCluster(_ context.Context, _ string) (bool, error) {
	return m.wasMemberResult, m.wasMemberErr
}

func (m *mockClusterClient) RemoveStaleMember(_ context.Context, peerURL string) error {
	m.removedPeerURL = peerURL
	return nil
}

func TestInitializer_SingleNode_HappyPath(t *testing.T) {
	dataDir := t.TempDir()
	// Create a valid DB file so validation passes.
	dbDir := filepath.Join(dataDir, "member", "snap")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatal(err)
	}

	rec := &mockRecorder{}
	init := New(
		"etcd-main-0", "default",
		"https://etcd-main-0:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380",
		true,  // singleNode
		false, // no learner annotation
		rec,
		&member.NoopClient{},
		&mockClusterClient{},
		nil,
		"http://localhost:2379",
		zap.NewNop(),
		nil, nil,
	)

	if init.GetStatus() != InitializationStatusNew {
		t.Fatalf("expected status New, got %s", init.GetStatus())
	}

	ctx := context.Background()
	if err := init.Start(ctx, "sanity"); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Wait for async initialization.
	deadline := time.After(5 * time.Second)
	for init.GetStatus() != InitializationStatusSuccessful {
		select {
		case <-deadline:
			t.Fatalf("initialization did not complete, status: %s", init.GetStatus())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify transitions.
	if len(rec.transitions) < 2 {
		t.Fatalf("expected at least 2 transitions, got %d", len(rec.transitions))
	}

	// First transition: Initializing/DBValidationSanity.
	if rec.transitions[0].State != statemachine.StateInitializing {
		t.Errorf("transition[0].State = %s, want Initializing", rec.transitions[0].State)
	}

	// Second transition: Started/Leader.
	if rec.transitions[1].State != statemachine.StateStarted {
		t.Errorf("transition[1].State = %s, want Started", rec.transitions[1].State)
	}
}

func TestInitializer_LearnerJoin_HappyPath(t *testing.T) {
	dataDir := t.TempDir()

	rec := &mockRecorder{}
	cc := &mockClusterClient{
		addLearnerID: 12345,
		members:      nil, // No existing learner.
	}

	init := New(
		"etcd-main-1", "default",
		"https://etcd-main-1:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380,etcd-main-1=https://etcd-main-1:2380",
		false, // multi-node
		true,  // has learner annotation
		rec,
		&member.NoopClient{},
		cc,
		nil,
		"http://localhost:2379",
		zap.NewNop(),
		nil, nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := init.Start(ctx, "full"); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Wait for initialization to succeed.
	deadline := time.After(5 * time.Second)
	for init.GetStatus() != InitializationStatusSuccessful {
		select {
		case <-deadline:
			t.Fatalf("initialization did not complete, status: %s", init.GetStatus())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Should be marked as learner.
	if !init.IsLearner() {
		t.Error("expected IsLearner() to be true after learner join")
	}

	// Verify transitions include PendingLearner and Learner.
	if len(rec.transitions) < 2 {
		t.Fatalf("expected at least 2 transitions, got %d", len(rec.transitions))
	}
	foundPending := false
	foundLearner := false
	for _, tr := range rec.transitions {
		if tr.SubState != nil {
			if *tr.SubState == statemachine.SubStatePendingLearner {
				foundPending = true
			}
			if *tr.SubState == statemachine.SubStateLearner {
				foundLearner = true
			}
		}
	}
	if !foundPending {
		t.Error("expected PendingLearner transition")
	}
	if !foundLearner {
		t.Error("expected Learner transition")
	}
}

func TestInitializer_MultiNode_DataLoss_TriggersRecovery(t *testing.T) {
	dataDir := t.TempDir()
	// Empty data dir triggers data-loss detection.

	rec := &mockRecorder{}
	cc := &mockClusterClient{
		addLearnerID:    12345,
		wasMemberResult: false, // Not in cluster.
	}

	init := New(
		"etcd-main-1", "default",
		"https://etcd-main-1:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380,etcd-main-1=https://etcd-main-1:2380",
		false, // multi-node
		false, // no learner annotation
		rec,
		&member.NoopClient{},
		cc,
		nil,
		"http://localhost:2379",
		zap.NewNop(),
		nil, nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := init.Start(ctx, "full"); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Wait for initialization.
	deadline := time.After(5 * time.Second)
	for init.GetStatus() != InitializationStatusSuccessful {
		select {
		case <-deadline:
			t.Fatalf("initialization did not complete, status: %s", init.GetStatus())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if !init.NeedsExistingClusterState() {
		t.Error("expected NeedsExistingClusterState() to be true during data-loss recovery")
	}

	// Verify RemoveStaleMember was called.
	if cc.removedPeerURL != "https://etcd-main-1:2380" {
		t.Errorf("RemoveStaleMember peerURL = %q, want https://etcd-main-1:2380", cc.removedPeerURL)
	}
}

func TestInitializer_SingleNode_ValidationFails_NoStore(t *testing.T) {
	dataDir := t.TempDir()
	// Create a corrupt DB file.
	dbDir := filepath.Join(dataDir, "member", "snap")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dbDir, "db"), []byte("corrupt-data"), 0600); err != nil {
		t.Fatal(err)
	}

	rec := &mockRecorder{}
	init := New(
		"etcd-main-0", "default",
		"https://etcd-main-0:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380",
		true,  // singleNode
		false, // no learner annotation
		rec,
		&member.NoopClient{},
		&mockClusterClient{},
		nil,
		"http://localhost:2379",
		zap.NewNop(),
		nil, nil, // No store configured.
	)

	ctx := context.Background()
	if err := init.Start(ctx, "full"); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Wait for initialization.
	deadline := time.After(5 * time.Second)
	for {
		status := init.GetStatus()
		if status == InitializationStatusSuccessful {
			// tryRestore with nil store should succeed (fresh start).
			break
		}
		select {
		case <-deadline:
			// Check if it stayed InProgress (means initialization failed).
			// With nil store, tryRestore returns nil (etcd starts fresh).
			// So we should eventually succeed.
			t.Fatalf("initialization did not complete, status: %s", init.GetStatus())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify restoration transition was recorded.
	foundRestoration := false
	for _, tr := range rec.transitions {
		if tr.SubState != nil && *tr.SubState == statemachine.SubStateRestoration {
			foundRestoration = true
		}
	}
	if !foundRestoration {
		t.Error("expected Restoration transition for failed validation")
	}
}

func TestInitializer_NeedsExistingClusterState(t *testing.T) {
	tests := []struct {
		name       string
		annotation bool
		recovery   bool
		want       bool
	}{
		{"no annotation, no recovery", false, false, false},
		{"annotation", true, false, true},
		{"recovery", false, true, true},
		{"both", true, true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			init := &Initializer{
				hasCreateAsLearnerAnnotation: tc.annotation,
			}
			init.status.Store(InitializationStatusNew)
			if tc.recovery {
				init.inDataLossRecovery.Store(true)
			}
			if got := init.NeedsExistingClusterState(); got != tc.want {
				t.Errorf("NeedsExistingClusterState() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInitializer_GetStatus_Initial(t *testing.T) {
	init := &Initializer{}
	init.status.Store(InitializationStatusNew)
	if got := init.GetStatus(); got != InitializationStatusNew {
		t.Errorf("GetStatus() = %s, want New", got)
	}
}
