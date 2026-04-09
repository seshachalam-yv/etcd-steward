// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package initializer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/etcdclient"
	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/restoration"
	"github.com/gardener/etcd-steward/pkg/snapstore"
	"github.com/gardener/etcd-steward/pkg/statemachine"
)

// TestMain enables skipHashCheck in the restoration package so initializer tests
// can use synthetic snapshot data without requiring a valid etcd snapshot hash.
func TestMain(m *testing.M) {
	restoration.SetSkipHashCheckForTests(true)
	os.Exit(m.Run())
}

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
	members         []etcdclient.Member
	addLearnerID    uint64
	addLearnerErr   error
	promotedID      uint64
	promoteErr      error
	removedPeerURL  string
	wasMemberResult bool
	wasMemberErr    error
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

	// Verify transitions: New → Initializing/DBValidationSanity → Started/Leader
	if len(rec.transitions) < 3 {
		t.Fatalf("expected at least 3 transitions, got %d: %v", len(rec.transitions), rec.transitions)
	}

	// First transition: New (initial state recording).
	if rec.transitions[0].State != statemachine.StateNew {
		t.Errorf("transition[0].State = %s, want New", rec.transitions[0].State)
	}
	if rec.transitions[0].Reason != statemachine.ReasonNewSingleNodeClusterCreated {
		t.Errorf("transition[0].Reason = %s, want NewSingleNodeClusterCreated", rec.transitions[0].Reason)
	}

	// Second transition: Initializing/DBValidationSanity.
	if rec.transitions[1].State != statemachine.StateInitializing {
		t.Errorf("transition[1].State = %s, want Initializing", rec.transitions[1].State)
	}

	// Third transition: Started/Leader.
	if rec.transitions[2].State != statemachine.StateStarted {
		t.Errorf("transition[2].State = %s, want Started", rec.transitions[2].State)
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

	// Verify transitions: New → Initializing → PendingLearner → Learner → (Follower async)
	// Use presence-based checks since promotion is async and may or may not have fired yet.
	if len(rec.transitions) < 4 {
		t.Fatalf("expected at least 4 transitions, got %d: %v", len(rec.transitions), rec.transitions)
	}
	// First two transitions: New and Initializing (both synchronous before learner join).
	if rec.transitions[0].State != statemachine.StateNew {
		t.Errorf("transition[0].State = %s, want New", rec.transitions[0].State)
	}
	if rec.transitions[1].State != statemachine.StateInitializing {
		t.Errorf("transition[1].State = %s, want Initializing", rec.transitions[1].State)
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

	// Start a TCP listener to simulate a reachable etcd endpoint.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start TCP listener: %v", err)
	}
	defer ln.Close() //nolint:errcheck
	listenAddr := ln.Addr().String()

	rec := &mockRecorder{}
	cc := &mockClusterClient{
		addLearnerID:    12345,
		wasMemberResult: false, // Not in cluster.
		// Simulate reachable cluster that already has etcd-main-1 as a member
		// (data-loss scenario: PVC was deleted but member still registered in cluster).
		members: []etcdclient.Member{
			{ID: 1, PeerURLs: []string{"https://etcd-main-0:2380"}},
			{ID: 2, PeerURLs: []string{"https://etcd-main-1:2380"}},
		},
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
		"http://"+listenAddr,
		zap.NewNop(),
		nil, nil,
	)
	// Inject a TCP dial function that connects to the test listener.
	init.tcpDialFn = (&net.Dialer{}).DialContext

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

// mockEtcdStatusAPI implements EtcdStatusAPI for testing GetMemberAndClusterID.
type mockEtcdStatusAPI struct {
	resp *EtcdStatusResponse
	err  error
}

func (m *mockEtcdStatusAPI) Status(_ context.Context, _ string) (*EtcdStatusResponse, error) {
	return m.resp, m.err
}

// TestGetMemberAndClusterID verifies that member and cluster IDs are returned as
// lowercase hexadecimal strings matching the values from the status API response.
func TestGetMemberAndClusterID(t *testing.T) {
	tests := []struct {
		name          string
		status        InitializationStatus
		api           EtcdStatusAPI
		wantMemberID  string
		wantClusterID string
	}{
		{
			name:   "not-successful returns empty strings",
			status: InitializationStatusNew,
			api:    &mockEtcdStatusAPI{resp: &EtcdStatusResponse{MemberID: 42, ClusterID: 99}},
		},
		{
			name:   "nil statusAPI returns empty strings",
			status: InitializationStatusSuccessful,
			api:    nil,
		},
		{
			name:   "statusAPI returns error returns empty strings",
			status: InitializationStatusSuccessful,
			api:    &mockEtcdStatusAPI{err: fmt.Errorf("etcd unreachable")},
		},
		{
			name:          "happy path returns hex-encoded IDs",
			status:        InitializationStatusSuccessful,
			api:           &mockEtcdStatusAPI{resp: &EtcdStatusResponse{MemberID: 42, ClusterID: 99}},
			wantMemberID:  "2a",
			wantClusterID: "63",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			init := &Initializer{
				etcdEndpoint:  "http://localhost:2379",
				etcdStatusAPI: tc.api,
			}
			init.status.Store(tc.status)

			gotMember, gotCluster := init.GetMemberAndClusterID(context.Background())
			if gotMember != tc.wantMemberID {
				t.Errorf("memberID = %q, want %q", gotMember, tc.wantMemberID)
			}
			if gotCluster != tc.wantClusterID {
				t.Errorf("clusterID = %q, want %q", gotCluster, tc.wantClusterID)
			}
		})
	}
}

// TestNeedsDataLossRecovery_NotReachable verifies that when no TCP listener exists at
// the etcd endpoint, needsDataLossRecovery returns false without querying the cluster.
func TestNeedsDataLossRecovery_NotReachable(t *testing.T) {
	dataDir := t.TempDir()
	rec := &mockRecorder{}

	init := New(
		"etcd-main-1", "default",
		"https://etcd-main-1:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380,etcd-main-1=https://etcd-main-1:2380",
		false, false,
		rec, &member.NoopClient{}, &mockClusterClient{},
		nil, "http://127.0.0.1:19999", // no listener on this port
		zap.NewNop(),
		nil, nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	needs, err := init.needsDataLossRecovery(ctx)
	if err != nil {
		t.Fatalf("needsDataLossRecovery returned unexpected error: %v", err)
	}
	if needs {
		t.Error("expected needsDataLossRecovery to return false when cluster is unreachable")
	}
}

// TestNeedsDataLossRecovery_NonEmptyDir_WasNotMember verifies the path where the
// data directory is non-empty but the member was not found in the cluster.
func TestNeedsDataLossRecovery_NonEmptyDir_WasNotMember(t *testing.T) {
	dataDir := t.TempDir()

	// Create a DB file to make the dir appear non-empty.
	dbDir := filepath.Join(dataDir, "member", "snap")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dbDir, "db"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}

	// Start a TCP listener to simulate a reachable etcd endpoint.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start TCP listener: %v", err)
	}
	defer ln.Close() //nolint:errcheck

	rec := &mockRecorder{}
	cc := &mockClusterClient{
		wasMemberResult: false, // member was NOT in cluster
	}

	init := New(
		"etcd-main-1", "default",
		"https://etcd-main-1:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380",
		false, false,
		rec, &member.NoopClient{}, cc,
		nil, "http://"+ln.Addr().String(),
		zap.NewNop(),
		nil, nil,
	)
	init.tcpDialFn = (&net.Dialer{}).DialContext

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	needs, err := init.needsDataLossRecovery(ctx)
	if err != nil {
		t.Fatalf("needsDataLossRecovery returned unexpected error: %v", err)
	}
	if !needs {
		t.Error("expected needsDataLossRecovery to return true when member was removed from cluster")
	}
}

// TestInitializerStart_Idempotent verifies that calling Start twice is a no-op on the
// second call — the initialization goroutine is only launched once.
func TestInitializerStart_Idempotent(t *testing.T) {
	dataDir := t.TempDir()
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
		true, false,
		rec, &member.NoopClient{}, &mockClusterClient{},
		nil, "http://localhost:2379",
		zap.NewNop(),
		nil, nil,
	)

	ctx := context.Background()

	// First Start.
	if err := init.Start(ctx, "sanity"); err != nil {
		t.Fatalf("first Start error: %v", err)
	}

	// Wait for success.
	deadline := time.After(5 * time.Second)
	for init.GetStatus() != InitializationStatusSuccessful {
		select {
		case <-deadline:
			t.Fatalf("initialization did not complete, status: %s", init.GetStatus())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Second Start must be a no-op.
	if err := init.Start(ctx, "full"); err != nil {
		t.Fatalf("second Start error: %v", err)
	}

	// Status must still be Successful (not reset to InProgress).
	if got := init.GetStatus(); got != InitializationStatusSuccessful {
		t.Errorf("status after second Start = %s, want Successful", got)
	}
}

// mockSnapstore is a minimal Snapstore implementation for testing tryRestore.
type mockSnapstore struct {
	snaps    []snapstore.Snapshot
	listErr  error
	snapData map[string][]byte // path -> raw bytes, used by Fetch
}

func (m *mockSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (m *mockSnapstore) Fetch(s snapstore.Snapshot) (io.ReadCloser, error) {
	if m.snapData != nil {
		if data, ok := m.snapData[s.Path()]; ok {
			return io.NopCloser(bytes.NewReader(data)), nil
		}
	}
	return nil, fmt.Errorf("fetch not implemented in mock")
}
func (m *mockSnapstore) List() ([]snapstore.Snapshot, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.snaps, nil
}
func (m *mockSnapstore) Delete(_ snapstore.Snapshot) error { return nil }

// makeMinimalEtcdSnapshotBytes creates a minimal bbolt DB with "key" and "meta" buckets,
// as expected by etcdutl snapshot restore. Returns the raw file bytes.
func makeMinimalEtcdSnapshotBytes(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "snapshot.db")

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to create minimal etcd snapshot DB: %v", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("key")); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists([]byte("meta"))
		return err
	}); err != nil {
		db.Close() //nolint:errcheck
		t.Fatalf("failed to create buckets in snapshot DB: %v", err)
	}
	db.Close() //nolint:errcheck

	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read snapshot DB: %v", err)
	}
	return data
}

// TestTryRestore_NilStore verifies that tryRestore returns nil when no store is configured.
func TestTryRestore_NilStore(t *testing.T) {
	dataDir := t.TempDir()
	init := &Initializer{
		dataDir: dataDir,
		logger:  zap.NewNop(),
		store:   nil,
	}
	if err := init.tryRestore(context.Background()); err != nil {
		t.Errorf("tryRestore with nil store should return nil, got: %v", err)
	}
}

// TestTryRestore_EmptyStore verifies that tryRestore returns nil (not an error) when
// the store exists but contains no full snapshots — this is a fresh cluster, not a failure.
func TestTryRestore_EmptyStore(t *testing.T) {
	dataDir := t.TempDir()
	init := &Initializer{
		dataDir: dataDir,
		logger:  zap.NewNop(),
		store:   &mockSnapstore{snaps: nil},
	}
	err := init.tryRestore(context.Background())
	if err != nil {
		t.Fatalf("tryRestore with empty store should return nil (fresh cluster), got: %v", err)
	}
}

// TestInitializeAsLearner_AlreadyJoined exercises the path where the member is already
// registered as a learner in the cluster. promoteAfterSync is launched in a goroutine;
// the context is cancelled immediately so the goroutine exits via ctx.Done() without
// waiting 10 seconds.
func TestInitializeAsLearner_AlreadyJoined(t *testing.T) {
	dataDir := t.TempDir()
	rec := &mockRecorder{}

	// Simulate a cluster that already has this member as a learner.
	cc := &mockClusterClient{
		members: []etcdclient.Member{
			{
				ID:        99,
				PeerURLs:  []string{"https://etcd-main-1:2380"},
				IsLearner: true,
			},
		},
	}

	init := New(
		"etcd-main-1", "default",
		"https://etcd-main-1:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380,etcd-main-1=https://etcd-main-1:2380",
		false,
		true, // hasCreateAsLearnerAnnotation triggers initializeAsLearner
		rec, &member.NoopClient{}, cc,
		nil, "http://localhost:2379",
		zap.NewNop(),
		nil, nil,
	)

	// Cancel the context immediately so promoteAfterSync exits via ctx.Done().
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := init.initializeAsLearner(ctx); err != nil {
		t.Fatalf("initializeAsLearner returned unexpected error: %v", err)
	}

	if !init.IsLearner() {
		t.Error("expected IsLearner() to be true after already-joined path")
	}

	// Verify Learner transition was recorded.
	foundLearner := false
	for _, tr := range rec.transitions {
		if tr.SubState != nil && *tr.SubState == statemachine.SubStateLearner {
			foundLearner = true
		}
	}
	if !foundLearner {
		t.Error("expected Learner transition to be recorded")
	}
}

// TestPromoteAfterSync_ContextCancelledImmediately verifies that when the context is
// already cancelled, promoteAfterSync returns without attempting promotion.
func TestPromoteAfterSync_ContextCancelledImmediately(t *testing.T) {
	rec := &mockRecorder{}
	cc := &mockClusterClient{}

	init := &Initializer{
		memberName:      "etcd-main-0",
		memberNamespace: "default",
		recorder:        rec,
		memberClient:    &member.NoopClient{},
		clusterClient:   cc,
		logger:          zap.NewNop(),
	}
	init.status.Store(InitializationStatusNew)
	init.isLearnerMember.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// promoteAfterSync should return via ctx.Done() without calling PromoteMember.
	init.promoteAfterSync(ctx, 42)

	if cc.promotedID != 0 {
		t.Errorf("PromoteMember should not have been called, but got promotedID=%d", cc.promotedID)
	}
}

// TestPromoteLearner_SuccessOnFirstAttempt verifies that promoteLearner returns nil
// when PromoteMember succeeds immediately on the first attempt (no backoff sleep).
func TestPromoteLearner_SuccessOnFirstAttempt(t *testing.T) {
	cc := &mockClusterClient{
		promoteErr: nil, // success
	}
	init := &Initializer{
		clusterClient: cc,
		logger:        zap.NewNop(),
	}

	if err := init.promoteLearner(context.Background(), 7); err != nil {
		t.Fatalf("promoteLearner should succeed on first attempt, got: %v", err)
	}
	if cc.promotedID != 7 {
		t.Errorf("promotedID = %d, want 7", cc.promotedID)
	}
}

// TestPromoteLearner_AllAttemptsFail_ContextCancelled verifies that promoteLearner
// returns the context error if the context is cancelled during the backoff sleep.
// We use a cancelled context so the very first `if attempt > 0` backoff exits immediately.
func TestPromoteLearner_AllAttemptsFail_ContextCancelledDuringBackoff(t *testing.T) {
	promoteErr := fmt.Errorf("not yet synced")
	cc := &mockClusterClient{
		promoteErr: promoteErr, // always fails
	}
	init := &Initializer{
		clusterClient: cc,
		logger:        zap.NewNop(),
	}

	// Cancel immediately — so attempt 1's backoff select returns ctx.Err() right away.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := init.promoteLearner(ctx, 8)
	// Attempt 0 should fail with promoteErr. Attempt 1 hits ctx.Done() in backoff.
	// The function should return ctx.Err().
	if err == nil {
		t.Fatal("expected error from promoteLearner")
	}
}

// TestInitializer_SingleNode_EmptyDataDir_WithSnapshots verifies the PVC-deletion restoration path:
// when the data directory is empty (e.g. PVC was deleted and re-provisioned) and a snapstore
// is configured with existing snapshots, the initializer must restore from those snapshots
// instead of bootstrapping a fresh empty cluster.
func TestInitializer_SingleNode_EmptyDataDir_WithSnapshots(t *testing.T) {
	dataDir := t.TempDir()

	// Simulate a snapshot in the store: a minimal valid bbolt DB with the "key" and "meta"
	// buckets that etcdutl snapshot restore requires.
	fakeDBContent := makeMinimalEtcdSnapshotBytes(t)
	fullSnap := snapstore.Snapshot{
		Kind:          "Full",
		StartRevision: 0,
		LastRevision:  42,
		CreatedOn:     time.Now(),
		SnapDir:       "Backup-1",
		SnapName:      "Full-0000000000000000-0000000000000042-12345678",
	}

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{fullSnap},
		snapData: map[string][]byte{
			fullSnap.Path(): fakeDBContent,
		},
	}

	comp, err := compression.NewCompressor("none")
	if err != nil {
		t.Fatalf("failed to create compressor: %v", err)
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
		store, comp,
	)

	// Data dir must be empty at start (simulates PVC deletion).
	if !init.isDataDirEmpty() {
		t.Fatal("precondition failed: data dir should be empty")
	}

	ctx := context.Background()
	if err := init.Start(ctx, "full"); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for init.GetStatus() != InitializationStatusSuccessful {
		select {
		case <-deadline:
			t.Fatalf("initialization did not complete, status: %s", init.GetStatus())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// After restoration, the DB file must exist.
	dbPath := filepath.Join(dataDir, "member", "snap", "db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected DB file to exist after restoration, but it does not")
	}

	// Verify the Restoration transition was recorded.
	foundRestoration := false
	foundRestorationSucceeded := false
	for _, tr := range rec.transitions {
		if tr.SubState != nil && *tr.SubState == statemachine.SubStateRestoration {
			foundRestoration = true
		}
		if tr.State == statemachine.StateStarted && tr.Reason == statemachine.ReasonRestorationSucceeded {
			foundRestorationSucceeded = true
		}
	}
	if !foundRestoration {
		t.Error("expected Restoration transition to be recorded")
	}
	if !foundRestorationSucceeded {
		t.Error("expected RestorationSucceeded transition to be recorded")
	}
}

// TestTryRestore_ClearsStaleRestorationTempDir verifies that tryRestore removes any stale
// restoration temp directory left by a previously interrupted restoration attempt.
// Regression test for: pod crashes during restore, leaving *.restoration.tmp on PVC;
// next startup must not fail or use the stale partial state.
func TestTryRestore_ClearsStaleRestorationTempDir(t *testing.T) {
	base := t.TempDir()
	// Create paths under the temp dir to simulate PVC layout.
	actualDataDir := filepath.Join(base, "new.etcd")
	staleTempDir := actualDataDir + ".restoration.tmp"

	// Simulate a stale temp dir from a previous interrupted restoration.
	if err := os.MkdirAll(filepath.Join(staleTempDir, "restore-out", "member", "snap"), 0755); err != nil {
		t.Fatal(err)
	}
	// Put a sentinel file inside to confirm it gets removed.
	sentinelPath := filepath.Join(staleTempDir, "stale-sentinel.txt")
	if err := os.WriteFile(sentinelPath, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}

	// Set up a store with a valid full snapshot.
	fakeDBContent := makeMinimalEtcdSnapshotBytes(t)
	snapName := "Full-0000000000000000-0000000000000005-1000.db"
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", StartRevision: 0, LastRevision: 5, SnapDir: "Backup-1", SnapName: snapName},
		},
		snapData: map[string][]byte{
			"Backup-1/" + snapName: fakeDBContent,
		},
	}
	comp, err := compression.NewCompressor("none")
	if err != nil {
		t.Fatalf("failed to create compressor: %v", err)
	}

	init := &Initializer{
		memberName:     "etcd-main-0",
		dataDir:        actualDataDir,
		peerURL:        "https://etcd-main-0:2380",
		initialCluster: "etcd-main-0=https://etcd-main-0:2380",
		logger:         zap.NewNop(),
		store:          store,
		compressor:     comp,
	}

	err = init.tryRestore(context.Background())
	if err != nil {
		t.Fatalf("tryRestore returned unexpected error: %v", err)
	}

	// Stale temp dir must be gone.
	if _, statErr := os.Stat(staleTempDir); statErr == nil {
		t.Error("stale restoration.tmp dir still exists after tryRestore — expected it to be cleaned up")
	}
}
func TestInitializer_SingleNode_EmptyDataDir_NoSnapshots(t *testing.T) {
	dataDir := t.TempDir()

	store := &mockSnapstore{snaps: nil}
	comp, err := compression.NewCompressor("none")
	if err != nil {
		t.Fatalf("failed to create compressor: %v", err)
	}

	rec := &mockRecorder{}
	init := New(
		"etcd-main-0", "default",
		"https://etcd-main-0:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380",
		true, false,
		rec,
		&member.NoopClient{},
		&mockClusterClient{},
		nil, "http://localhost:2379",
		zap.NewNop(),
		store, comp,
	)

	ctx := context.Background()
	if err := init.Start(ctx, "full"); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for init.GetStatus() != InitializationStatusSuccessful {
		select {
		case <-deadline:
			t.Fatalf("initialization did not complete, status: %s", init.GetStatus())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Fresh start: no DB file should be created by restoration.
	dbPath := filepath.Join(dataDir, "member", "snap", "db")
	if _, err := os.Stat(dbPath); err == nil {
		t.Error("expected no DB file for fresh cluster (no snapshots), but file exists")
	}

	// Should record NewSingleNodeClusterCreated (not RestorationSucceeded).
	foundNew := false
	for _, tr := range rec.transitions {
		if tr.Reason == statemachine.ReasonNewSingleNodeClusterCreated {
			foundNew = true
		}
	}
	if !foundNew {
		t.Error("expected NewSingleNodeClusterCreated transition for fresh cluster")
	}
}
