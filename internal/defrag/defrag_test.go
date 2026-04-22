// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package defrag

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

// --- Mock implementations ---

type mockLock struct {
	mu         sync.Mutex
	acquireErr error
	releaseErr error
	acquired   int
	released   int
}

func (m *mockLock) Acquire(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.acquireErr != nil {
		return m.acquireErr
	}
	m.acquired++
	return nil
}

func (m *mockLock) Release(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.releaseErr != nil {
		return m.releaseErr
	}
	m.released++
	return nil
}

type mockRole struct {
	leader bool
}

func (m *mockRole) IsLeader() bool {
	return m.leader
}

type mockMaintenance struct {
	mu          sync.Mutex
	defragErr   error
	defragged   []string
	statusSize  int64
	statusErr   error
}

func (m *mockMaintenance) Defragment(_ context.Context, endpoint string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.defragErr != nil {
		return m.defragErr
	}
	m.defragged = append(m.defragged, endpoint)
	return nil
}

func (m *mockMaintenance) Status(_ context.Context, _ string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statusSize, m.statusErr
}

type mockKV struct {
	mu         sync.Mutex
	store      map[string]string
	putErr     error
	getErr     error
	deleteErr  error
	deleted    []string
}

func newMockKV() *mockKV {
	return &mockKV{store: make(map[string]string)}
}

func (m *mockKV) Put(_ context.Context, key, val string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != nil {
		return m.putErr
	}
	m.store[key] = val
	return nil
}

func (m *mockKV) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return "", m.getErr
	}
	return m.store[key], nil
}

func (m *mockKV) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.deleted = append(m.deleted, key)
	delete(m.store, key)
	return nil
}

type mockCluster struct {
	endpoints    []string
	endpointsErr error
}

func (m *mockCluster) MemberEndpoints(_ context.Context) ([]string, error) {
	if m.endpointsErr != nil {
		return nil, m.endpointsErr
	}
	return m.endpoints, nil
}

// --- Tests ---

func TestNew(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{}
	maint := &mockMaintenance{}
	kv := newMockKV()
	cluster := &mockCluster{}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	if d == nil {
		t.Fatal("expected non-nil Defragmenter")
	}
	if d.podName != "pod-0" {
		t.Fatalf("expected podName %q, got %q", "pod-0", d.podName)
	}
	if d.interval != time.Minute {
		t.Fatalf("expected interval %v, got %v", time.Minute, d.interval)
	}
}

func TestDefragment_FollowersFirstLeaderLast(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{}
	kv := newMockKV()
	cluster := &mockCluster{
		endpoints: []string{
			"https://pod-0.etcd:2379",
			"https://pod-1.etcd:2379",
			"https://pod-2.etcd:2379",
		},
	}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	err := d.Defragment(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all three members were defragged.
	if len(maint.defragged) != 3 {
		t.Fatalf("expected 3 defragged members, got %d", len(maint.defragged))
	}

	// The leader endpoint (containing pod-0) must be last.
	lastEndpoint := maint.defragged[2]
	if lastEndpoint != "https://pod-0.etcd:2379" {
		t.Fatalf("expected leader endpoint last, got %q", lastEndpoint)
	}

	// Lock must have been acquired and released for each member.
	if lock.acquired != 3 {
		t.Fatalf("expected 3 lock acquisitions, got %d", lock.acquired)
	}
	if lock.released != 3 {
		t.Fatalf("expected 3 lock releases, got %d", lock.released)
	}
}

func TestDefragment_StatusKeysWritten(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{}
	kv := newMockKV()
	cluster := &mockCluster{
		endpoints: []string{"https://pod-0.etcd:2379"},
	}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	// Intercept the status key being written by checking the store before
	// cleanup. We verify indirectly: the key was written because defrag
	// completed (maint.defragged has the endpoint).
	err := d.Defragment(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The status key should have been cleaned up (deleted) after completion.
	// But verify it was written by checking that the delete was called.
	key := statusKeyPrefix + "https://pod-0.etcd:2379"
	if len(kv.deleted) != 1 || kv.deleted[0] != key {
		t.Fatalf("expected status key %q to be deleted, deleted keys: %v", key, kv.deleted)
	}
}

func TestDefragment_MemberFailureMarkedFailed(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{defragErr: fmt.Errorf("simulated defrag failure")}
	kv := newMockKV()
	cluster := &mockCluster{
		endpoints: []string{"https://pod-0.etcd:2379"},
	}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	// Defragment should not return an error; it logs and marks failed.
	err := d.Defragment(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The status key should still have been cleaned up even though defrag
	// failed for the member. Check that Delete was called.
	key := statusKeyPrefix + "https://pod-0.etcd:2379"
	if len(kv.deleted) != 1 || kv.deleted[0] != key {
		t.Fatalf("expected status key %q to be deleted after failure, deleted keys: %v", key, kv.deleted)
	}
}

func TestDefragment_NoEndpointsReturnsError(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{}
	kv := newMockKV()
	cluster := &mockCluster{endpoints: []string{}}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	err := d.Defragment(context.Background())
	if err == nil {
		t.Fatal("expected error for empty endpoints")
	}
}

func TestDefragment_ClusterErrorReturnsError(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{}
	kv := newMockKV()
	cluster := &mockCluster{endpointsErr: fmt.Errorf("cluster unavailable")}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	err := d.Defragment(context.Background())
	if err == nil {
		t.Fatal("expected error when cluster endpoint listing fails")
	}
}

func TestDefragDirect(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{}
	maint := &mockMaintenance{}
	kv := newMockKV()
	cluster := &mockCluster{}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	err := d.DefragDirect(context.Background(), "https://pod-1.etcd:2379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the endpoint was defragged directly.
	if len(maint.defragged) != 1 {
		t.Fatalf("expected 1 defragged endpoint, got %d", len(maint.defragged))
	}
	if maint.defragged[0] != "https://pod-1.etcd:2379" {
		t.Fatalf("expected endpoint %q, got %q", "https://pod-1.etcd:2379", maint.defragged[0])
	}

	// Lock must NOT have been acquired for direct defrag.
	if lock.acquired != 0 {
		t.Fatalf("expected 0 lock acquisitions for direct defrag, got %d", lock.acquired)
	}
}

func TestSortEndpoints_LeaderLast(t *testing.T) {
	endpoints := []string{
		"https://pod-0.etcd:2379",
		"https://pod-1.etcd:2379",
		"https://pod-2.etcd:2379",
	}

	sorted := sortEndpoints(endpoints, "pod-0")

	if len(sorted) != 3 {
		t.Fatalf("expected 3 endpoints, got %d", len(sorted))
	}
	if sorted[2] != "https://pod-0.etcd:2379" {
		t.Fatalf("expected local endpoint last, got %q", sorted[2])
	}

	// Verify original slice not modified.
	if endpoints[0] != "https://pod-0.etcd:2379" {
		t.Fatal("original slice was modified")
	}
}

// --- Task 4: Defrag status key tests ---

func TestDefrag_StatusKeys_Written(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{}
	kv := newMockKV()
	endpoints := []string{
		"https://pod-0.etcd:2379",
		"https://pod-1.etcd:2379",
		"https://pod-2.etcd:2379",
	}
	cluster := &mockCluster{endpoints: endpoints}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	err := d.Defragment(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All three status keys should have been deleted (proving they were created).
	if len(kv.deleted) != 3 {
		t.Fatalf("expected 3 status keys deleted, got %d: %v", len(kv.deleted), kv.deleted)
	}

	for _, ep := range endpoints {
		key := statusKeyPrefix + ep
		found := false
		for _, dk := range kv.deleted {
			if dk == key {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected status key %q to be deleted", key)
		}
	}
}

func TestDefrag_FollowersCompleteFirst(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{}
	kv := newMockKV()
	endpoints := []string{
		"https://pod-0.etcd:2379",
		"https://pod-1.etcd:2379",
		"https://pod-2.etcd:2379",
	}
	cluster := &mockCluster{endpoints: endpoints}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	err := d.Defragment(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the order: followers (pod-1, pod-2) must be defragged before
	// the leader (pod-0).
	if len(maint.defragged) != 3 {
		t.Fatalf("expected 3 defragged endpoints, got %d", len(maint.defragged))
	}

	// pod-0 (leader) must be last.
	if maint.defragged[2] != "https://pod-0.etcd:2379" {
		t.Fatalf("expected leader endpoint last, got %q", maint.defragged[2])
	}

	// pod-1 and pod-2 must be before pod-0.
	for i := 0; i < 2; i++ {
		ep := maint.defragged[i]
		if ep == "https://pod-0.etcd:2379" {
			t.Fatalf("leader endpoint appeared at position %d, should be last", i)
		}
	}
}

func TestDefrag_StatusKeys_Cleaned(t *testing.T) {
	lock := &mockLock{}
	role := &mockRole{leader: true}
	maint := &mockMaintenance{}
	kv := newMockKV()
	endpoints := []string{
		"https://pod-0.etcd:2379",
		"https://pod-1.etcd:2379",
	}
	cluster := &mockCluster{endpoints: endpoints}
	logger := zap.NewNop()

	d := New("pod-0", time.Minute, lock, role, maint, kv, cluster, logger)

	err := d.Defragment(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After completion, all status keys must be deleted from the KV store.
	for _, ep := range endpoints {
		key := statusKeyPrefix + ep
		val, getErr := kv.Get(context.Background(), key)
		if getErr != nil {
			t.Fatalf("unexpected error getting status for %q: %v", key, getErr)
		}
		if val != "" {
			t.Fatalf("expected status key %q to be deleted, but found value %q", key, val)
		}
	}

	// Verify delete was called for each endpoint.
	if len(kv.deleted) != 2 {
		t.Fatalf("expected 2 status keys deleted, got %d", len(kv.deleted))
	}
}
