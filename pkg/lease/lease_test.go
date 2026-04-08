// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	applyconfigurationscoordinationv1 "k8s.io/client-go/applyconfigurations/coordination/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"go.uber.org/zap"
)

// mockLeasesGetter implements coordinationv1client.LeasesGetter for testing.
type mockLeasesGetter struct {
	leaseClient *mockLeaseClient
}

func (m *mockLeasesGetter) Leases(_ string) coordinationv1client.LeaseInterface {
	return m.leaseClient
}

// mockLeaseClient implements coordinationv1client.LeaseInterface for testing.
type mockLeaseClient struct {
	lease       *coordinationv1.Lease
	createCalls int
	updateCalls int
	createError error
	updateError error
	getError    error
}

func (m *mockLeaseClient) Get(_ context.Context, name string, _ metav1.GetOptions) (*coordinationv1.Lease, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	if m.lease == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, name)
	}
	return m.lease.DeepCopy(), nil
}

func (m *mockLeaseClient) Create(_ context.Context, lease *coordinationv1.Lease, _ metav1.CreateOptions) (*coordinationv1.Lease, error) {
	m.createCalls++
	if m.createError != nil {
		return nil, m.createError
	}
	m.lease = lease.DeepCopy()
	return m.lease.DeepCopy(), nil
}

func (m *mockLeaseClient) Update(_ context.Context, lease *coordinationv1.Lease, _ metav1.UpdateOptions) (*coordinationv1.Lease, error) {
	m.updateCalls++
	if m.updateError != nil {
		return nil, m.updateError
	}
	m.lease = lease.DeepCopy()
	return m.lease.DeepCopy(), nil
}

func (m *mockLeaseClient) Delete(_ context.Context, _ string, _ metav1.DeleteOptions) error {
	panic("not implemented")
}

func (m *mockLeaseClient) DeleteCollection(_ context.Context, _ metav1.DeleteOptions, _ metav1.ListOptions) error {
	panic("not implemented")
}

func (m *mockLeaseClient) List(_ context.Context, _ metav1.ListOptions) (*coordinationv1.LeaseList, error) {
	panic("not implemented")
}

func (m *mockLeaseClient) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	panic("not implemented")
}

func (m *mockLeaseClient) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, _ metav1.PatchOptions, _ ...string) (*coordinationv1.Lease, error) {
	panic("not implemented")
}

func (m *mockLeaseClient) Apply(_ context.Context, _ *applyconfigurationscoordinationv1.LeaseApplyConfiguration, _ metav1.ApplyOptions) (*coordinationv1.Lease, error) {
	panic("not implemented")
}

func constState(memberID, clusterID, role string) StateFunc {
	return func() (string, string, string) {
		return memberID, clusterID, role
	}
}

func newTestRenewer(client *mockLeaseClient, stateFunc StateFunc) *Renewer {
	logger, _ := zap.NewDevelopment()
	return &Renewer{
		leaseName:         "etcd-main-0",
		namespace:         "default",
		heartbeatInterval: 10 * time.Second,
		leaseDuration:     30,
		client:            &mockLeasesGetter{leaseClient: client},
		stateFunc:         stateFunc,
		peerTLSEnabled:    false,
		logger:            logger,
	}
}

func TestRenewCreatesLeaseWhenNotFound(t *testing.T) {
	client := &mockLeaseClient{}
	r := newTestRenewer(client, constState("8e9e05c52164694d", "abc123def456", "Leader"))

	err := r.renew(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.createCalls != 1 {
		t.Errorf("expected 1 create call, got %d", client.createCalls)
	}
	if client.updateCalls != 0 {
		t.Errorf("expected 0 update calls, got %d", client.updateCalls)
	}
	if client.lease == nil {
		t.Fatal("expected lease to be created")
	}
	if client.lease.Name != "etcd-main-0" {
		t.Errorf("expected lease name etcd-main-0, got %s", client.lease.Name)
	}
	if client.lease.Spec.HolderIdentity == nil || *client.lease.Spec.HolderIdentity != "8e9e05c52164694d:abc123def456:Leader" {
		t.Errorf("unexpected holderIdentity: %v", client.lease.Spec.HolderIdentity)
	}
}

func TestRenewUpdatesExistingLease(t *testing.T) {
	oldIdentity := "old:old:Member"
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd-main-0",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &oldIdentity,
		},
	}
	client := &mockLeaseClient{lease: existing}
	r := newTestRenewer(client, constState("8e9e05c52164694d", "abc123def456", "Leader"))

	before := time.Now()
	err := r.renew(context.Background())
	after := time.Now()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.updateCalls != 1 {
		t.Errorf("expected 1 update call, got %d", client.updateCalls)
	}
	if client.createCalls != 0 {
		t.Errorf("expected 0 create calls, got %d", client.createCalls)
	}

	if client.lease.Spec.HolderIdentity == nil || *client.lease.Spec.HolderIdentity != "8e9e05c52164694d:abc123def456:Leader" {
		t.Errorf("unexpected holderIdentity: %v", client.lease.Spec.HolderIdentity)
	}

	if client.lease.Spec.RenewTime == nil {
		t.Fatal("expected renewTime to be set")
	}
	renewTime := client.lease.Spec.RenewTime.Time
	if renewTime.Before(before) || renewTime.After(after) {
		t.Errorf("renewTime %v not in range [%v, %v]", renewTime, before, after)
	}
}

func TestHolderIdentityFormat(t *testing.T) {
	tests := []struct {
		name     string
		memberID string
		clusterID string
		role     string
		expected string
	}{
		{
			name:     "leader",
			memberID: "8e9e05c52164694d",
			clusterID: "abc123def456",
			role:     "Leader",
			expected: "8e9e05c52164694d:abc123def456:Leader",
		},
		{
			name:     "member",
			memberID: "deadbeef01234567",
			clusterID: "cafe0000babe0001",
			role:     "Member",
			expected: "deadbeef01234567:cafe0000babe0001:Member",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockLeaseClient{}
			r := newTestRenewer(client, constState(tc.memberID, tc.clusterID, tc.role))

			if err := r.renew(context.Background()); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			identity := *client.lease.Spec.HolderIdentity
			parts := strings.Split(identity, ":")
			if len(parts) != 3 {
				t.Fatalf("expected 3 parts in holderIdentity, got %d", len(parts))
			}
			if identity != tc.expected {
				t.Errorf("holderIdentity = %q, want %q", identity, tc.expected)
			}
		})
	}
}

func TestRenewCachesIDsAndAvoidsMemberOnlyIdentity(t *testing.T) {
	// Simulate the real scenario:
	// - First call: memberID and clusterID are empty (etcd not yet ready)
	// - Second call: memberID and clusterID populated (etcd is up)
	// - Third call: empty again (transient etcd unreachable)
	// Expected: the lease always uses the last valid IDs, never "::Member" after valid IDs seen.
	callCount := 0
	stateSeq := []struct{ memberID, clusterID, role string }{
		{"", "", "Member"},                   // etcd not ready yet
		{"8e9e05c52164694d", "abc123", "Leader"}, // etcd up, became leader
		{"", "", "Leader"},                   // transient unreachable
	}
	stateFunc := func() (string, string, string) {
		idx := callCount
		if idx >= len(stateSeq) {
			idx = len(stateSeq) - 1
		}
		callCount++
		return stateSeq[idx].memberID, stateSeq[idx].clusterID, stateSeq[idx].role
	}

	client := &mockLeaseClient{}
	r := newTestRenewer(client, stateFunc)

	// First renewal: empty IDs — should still write "::Member" (no cached value yet)
	if err := r.renew(context.Background()); err != nil {
		t.Fatalf("unexpected error on first renew: %v", err)
	}
	if client.lease != nil && client.lease.Spec.HolderIdentity != nil {
		identity := *client.lease.Spec.HolderIdentity
		if identity != "::Member" {
			t.Errorf("first renew: expected '::Member' before IDs known, got %q", identity)
		}
	}

	// Second renewal: valid IDs — should cache and use them
	if err := r.renew(context.Background()); err != nil {
		t.Fatalf("unexpected error on second renew: %v", err)
	}
	if client.lease == nil || client.lease.Spec.HolderIdentity == nil {
		t.Fatal("expected lease to exist after second renew")
	}
	if *client.lease.Spec.HolderIdentity != "8e9e05c52164694d:abc123:Leader" {
		t.Errorf("second renew: expected '8e9e05c52164694d:abc123:Leader', got %q", *client.lease.Spec.HolderIdentity)
	}

	// Third renewal: empty IDs again — should use cached values
	if err := r.renew(context.Background()); err != nil {
		t.Fatalf("unexpected error on third renew: %v", err)
	}
	if *client.lease.Spec.HolderIdentity != "8e9e05c52164694d:abc123:Leader" {
		t.Errorf("third renew (transient empty): expected cached '8e9e05c52164694d:abc123:Leader', got %q",
			*client.lease.Spec.HolderIdentity)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	client := &mockLeaseClient{}
	logger, _ := zap.NewDevelopment()
	r := &Renewer{
		leaseName:         "etcd-main-0",
		namespace:         "default",
		heartbeatInterval: 1 * time.Hour,
		leaseDuration:     3600,
		client:            &mockLeasesGetter{leaseClient: client},
		stateFunc:         constState("aaa", "bbb", "Leader"),
		peerTLSEnabled:    false,
		logger:            logger,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation within 2s")
	}
}

func TestRenewSetsPeerTLSAnnotation(t *testing.T) {
	tests := []struct {
		name           string
		peerTLSEnabled bool
		wantAnnotation string
	}{
		{
			name:           "TLS disabled",
			peerTLSEnabled: false,
			wantAnnotation: "false",
		},
		{
			name:           "TLS enabled",
			peerTLSEnabled: true,
			wantAnnotation: "true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockLeaseClient{}
			logger, _ := zap.NewDevelopment()
			r := &Renewer{
				leaseName:         "etcd-main-0",
				namespace:         "default",
				heartbeatInterval: 10 * time.Second,
				leaseDuration:     30,
				client:            &mockLeasesGetter{leaseClient: client},
				stateFunc:         constState("aaa", "bbb", "Leader"),
				peerTLSEnabled:    tc.peerTLSEnabled,
				logger:            logger,
			}

			if err := r.renew(context.Background()); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if client.lease == nil {
				t.Fatal("expected lease to be created")
			}
			got := client.lease.Annotations[LeaseAnnotationKeyPeerURLTLSEnabled]
			if got != tc.wantAnnotation {
				t.Errorf("annotation %q = %q, want %q",
					LeaseAnnotationKeyPeerURLTLSEnabled, got, tc.wantAnnotation)
			}
		})
	}
}

func TestRenewPreservesPeerTLSAnnotationOnUpdate(t *testing.T) {
	// Existing lease has no annotation; after renew it must have the correct annotation.
	oldIdentity := "old:old:Member"
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd-main-0",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &oldIdentity,
		},
	}
	client := &mockLeaseClient{lease: existing}
	logger, _ := zap.NewDevelopment()
	r := &Renewer{
		leaseName:         "etcd-main-0",
		namespace:         "default",
		heartbeatInterval: 10 * time.Second,
		leaseDuration:     30,
		client:            &mockLeasesGetter{leaseClient: client},
		stateFunc:         constState("aaa", "bbb", "Leader"),
		peerTLSEnabled:    true,
		logger:            logger,
	}

	if err := r.renew(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := client.lease.Annotations[LeaseAnnotationKeyPeerURLTLSEnabled]
	if got != "true" {
		t.Errorf("expected annotation to be 'true' after update, got %q", got)
	}
}
