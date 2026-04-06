// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotlease

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// mockSnapstore implements snapstore.Snapstore for testing.
type mockSnapstore struct {
	snaps []snapstore.Snapshot
	err   error
}

func (m *mockSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (m *mockSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockSnapstore) List() ([]snapstore.Snapshot, error) {
	return m.snaps, m.err
}
func (m *mockSnapstore) Delete(_ snapstore.Snapshot) error { return nil }

func TestRenewer_Update_CreatesLeases(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", StartRevision: 0, LastRevision: 100, CreatedOn: time.Unix(0, 100)},
			{Kind: "Incremental", StartRevision: 100, LastRevision: 200, CreatedOn: time.Unix(0, 200)},
		},
	}

	fakeClient := fake.NewSimpleClientset()
	r := New("etcd-main", "default", store, fakeClient.CoordinationV1(), time.Minute, zap.NewNop())

	if err := r.update(context.Background()); err != nil {
		t.Fatalf("update error: %v", err)
	}

	// Verify leases were created.
	fullLease, err := fakeClient.CoordinationV1().Leases("default").Get(context.Background(), "etcd-main-full-snapshot", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get full-snapshot lease: %v", err)
	}
	if *fullLease.Spec.HolderIdentity != "100" {
		t.Errorf("full-snapshot holderIdentity = %q, want %q", *fullLease.Spec.HolderIdentity, "100")
	}

	deltaLease, err := fakeClient.CoordinationV1().Leases("default").Get(context.Background(), "etcd-main-delta-snapshot", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get delta-snapshot lease: %v", err)
	}
	if *deltaLease.Spec.HolderIdentity != "200" {
		t.Errorf("delta-snapshot holderIdentity = %q, want %q", *deltaLease.Spec.HolderIdentity, "200")
	}
}

func TestRenewer_Update_EmptyStore(t *testing.T) {
	store := &mockSnapstore{snaps: nil}
	fakeClient := fake.NewSimpleClientset()
	r := New("etcd-main", "default", store, fakeClient.CoordinationV1(), time.Minute, zap.NewNop())

	if err := r.update(context.Background()); err != nil {
		t.Fatalf("expected no error for empty store, got: %v", err)
	}
}

func TestRenewer_Update_StoreError(t *testing.T) {
	store := &mockSnapstore{err: fmt.Errorf("connection refused")}
	fakeClient := fake.NewSimpleClientset()
	r := New("etcd-main", "default", store, fakeClient.CoordinationV1(), time.Minute, zap.NewNop())

	err := r.update(context.Background())
	if err == nil {
		t.Fatal("expected error from store.List, got nil")
	}
}

func TestRenewer_Update_OnlyFullSnapshots(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", StartRevision: 0, LastRevision: 50, CreatedOn: time.Unix(0, 50)},
			{Kind: "Full", StartRevision: 0, LastRevision: 150, CreatedOn: time.Unix(0, 150)},
		},
	}
	fakeClient := fake.NewSimpleClientset()
	r := New("etcd-main", "default", store, fakeClient.CoordinationV1(), time.Minute, zap.NewNop())

	if err := r.update(context.Background()); err != nil {
		t.Fatalf("update error: %v", err)
	}

	// Verify full-snapshot lease exists with latest revision.
	fullLease, err := fakeClient.CoordinationV1().Leases("default").Get(context.Background(), "etcd-main-full-snapshot", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get full-snapshot lease: %v", err)
	}
	if *fullLease.Spec.HolderIdentity != "150" {
		t.Errorf("full-snapshot holderIdentity = %q, want %q", *fullLease.Spec.HolderIdentity, "150")
	}
}

func TestRenewer_Update_UpdatesExistingLease(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", StartRevision: 0, LastRevision: 100, CreatedOn: time.Unix(0, 100)},
		},
	}

	fakeClient := fake.NewSimpleClientset()
	r := New("etcd-main", "default", store, fakeClient.CoordinationV1(), time.Minute, zap.NewNop())

	// First update -- creates the lease.
	if err := r.update(context.Background()); err != nil {
		t.Fatalf("first update error: %v", err)
	}

	// Update store with newer snapshot.
	store.snaps = []snapstore.Snapshot{
		{Kind: "Full", StartRevision: 0, LastRevision: 200, CreatedOn: time.Unix(0, 200)},
	}

	// Second update -- updates the existing lease.
	if err := r.update(context.Background()); err != nil {
		t.Fatalf("second update error: %v", err)
	}

	fullLease, err := fakeClient.CoordinationV1().Leases("default").Get(context.Background(), "etcd-main-full-snapshot", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get updated lease: %v", err)
	}
	if *fullLease.Spec.HolderIdentity != "200" {
		t.Errorf("holderIdentity = %q, want %q", *fullLease.Spec.HolderIdentity, "200")
	}
}
