// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestNewRenewer(t *testing.T) {
	client := fakeclientset.NewSimpleClientset()
	logger := zap.NewNop()

	r := NewRenewer("pod-0", "ns", "etcd-main-member-pod-0", 10*time.Second, client, logger)

	if r == nil {
		t.Fatal("expected non-nil Renewer")
	}
	if r.podName != "pod-0" {
		t.Fatalf("expected podName %q, got %q", "pod-0", r.podName)
	}
	if r.leaseName != "etcd-main-member-pod-0" {
		t.Fatalf("expected leaseName %q, got %q", "etcd-main-member-pod-0", r.leaseName)
	}
	if r.heartbeat != 10*time.Second {
		t.Fatalf("expected heartbeat %v, got %v", 10*time.Second, r.heartbeat)
	}
}

func TestRenew_CreatesLeaseIfNotExists(t *testing.T) {
	client := fakeclientset.NewSimpleClientset()
	logger := zap.NewNop()

	r := NewRenewer("pod-0", "ns", "etcd-main-member-pod-0", 10*time.Second, client, logger)

	r.renew(context.Background())

	// Verify the lease was created.
	lease, err := client.CoordinationV1().Leases("ns").Get(
		context.Background(), "etcd-main-member-pod-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected lease to be created, got error: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "pod-0" {
		t.Fatalf("expected holder identity %q, got %v", "pod-0", lease.Spec.HolderIdentity)
	}
	if lease.Spec.RenewTime == nil {
		t.Fatal("expected non-nil renewTime")
	}
}

func TestRenew_UpdatesExistingLease(t *testing.T) {
	// Pre-create the lease.
	oldTime := metav1.NewMicroTime(time.Now().Add(-time.Minute))
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd-main-member-pod-0",
			Namespace: "ns",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: strPtr("pod-0"),
			RenewTime:      &oldTime,
		},
	}
	client := fakeclientset.NewSimpleClientset(existing)
	logger := zap.NewNop()

	r := NewRenewer("pod-0", "ns", "etcd-main-member-pod-0", 10*time.Second, client, logger)

	r.renew(context.Background())

	// Verify the lease was updated (renewTime should be more recent).
	lease, err := client.CoordinationV1().Leases("ns").Get(
		context.Background(), "etcd-main-member-pod-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lease.Spec.RenewTime == nil {
		t.Fatal("expected non-nil renewTime after update")
	}
	if !lease.Spec.RenewTime.Time.After(oldTime.Time) {
		t.Fatal("expected renewTime to be updated to a more recent time")
	}
}

func TestRenew_HandlesUpdateError(t *testing.T) {
	// Pre-create the lease.
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd-main-member-pod-0",
			Namespace: "ns",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: strPtr("pod-0"),
		},
	}
	client := fakeclientset.NewSimpleClientset(existing)

	// Inject an update failure.
	client.PrependReactor("update", "leases", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})
	logger := zap.NewNop()

	r := NewRenewer("pod-0", "ns", "etcd-main-member-pod-0", 10*time.Second, client, logger)

	// Should not panic; just log the error.
	r.renew(context.Background())
}

func TestRun_CancelsOnContextDone(t *testing.T) {
	client := fakeclientset.NewSimpleClientset()
	logger := zap.NewNop()

	r := NewRenewer("pod-0", "ns", "etcd-main-member-pod-0", 50*time.Millisecond, client, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	// Let at least one heartbeat cycle complete.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	// Verify at least one lease was created by the initial renew call.
	lease, err := client.CoordinationV1().Leases("ns").Get(
		context.Background(), "etcd-main-member-pod-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected lease to exist after Run, got error: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "pod-0" {
		t.Fatalf("expected holder identity %q", "pod-0")
	}
}
