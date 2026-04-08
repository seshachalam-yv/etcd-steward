// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/member"
)

// fakeClient records PatchStatus calls for inspection.
type fakeClient struct {
	updateStatusErr error
	patchStatusErr  error
	patchCalls      atomic.Int64
	lastPatchData   atomic.Value // stores []byte
}

func (f *fakeClient) UpdateStatus(_ context.Context, _, _ string, _ member.UpdateStatusOpts) error {
	return f.updateStatusErr
}
func (f *fakeClient) RemoveCreateAsLearnerAnnotation(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeClient) PatchStatus(_ context.Context, _, _ string, _ types.PatchType, data []byte) error {
	f.patchCalls.Add(1)
	f.lastPatchData.Store(data)
	return f.patchStatusErr
}
func (f *fakeClient) SetCondition(_ context.Context, _, _ string, _ member.Condition) error {
	return nil
}

// staticProvider always returns the same StatusInfo.
type staticProvider struct {
	info member.StatusInfo
}

func (p *staticProvider) ProvideInfo() member.StatusInfo { return p.info }

func ptr[T any](v T) *T { return &v }

func TestMemberStatusReconciler_ZeroProviders(t *testing.T) {
	fc := &fakeClient{}
	r := member.NewStatusReconciler(fc, "etcd-0", "default", 10*time.Millisecond, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_ = r.Run(ctx)

	// With zero providers, PatchStatus is never called (nothing to patch).
	if got := fc.patchCalls.Load(); got != 0 {
		t.Errorf("expected 0 patch calls with no providers, got %d", got)
	}
}

func TestMemberStatusReconciler_OneProvider_AllFields(t *testing.T) {
	dbSize := resource.MustParse("1Gi")
	dbSizeInUse := resource.MustParse("512Mi")
	peerTLS := true
	accDelta := resource.MustParse("200Mi")

	fc := &fakeClient{}
	r := member.NewStatusReconciler(fc, "etcd-0", "default", 10*time.Millisecond, zap.NewNop())
	r.RegisterProvider("all", &staticProvider{
		info: member.StatusInfo{
			DBSize:         &dbSize,
			DBSizeInUse:    &dbSizeInUse,
			PeerTLSEnabled: &peerTLS,
			Snapshots: &member.SnapshotInfo{
				AccumulatedDeltaSize: &accDelta,
			},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)

	if fc.patchCalls.Load() == 0 {
		t.Fatal("expected at least one patch call")
	}
	data, _ := fc.lastPatchData.Load().([]byte)
	patch := string(data)

	for _, want := range []string{"dbSize", "dbSizeInUse", "peerTLSEnabled", "accumulatedDeltaSize"} {
		if !contains(patch, want) {
			t.Errorf("patch missing field %q; patch=%s", want, patch)
		}
	}
}

func TestMemberStatusReconciler_TwoProviders_Merge(t *testing.T) {
	dbSize := resource.MustParse("2Gi")
	accDelta := resource.MustParse("100Mi")

	fc := &fakeClient{}
	r := member.NewStatusReconciler(fc, "etcd-0", "default", 10*time.Millisecond, zap.NewNop())
	r.RegisterProvider("leaderwatch", &staticProvider{
		info: member.StatusInfo{DBSize: &dbSize},
	})
	r.RegisterProvider("snapshotter", &staticProvider{
		info: member.StatusInfo{
			Snapshots: &member.SnapshotInfo{AccumulatedDeltaSize: &accDelta},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)

	if fc.patchCalls.Load() == 0 {
		t.Fatal("expected at least one patch call")
	}
	data, _ := fc.lastPatchData.Load().([]byte)
	patch := string(data)

	// Both providers' fields must appear in one patch.
	if !contains(patch, "dbSize") {
		t.Errorf("patch missing dbSize; patch=%s", patch)
	}
	if !contains(patch, "accumulatedDeltaSize") {
		t.Errorf("patch missing accumulatedDeltaSize; patch=%s", patch)
	}
}

func TestMemberStatusReconciler_ClientError_LoopContinues(t *testing.T) {
	dbSize := resource.MustParse("1Gi")
	fc := &fakeClient{patchStatusErr: errors.New("k8s unavailable")}
	r := member.NewStatusReconciler(fc, "etcd-0", "default", 10*time.Millisecond, zap.NewNop())
	r.RegisterProvider("p", &staticProvider{info: member.StatusInfo{DBSize: &dbSize}})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx) // must not panic or exit early

	// Multiple ticks should have fired despite errors.
	if fc.patchCalls.Load() < 2 {
		t.Errorf("expected loop to continue on error (>=2 calls), got %d", fc.patchCalls.Load())
	}
}

func TestMemberStatusReconciler_CtxCancellation(t *testing.T) {
	fc := &fakeClient{}
	r := member.NewStatusReconciler(fc, "etcd-0", "default", 5*time.Minute, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = r.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// good
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not stop after ctx cancellation within 500ms")
	}
}

// contains checks if substr appears in s (simple string containment).
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
