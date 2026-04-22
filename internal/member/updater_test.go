// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
)

// --- Mock implementations ---

type mockInfoProvider struct {
	info MemberInfo
	err  error
}

func (m *mockInfoProvider) MemberInfo(_ context.Context) (MemberInfo, error) {
	return m.info, m.err
}

type mockStateRecorder struct {
	recorded []MemberInfo
	err      error
}

func (m *mockStateRecorder) RecordMemberState(_ context.Context, _, _ string, info MemberInfo) error {
	if m.err != nil {
		return m.err
	}
	m.recorded = append(m.recorded, info)
	return nil
}

type mockStatusClient struct {
	resp StatusResponse
	err  error
}

func (m *mockStatusClient) Status(_ context.Context, _ string) (StatusResponse, error) {
	return m.resp, m.err
}

// --- Updater tests ---

func TestNewUpdater(t *testing.T) {
	rec := &mockStateRecorder{}
	logger := zap.NewNop()
	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)

	if u == nil {
		t.Fatal("expected non-nil Updater")
	}
	if u.podName != "pod-0" {
		t.Fatalf("expected podName %q, got %q", "pod-0", u.podName)
	}
	if u.namespace != "ns" {
		t.Fatalf("expected namespace %q, got %q", "ns", u.namespace)
	}
}

func TestRecordStateTransition_Success(t *testing.T) {
	info := MemberInfo{
		ID:          1,
		Name:        "pod-0",
		Role:        "Leader",
		DBSize:      1024,
		DBSizeInUse: 512,
		IsHealthy:   true,
	}
	provider := &mockInfoProvider{info: info}
	rec := &mockStateRecorder{}
	logger := zap.NewNop()

	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)
	u.RegisterInfoProvider(provider)

	err := u.RecordStateTransition(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rec.recorded) != 1 {
		t.Fatalf("expected 1 recorded state, got %d", len(rec.recorded))
	}
	if rec.recorded[0].ID != 1 {
		t.Fatalf("expected ID 1, got %d", rec.recorded[0].ID)
	}
	if rec.recorded[0].Role != "Leader" {
		t.Fatalf("expected role %q, got %q", "Leader", rec.recorded[0].Role)
	}
}

func TestRecordStateTransition_NoProviders(t *testing.T) {
	rec := &mockStateRecorder{}
	logger := zap.NewNop()

	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)

	err := u.RecordStateTransition(context.Background())
	if err == nil {
		t.Fatal("expected error when no providers registered")
	}
}

func TestRecordStateTransition_ProviderError_FallsThrough(t *testing.T) {
	failProvider := &mockInfoProvider{err: fmt.Errorf("provider1 failed")}
	goodProvider := &mockInfoProvider{info: MemberInfo{ID: 2, Role: "Follower"}}
	rec := &mockStateRecorder{}
	logger := zap.NewNop()

	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)
	u.RegisterInfoProvider(failProvider)
	u.RegisterInfoProvider(goodProvider)

	err := u.RecordStateTransition(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rec.recorded) != 1 {
		t.Fatalf("expected 1 recorded state, got %d", len(rec.recorded))
	}
	if rec.recorded[0].ID != 2 {
		t.Fatalf("expected ID 2 from fallback provider, got %d", rec.recorded[0].ID)
	}
}

func TestRecordStateTransition_RecorderError(t *testing.T) {
	provider := &mockInfoProvider{info: MemberInfo{ID: 1, Role: "Follower"}}
	rec := &mockStateRecorder{err: fmt.Errorf("recorder failed")}
	logger := zap.NewNop()

	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)
	u.RegisterInfoProvider(provider)

	err := u.RecordStateTransition(context.Background())
	if err == nil {
		t.Fatal("expected error when recorder fails")
	}
}

func TestLastInfo_ReturnsNilBeforeFirstUpdate(t *testing.T) {
	rec := &mockStateRecorder{}
	logger := zap.NewNop()
	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)

	if u.LastInfo() != nil {
		t.Fatal("expected nil LastInfo before any update")
	}
}

func TestLastInfo_ReturnsCopyAfterUpdate(t *testing.T) {
	info := MemberInfo{ID: 1, Role: "Leader", DBSize: 2048}
	provider := &mockInfoProvider{info: info}
	rec := &mockStateRecorder{}
	logger := zap.NewNop()

	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)
	u.RegisterInfoProvider(provider)

	err := u.RecordStateTransition(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	last := u.LastInfo()
	if last == nil {
		t.Fatal("expected non-nil LastInfo after update")
	}
	if last.ID != 1 {
		t.Fatalf("expected ID 1, got %d", last.ID)
	}
	if last.DBSize != 2048 {
		t.Fatalf("expected DBSize 2048, got %d", last.DBSize)
	}

	// Verify it's a copy by modifying the returned value.
	last.ID = 999
	again := u.LastInfo()
	if again.ID != 1 {
		t.Fatal("LastInfo must return a copy, not a reference")
	}
}

// --- Supplementary provider tests ---

type mockSupplementaryProvider struct {
	id   string
	data map[string]interface{}
	err  error
}

func (m *mockSupplementaryProvider) ID() string { return m.id }

func (m *mockSupplementaryProvider) GetInfo(_ context.Context) (map[string]interface{}, error) {
	return m.data, m.err
}

func TestRecordStateTransition_WithSupplementaryProvider(t *testing.T) {
	info := MemberInfo{ID: 1, Role: "Leader", DBSize: 1024}
	provider := &mockInfoProvider{info: info}
	suppProvider := &mockSupplementaryProvider{
		id: "snapshot-info",
		data: map[string]interface{}{
			"lastFullSnapshotRevision":  int64(100),
			"lastDeltaSnapshotRevision": int64(200),
			"totalSnapshotCount":        3,
			"accumulatedDeltaSize":      int64(5000),
		},
	}
	rec := &mockStateRecorder{}
	logger := zap.NewNop()

	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)
	u.RegisterInfoProvider(provider)
	u.RegisterSupplementaryProvider(suppProvider)

	err := u.RecordStateTransition(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rec.recorded) != 1 {
		t.Fatalf("expected 1 recorded state, got %d", len(rec.recorded))
	}
	recorded := rec.recorded[0]
	if recorded.SupplementaryInfo == nil {
		t.Fatal("expected non-nil SupplementaryInfo")
	}
	snapInfo, ok := recorded.SupplementaryInfo["snapshot-info"]
	if !ok {
		t.Fatal("expected snapshot-info key in SupplementaryInfo")
	}
	if v, ok := snapInfo["lastFullSnapshotRevision"].(int64); !ok || v != 100 {
		t.Fatalf("expected lastFullSnapshotRevision=100, got %v", snapInfo["lastFullSnapshotRevision"])
	}
}

func TestRecordStateTransition_SupplementaryProviderError_Skipped(t *testing.T) {
	info := MemberInfo{ID: 1, Role: "Follower"}
	provider := &mockInfoProvider{info: info}
	failSuppProvider := &mockSupplementaryProvider{
		id:  "failing-provider",
		err: fmt.Errorf("snap list failed"),
	}
	rec := &mockStateRecorder{}
	logger := zap.NewNop()

	u := NewUpdater("pod-0", "ns", time.Second, rec, logger)
	u.RegisterInfoProvider(provider)
	u.RegisterSupplementaryProvider(failSuppProvider)

	err := u.RecordStateTransition(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rec.recorded) != 1 {
		t.Fatalf("expected 1 recorded state, got %d", len(rec.recorded))
	}
	// SupplementaryInfo should be initialised but empty since the provider failed.
	recorded := rec.recorded[0]
	if recorded.SupplementaryInfo == nil {
		t.Fatal("expected non-nil SupplementaryInfo map")
	}
	if _, ok := recorded.SupplementaryInfo["failing-provider"]; ok {
		t.Fatal("expected failing provider to be absent from SupplementaryInfo")
	}
}

// --- InfoProvider tests ---

func TestMaintenanceStatusProvider_Leader(t *testing.T) {
	sc := &mockStatusClient{
		resp: StatusResponse{
			MemberID:    100,
			Leader:      100, // same as MemberID => leader
			DBSize:      4096,
			DBSizeInUse: 2048,
			IsLearner:   false,
		},
	}

	p := NewMaintenanceStatusProvider("pod-0", "https://pod-0:2379", sc)

	info, err := p.MemberInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Role != "Leader" {
		t.Fatalf("expected role %q, got %q", "Leader", info.Role)
	}
	if info.ID != 100 {
		t.Fatalf("expected ID 100, got %d", info.ID)
	}
	if info.DBSize != 4096 {
		t.Fatalf("expected DBSize 4096, got %d", info.DBSize)
	}
}

func TestMaintenanceStatusProvider_Follower(t *testing.T) {
	sc := &mockStatusClient{
		resp: StatusResponse{
			MemberID:    100,
			Leader:      200, // different => follower
			DBSize:      4096,
			DBSizeInUse: 2048,
			IsLearner:   false,
		},
	}

	p := NewMaintenanceStatusProvider("pod-1", "https://pod-1:2379", sc)

	info, err := p.MemberInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Role != "Follower" {
		t.Fatalf("expected role %q, got %q", "Follower", info.Role)
	}
}

func TestMaintenanceStatusProvider_Learner(t *testing.T) {
	sc := &mockStatusClient{
		resp: StatusResponse{
			MemberID:  100,
			Leader:    200,
			IsLearner: true,
		},
	}

	p := NewMaintenanceStatusProvider("pod-2", "https://pod-2:2379", sc)

	info, err := p.MemberInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Role != "Learner" {
		t.Fatalf("expected role %q, got %q", "Learner", info.Role)
	}
}

func TestMaintenanceStatusProvider_Error(t *testing.T) {
	sc := &mockStatusClient{err: fmt.Errorf("connection refused")}

	p := NewMaintenanceStatusProvider("pod-0", "https://pod-0:2379", sc)

	_, err := p.MemberInfo(context.Background())
	if err == nil {
		t.Fatal("expected error when status client fails")
	}
}
