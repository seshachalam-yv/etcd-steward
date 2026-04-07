// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package alarm

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// fakeMaintenance implements MaintenanceAPI for testing.
type fakeMaintenance struct {
	alarms        []*pb.AlarmMember
	statusRev     int64
	defragCalled  bool
	disarmed      []*clientv3.AlarmMember
	alarmListErr  error
	statusErr     error
	defragErr     error
	alarmDisarmErr error
}

func (f *fakeMaintenance) AlarmList(_ context.Context) (*clientv3.AlarmResponse, error) {
	if f.alarmListErr != nil {
		return nil, f.alarmListErr
	}
	return (*clientv3.AlarmResponse)(&pb.AlarmResponse{Alarms: f.alarms}), nil
}

func (f *fakeMaintenance) AlarmDisarm(_ context.Context, m *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	if f.alarmDisarmErr != nil {
		return nil, f.alarmDisarmErr
	}
	f.disarmed = append(f.disarmed, m)
	return (*clientv3.AlarmResponse)(&pb.AlarmResponse{}), nil
}

func (f *fakeMaintenance) Defragment(_ context.Context, _ string) (*clientv3.DefragmentResponse, error) {
	f.defragCalled = true
	if f.defragErr != nil {
		return nil, f.defragErr
	}
	return (*clientv3.DefragmentResponse)(&pb.DefragmentResponse{}), nil
}

func (f *fakeMaintenance) Status(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	return (*clientv3.StatusResponse)(&pb.StatusResponse{
		Header: &pb.ResponseHeader{Revision: f.statusRev},
	}), nil
}

// fakeKV implements KVCompactAPI for testing.
type fakeKV struct {
	compactedRev int64
	compactErr   error
}

func (f *fakeKV) Compact(_ context.Context, rev int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	if f.compactErr != nil {
		return nil, f.compactErr
	}
	f.compactedRev = rev
	return &clientv3.CompactResponse{}, nil
}

func TestHandler_NoAlarms(t *testing.T) {
	m := &fakeMaintenance{alarms: nil, statusRev: 5000}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	if err := h.check(context.Background()); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if m.defragCalled {
		t.Error("defrag should not be called when no alarms")
	}
}

func TestHandler_NOSPACE_Remediation(t *testing.T) {
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{nospaceAlarm},
		statusRev: 5000,
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	if err := h.check(context.Background()); err != nil {
		t.Fatalf("check error: %v", err)
	}

	// Verify compact was called with currentRevision - compactRevisionLag.
	expectedCompactRev := int64(4000)
	if kv.compactedRev != expectedCompactRev {
		t.Errorf("compact revision = %d, want %d", kv.compactedRev, expectedCompactRev)
	}

	if !m.defragCalled {
		t.Error("expected defragment to be called")
	}

	if len(m.disarmed) != 1 {
		t.Fatalf("expected 1 disarmed alarm, got %d", len(m.disarmed))
	}
	if m.disarmed[0].MemberID != 1 {
		t.Errorf("disarmed member ID = %d, want 1", m.disarmed[0].MemberID)
	}
}

func TestHandler_CORRUPT_NoDisarm(t *testing.T) {
	corruptAlarm := &pb.AlarmMember{MemberID: 2, Alarm: pb.AlarmType_CORRUPT}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{corruptAlarm},
		statusRev: 5000,
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	if err := h.check(context.Background()); err != nil {
		t.Fatalf("check error: %v", err)
	}

	if len(m.disarmed) != 0 {
		t.Error("CORRUPT alarm should not be disarmed")
	}
	if m.defragCalled {
		t.Error("defrag should not be called for CORRUPT alarm")
	}
}

func TestHandler_NOSPACE_LowRevision(t *testing.T) {
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{nospaceAlarm},
		statusRev: 500, // Less than compactRevisionLag.
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	if err := h.check(context.Background()); err != nil {
		t.Fatalf("check error: %v", err)
	}

	// compactRev = 500 - 1000 = -500, clamped to 1.
	if kv.compactedRev != 1 {
		t.Errorf("compact revision = %d, want 1 (clamped)", kv.compactedRev)
	}
}

func TestHandler_Mixed_NOSPACE_And_CORRUPT(t *testing.T) {
	alarms := []*pb.AlarmMember{
		{MemberID: 1, Alarm: pb.AlarmType_NOSPACE},
		{MemberID: 2, Alarm: pb.AlarmType_CORRUPT},
	}
	m := &fakeMaintenance{
		alarms:    alarms,
		statusRev: 5000,
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	if err := h.check(context.Background()); err != nil {
		t.Fatalf("check error: %v", err)
	}

	// NOSPACE should be remediated (compact + defrag + disarm).
	if !m.defragCalled {
		t.Error("expected defragment for NOSPACE")
	}
	// Only NOSPACE should be disarmed, not CORRUPT.
	if len(m.disarmed) != 1 {
		t.Fatalf("expected 1 disarmed alarm, got %d", len(m.disarmed))
	}
	if m.disarmed[0].Alarm != pb.AlarmType_NOSPACE {
		t.Errorf("expected NOSPACE alarm disarmed, got %v", m.disarmed[0].Alarm)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — check: alarm list error
// ---------------------------------------------------------------------------

func TestHandler_Check_AlarmListError(t *testing.T) {
	m := &fakeMaintenance{
		alarmListErr: fmt.Errorf("etcd alarm list failed"),
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	err := h.check(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Additional tests — check: cancelled context (AlarmList sees ctx.Done)
// ---------------------------------------------------------------------------

func TestHandler_Check_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before check runs

	m := &fakeMaintenance{alarmListErr: ctx.Err()}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	err := h.check(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// Additional tests — remediateNOSPACE error paths
// ---------------------------------------------------------------------------

func TestHandler_RemediateNOSPACE_StatusError(t *testing.T) {
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{nospaceAlarm},
		statusErr: fmt.Errorf("status unavailable"),
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	err := h.check(context.Background())
	if err == nil {
		t.Fatal("expected error when status fails, got nil")
	}
}

func TestHandler_RemediateNOSPACE_CompactError(t *testing.T) {
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{nospaceAlarm},
		statusRev: 5000,
	}
	kv := &fakeKV{compactErr: fmt.Errorf("compact failed")}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	err := h.check(context.Background())
	if err == nil {
		t.Fatal("expected error when compact fails, got nil")
	}
	if m.defragCalled {
		t.Error("defrag should not be called after compact error")
	}
}

func TestHandler_RemediateNOSPACE_DefragmentError(t *testing.T) {
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{nospaceAlarm},
		statusRev: 5000,
		defragErr: fmt.Errorf("defrag failed"),
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	err := h.check(context.Background())
	if err == nil {
		t.Fatal("expected error when defragment fails, got nil")
	}
	if len(m.disarmed) != 0 {
		t.Error("alarm should not be disarmed after defrag error")
	}
}

func TestHandler_RemediateNOSPACE_AlarmDisarmError(t *testing.T) {
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:         []*pb.AlarmMember{nospaceAlarm},
		statusRev:      5000,
		alarmDisarmErr: fmt.Errorf("disarm failed"),
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	err := h.check(context.Background())
	if err == nil {
		t.Fatal("expected error when alarm disarm fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// Additional tests — remediateNOSPACE: compactRev clamped when currentRev == compactRevisionLag
// ---------------------------------------------------------------------------

func TestHandler_NOSPACE_ExactLagBoundary(t *testing.T) {
	// currentRev == compactRevisionLag → compactRev = 0, clamped to 1.
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{nospaceAlarm},
		statusRev: 1000, // exactly equal to lag
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	if err := h.check(context.Background()); err != nil {
		t.Fatalf("check error: %v", err)
	}
	// compactRev = 1000 - 1000 = 0, clamped to 1.
	if kv.compactedRev != 1 {
		t.Errorf("compact revision = %d, want 1 (clamped from 0)", kv.compactedRev)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — Run: exits when ctx is cancelled after a tick
// ---------------------------------------------------------------------------

func TestHandler_Run_ExitsOnContextCancel(t *testing.T) {
	// Use a very short ticker interval so Run calls check at least once before we cancel.
	m := &fakeMaintenance{alarms: nil, statusRev: 0}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Millisecond, "default", "etcd-main", zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		h.Run(ctx)
		close(done)
	}()

	// Give Run a chance to tick at least once.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Run returned as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation within 2s")
	}
}

func TestHandler_Run_CallsCheckOnTick(t *testing.T) {
	// Verify that Run invokes check on each tick by observing a NOSPACE remediation.
	nospaceAlarm := &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{nospaceAlarm},
		statusRev: 5000,
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, 5*time.Millisecond, "default", "etcd-main", zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.Run(ctx)
		close(done)
	}()

	// Wait until defrag is called (remediation happened), then cancel.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("defrag was not called within 2s — Run did not invoke check on tick")
		default:
			if m.defragCalled {
				cancel()
				<-done
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------------
// Additional tests — unknown alarm type (default branch in check)
// ---------------------------------------------------------------------------

func TestHandler_UnknownAlarmType(t *testing.T) {
	// Use an alarm type that is neither NOSPACE nor CORRUPT to hit the default branch.
	unknownAlarm := &pb.AlarmMember{MemberID: 3, Alarm: pb.AlarmType(999)}
	m := &fakeMaintenance{
		alarms:    []*pb.AlarmMember{unknownAlarm},
		statusRev: 5000,
	}
	kv := &fakeKV{}
	h := New(m, kv, "http://localhost:2379", 1000, time.Minute, "default", "etcd-main", zap.NewNop())

	if err := h.check(context.Background()); err != nil {
		t.Fatalf("unexpected error for unknown alarm type: %v", err)
	}
	if m.defragCalled {
		t.Error("defrag should not be called for unknown alarm type")
	}
	if len(m.disarmed) != 0 {
		t.Error("alarm should not be disarmed for unknown alarm type")
	}
}
