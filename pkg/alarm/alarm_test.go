// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package alarm

import (
	"context"
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
}

func (f *fakeMaintenance) AlarmList(_ context.Context) (*clientv3.AlarmResponse, error) {
	if f.alarmListErr != nil {
		return nil, f.alarmListErr
	}
	return (*clientv3.AlarmResponse)(&pb.AlarmResponse{Alarms: f.alarms}), nil
}

func (f *fakeMaintenance) AlarmDisarm(_ context.Context, m *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	f.disarmed = append(f.disarmed, m)
	return (*clientv3.AlarmResponse)(&pb.AlarmResponse{}), nil
}

func (f *fakeMaintenance) Defragment(_ context.Context, _ string) (*clientv3.DefragmentResponse, error) {
	f.defragCalled = true
	return (*clientv3.DefragmentResponse)(&pb.DefragmentResponse{}), nil
}

func (f *fakeMaintenance) Status(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
	return (*clientv3.StatusResponse)(&pb.StatusResponse{
		Header: &pb.ResponseHeader{Revision: f.statusRev},
	}), nil
}

// fakeKV implements KVCompactAPI for testing.
type fakeKV struct {
	compactedRev int64
}

func (f *fakeKV) Compact(_ context.Context, rev int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
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
