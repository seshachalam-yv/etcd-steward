// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package alarm

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// mockKV implements etcdclient.KV for testing.
type mockKV struct {
	mu           sync.Mutex
	getResp      *clientv3.GetResponse
	getErr       error
	compactCalls []int64
	compactErr   error
}

func (m *mockKV) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getResp, m.getErr
}

func (m *mockKV) Put(_ context.Context, _, _ string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return nil, nil
}

func (m *mockKV) Delete(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return nil, nil
}

func (m *mockKV) Compact(_ context.Context, rev int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactCalls = append(m.compactCalls, rev)
	return &clientv3.CompactResponse{}, m.compactErr
}

// mockMaintenance implements etcdclient.Maintenance for testing.
type mockMaintenance struct {
	mu            sync.Mutex
	alarmListResp *clientv3.AlarmResponse
	alarmListErr  error
	defragCalls   []string
	defragErr     error
	disarmCalls   []*clientv3.AlarmMember
	disarmErr     error
}

func (m *mockMaintenance) AlarmList(_ context.Context) (*clientv3.AlarmResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alarmListResp, m.alarmListErr
}

func (m *mockMaintenance) AlarmDisarm(_ context.Context, am *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disarmCalls = append(m.disarmCalls, am)
	return &clientv3.AlarmResponse{}, m.disarmErr
}

func (m *mockMaintenance) Defragment(_ context.Context, endpoint string) (*clientv3.DefragmentResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defragCalls = append(m.defragCalls, endpoint)
	return &clientv3.DefragmentResponse{}, m.defragErr
}

func (m *mockMaintenance) Status(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
	return &clientv3.StatusResponse{}, nil
}

func (m *mockMaintenance) Snapshot(_ context.Context) (io.ReadCloser, error) {
	return io.NopCloser(nil), nil
}

func TestHandler_NoAlarms(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mk := &mockKV{}
	mm := &mockMaintenance{
		alarmListResp: &clientv3.AlarmResponse{},
	}

	h := New("pod-0", 100*time.Millisecond, 1000, []string{"http://localhost:2379"}, mk, mm, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	h.Run(ctx)

	mm.mu.Lock()
	defer mm.mu.Unlock()
	mk.mu.Lock()
	defer mk.mu.Unlock()

	if len(mk.compactCalls) != 0 {
		t.Errorf("expected 0 compact calls, got %d", len(mk.compactCalls))
	}
	if len(mm.defragCalls) != 0 {
		t.Errorf("expected 0 defrag calls, got %d", len(mm.defragCalls))
	}
	if len(mm.disarmCalls) != 0 {
		t.Errorf("expected 0 disarm calls, got %d", len(mm.disarmCalls))
	}
}

func TestHandler_Nospace(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zap.WarnLevel))
	mk := &mockKV{
		getResp: &clientv3.GetResponse{
			Header: &pb.ResponseHeader{Revision: 5000},
		},
	}
	nospaceAlarm := &pb.AlarmMember{
		Alarm:    pb.AlarmType_NOSPACE,
		MemberID: 123,
	}
	mm := &mockMaintenance{
		alarmListResp: &clientv3.AlarmResponse{
			Alarms: []*pb.AlarmMember{nospaceAlarm},
		},
	}

	endpoints := []string{"http://ep1:2379", "http://ep2:2379"}
	h := New("pod-0", 50*time.Millisecond, 1000, endpoints, mk, mm, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	h.Run(ctx)

	// Verify Compact was called.
	mk.mu.Lock()
	if len(mk.compactCalls) == 0 {
		t.Fatal("expected at least 1 compact call")
	}
	compactRev := mk.compactCalls[0]
	mk.mu.Unlock()

	// Compact revision should be 5000 - 1000 = 4000.
	if compactRev != 4000 {
		t.Errorf("expected compact revision 4000, got %d", compactRev)
	}

	// Verify Defragment was called for all endpoints.
	mm.mu.Lock()
	if len(mm.defragCalls) < len(endpoints) {
		t.Errorf("expected at least %d defrag calls, got %d", len(endpoints), len(mm.defragCalls))
	}
	// Verify Disarm was called.
	if len(mm.disarmCalls) == 0 {
		t.Fatal("expected at least 1 disarm call")
	}
	mm.mu.Unlock()
}

func TestHandler_Corrupt(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zap.ErrorLevel))
	mk := &mockKV{}
	corruptAlarm := &pb.AlarmMember{
		Alarm:    pb.AlarmType_CORRUPT,
		MemberID: 456,
	}
	mm := &mockMaintenance{
		alarmListResp: &clientv3.AlarmResponse{
			Alarms: []*pb.AlarmMember{corruptAlarm},
		},
	}

	h := New("pod-0", 50*time.Millisecond, 1000, []string{"http://localhost:2379"}, mk, mm, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	h.Run(ctx)

	// Corrupt should only log, not compact or defrag.
	mk.mu.Lock()
	defer mk.mu.Unlock()
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if len(mk.compactCalls) != 0 {
		t.Errorf("expected 0 compact calls for CORRUPT alarm, got %d", len(mk.compactCalls))
	}
	if len(mm.defragCalls) != 0 {
		t.Errorf("expected 0 defrag calls for CORRUPT alarm, got %d", len(mm.defragCalls))
	}
	if len(mm.disarmCalls) != 0 {
		t.Errorf("expected 0 disarm calls for CORRUPT alarm, got %d", len(mm.disarmCalls))
	}
}
