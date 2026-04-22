// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package leaderwatch

import (
	"context"
	"sync"
	"testing"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// mockKV implements clientv3.KV for testing. It uses sync.RWMutex and a
// SetValue method to avoid data races (learning from previous session).
type mockKV struct {
	mu    sync.RWMutex
	value string
}

// SetValue safely sets the value returned by subsequent Get calls.
func (m *mockKV) SetValue(v string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = v
}

func (m *mockKV) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.value == "" {
		return &clientv3.GetResponse{}, nil
	}
	return &clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{
			{Value: []byte(m.value)},
		},
	}, nil
}

func (m *mockKV) Put(_ context.Context, _, _ string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}

func (m *mockKV) Delete(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (m *mockKV) Compact(_ context.Context, _ int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (m *mockKV) Do(_ context.Context, _ clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (m *mockKV) Txn(_ context.Context) clientv3.Txn {
	return nil
}

func TestInitialRoleUnknown(t *testing.T) {
	mock := &mockKV{}
	logger := zap.NewNop()
	w := New("pod-0", time.Second, mock, logger)

	if role := w.GetCurrentRole(); role != Unknown {
		t.Fatalf("expected initial role %q, got %q", Unknown, role)
	}
}

func TestDetectsLeader(t *testing.T) {
	mock := &mockKV{}
	mock.SetValue("pod-0")
	logger := zap.NewNop()
	w := New("pod-0", 10*time.Millisecond, mock, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Wait for at least one poll cycle.
	time.Sleep(50 * time.Millisecond)

	if role := w.GetCurrentRole(); role != Leader {
		t.Fatalf("expected role %q, got %q", Leader, role)
	}
}

func TestDetectsFollower(t *testing.T) {
	mock := &mockKV{}
	mock.SetValue("pod-1")
	logger := zap.NewNop()
	w := New("pod-0", 10*time.Millisecond, mock, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	time.Sleep(50 * time.Millisecond)

	if role := w.GetCurrentRole(); role != Follower {
		t.Fatalf("expected role %q, got %q", Follower, role)
	}
}

func TestRoleChangeNotifiesSubscribers(t *testing.T) {
	mock := &mockKV{}
	logger := zap.NewNop()
	w := New("pod-0", 10*time.Millisecond, mock, logger)

	ch := w.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Set value so role changes from Unknown -> Leader.
	mock.SetValue("pod-0")

	select {
	case role := <-ch:
		if role != Leader {
			t.Fatalf("expected notification for %q, got %q", Leader, role)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for role change notification")
	}
}
