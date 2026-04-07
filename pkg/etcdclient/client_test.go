// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdclient

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// mockCluster implements clientv3.Cluster for testing.
type mockCluster struct {
	memberAddAsLearnerResponses []memberAddResult
	memberAddAsLearnerCallCount int

	memberPromoteErr error
	memberRemoveErr  error

	memberListResp *clientv3.MemberListResponse
	memberListErr  error
}

type memberAddResult struct {
	resp *clientv3.MemberAddResponse
	err  error
}

func (m *mockCluster) MemberList(_ context.Context) (*clientv3.MemberListResponse, error) {
	return m.memberListResp, m.memberListErr
}

func (m *mockCluster) MemberAdd(_ context.Context, _ []string) (*clientv3.MemberAddResponse, error) {
	panic("not implemented")
}

func (m *mockCluster) MemberAddAsLearner(_ context.Context, _ []string) (*clientv3.MemberAddResponse, error) {
	idx := m.memberAddAsLearnerCallCount
	if idx >= len(m.memberAddAsLearnerResponses) {
		idx = len(m.memberAddAsLearnerResponses) - 1
	}
	m.memberAddAsLearnerCallCount++
	r := m.memberAddAsLearnerResponses[idx]
	return r.resp, r.err
}

func (m *mockCluster) MemberRemove(_ context.Context, _ uint64) (*clientv3.MemberRemoveResponse, error) {
	return &clientv3.MemberRemoveResponse{}, m.memberRemoveErr
}

func (m *mockCluster) MemberUpdate(_ context.Context, _ uint64, _ []string) (*clientv3.MemberUpdateResponse, error) {
	panic("not implemented")
}

func (m *mockCluster) MemberPromote(_ context.Context, _ uint64) (*clientv3.MemberPromoteResponse, error) {
	return &clientv3.MemberPromoteResponse{}, m.memberPromoteErr
}

func newTestClient(mock *mockCluster) *EtcdClusterClient {
	return &EtcdClusterClient{
		cluster:           mock,
		initialBackoff:    time.Millisecond,
		maxRetries:        6,
		perAttemptTimeout: 30 * time.Second,
	}
}

func TestAddLearner(t *testing.T) {
	tests := []struct {
		name           string
		responses      []memberAddResult
		wantID         uint64
		wantErr        bool
		wantErrContain string
		wantCalls      int
	}{
		{
			name: "success on first attempt",
			responses: []memberAddResult{
				{resp: &clientv3.MemberAddResponse{Member: &pb.Member{ID: 0xdeadbeef}}},
			},
			wantID:    0xdeadbeef,
			wantCalls: 1,
		},
		{
			name: "retries on transient error then succeeds",
			responses: []memberAddResult{
				{err: errors.New("transient error")},
				{err: errors.New("transient error")},
				{resp: &clientv3.MemberAddResponse{Member: &pb.Member{ID: 0xabcd1234}}},
			},
			wantID:    0xabcd1234,
			wantCalls: 3,
		},
		{
			name: "fails after six attempts",
			responses: []memberAddResult{
				{err: errors.New("connection refused")},
			},
			wantErr:        true,
			wantErrContain: "after 6 attempts",
			wantCalls:      6,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockCluster{memberAddAsLearnerResponses: tc.responses}
			c := newTestClient(mock)

			id, err := c.AddLearner(context.Background(), "https://etcd-main-0:2380")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.wantErrContain != "" && !containsStr(err.Error(), tc.wantErrContain) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrContain)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if id != tc.wantID {
					t.Errorf("ID = %x, want %x", id, tc.wantID)
				}
			}
			if mock.memberAddAsLearnerCallCount != tc.wantCalls {
				t.Errorf("call count = %d, want %d", mock.memberAddAsLearnerCallCount, tc.wantCalls)
			}
		})
	}
}

func TestAddLearner_ContextCancelledDuringBackoff(t *testing.T) {
	mock := &mockCluster{
		memberAddAsLearnerResponses: []memberAddResult{
			{err: errors.New("error")},
		},
	}
	c := &EtcdClusterClient{
		cluster:           mock,
		initialBackoff:    10 * time.Second,
		maxRetries:        6,
		perAttemptTimeout: 30 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := c.AddLearner(ctx, "https://etcd-main-0:2380")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected context error, got: %v", err)
	}
}

func TestPromoteMember(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := newTestClient(&mockCluster{})
		if err := c.PromoteMember(context.Background(), 0xdeadbeef); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("error", func(t *testing.T) {
		c := newTestClient(&mockCluster{memberPromoteErr: errors.New("promote failed")})
		err := c.PromoteMember(context.Background(), 0xdeadbeef)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !containsStr(err.Error(), "failed to promote member") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestRemoveMember(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := newTestClient(&mockCluster{})
		if err := c.RemoveMember(context.Background(), 0xdeadbeef); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("error", func(t *testing.T) {
		c := newTestClient(&mockCluster{memberRemoveErr: errors.New("remove failed")})
		err := c.RemoveMember(context.Background(), 0xdeadbeef)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !containsStr(err.Error(), "failed to remove member") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestListMembers(t *testing.T) {
	t.Run("returns member info", func(t *testing.T) {
		mock := &mockCluster{
			memberListResp: (*clientv3.MemberListResponse)(&pb.MemberListResponse{
				Members: []*pb.Member{
					{ID: 0x1111, Name: "etcd-main-0", PeerURLs: []string{"https://etcd-main-0:2380"}, IsLearner: false},
					{ID: 0x2222, Name: "etcd-main-1", PeerURLs: []string{"https://etcd-main-1:2380"}, IsLearner: true},
				},
			}),
		}
		c := newTestClient(mock)
		members, err := c.ListMembers(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(members) != 2 {
			t.Fatalf("expected 2 members, got %d", len(members))
		}
		if members[0].ID != 0x1111 || members[0].Name != "etcd-main-0" || members[0].IsLearner {
			t.Errorf("unexpected member[0]: %+v", members[0])
		}
		if members[1].ID != 0x2222 || !members[1].IsLearner {
			t.Errorf("unexpected member[1]: %+v", members[1])
		}
	})
	t.Run("error", func(t *testing.T) {
		mock := &mockCluster{memberListErr: errors.New("list failed")}
		c := newTestClient(mock)
		_, err := c.ListMembers(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestWasMemberInCluster(t *testing.T) {
	mock := &mockCluster{
		memberListResp: (*clientv3.MemberListResponse)(&pb.MemberListResponse{
			Members: []*pb.Member{
				{ID: 0x1111, Name: "etcd-main-0", PeerURLs: []string{"https://etcd-main-0:2380"}},
			},
		}),
	}
	c := newTestClient(mock)

	t.Run("found", func(t *testing.T) {
		found, err := c.WasMemberInCluster(context.Background(), "https://etcd-main-0:2380")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Error("expected true, got false")
		}
	})
	t.Run("not found", func(t *testing.T) {
		found, err := c.WasMemberInCluster(context.Background(), "https://etcd-main-2:2380")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if found {
			t.Error("expected false, got true")
		}
	})
}

func TestRemoveStaleMember(t *testing.T) {
	t.Run("found and removed", func(t *testing.T) {
		mock := &mockCluster{
			memberListResp: (*clientv3.MemberListResponse)(&pb.MemberListResponse{
				Members: []*pb.Member{
					{ID: 0x2222, Name: "etcd-main-2", PeerURLs: []string{"https://etcd-main-2:2380"}},
				},
			}),
		}
		c := newTestClient(mock)
		if err := c.RemoveStaleMember(context.Background(), "https://etcd-main-2:2380"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("not found is no-op", func(t *testing.T) {
		mock := &mockCluster{
			memberListResp: (*clientv3.MemberListResponse)(&pb.MemberListResponse{
				Members: []*pb.Member{
					{ID: 0x1111, Name: "etcd-main-0", PeerURLs: []string{"https://etcd-main-0:2380"}},
				},
			}),
		}
		c := newTestClient(mock)
		if err := c.RemoveStaleMember(context.Background(), "https://etcd-main-2:2380"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("remove error", func(t *testing.T) {
		mock := &mockCluster{
			memberListResp: (*clientv3.MemberListResponse)(&pb.MemberListResponse{
				Members: []*pb.Member{
					{ID: 0x2222, Name: "etcd-main-2", PeerURLs: []string{"https://etcd-main-2:2380"}},
				},
			}),
			memberRemoveErr: errors.New("remove failed"),
		}
		c := newTestClient(mock)
		err := c.RemoveStaleMember(context.Background(), "https://etcd-main-2:2380")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
