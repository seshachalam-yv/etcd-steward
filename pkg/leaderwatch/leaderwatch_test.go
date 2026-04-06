// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package leaderwatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/statemachine"
)

// mockStatusAPI implements StatusAPI for testing.
type mockStatusAPI struct {
	mu        sync.Mutex
	responses []*clientv3.StatusResponse
	errors    []error
	callCount int
}

func (m *mockStatusAPI) addResponse(memberID, leaderID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	resp := &clientv3.StatusResponse{}
	resp.Header = &pb.ResponseHeader{}
	resp.Header.MemberId = memberID
	resp.Leader = leaderID
	m.responses = append(m.responses, resp)
	m.errors = append(m.errors, nil)
}

func (m *mockStatusAPI) addError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = append(m.responses, nil)
	m.errors = append(m.errors, err)
}

func (m *mockStatusAPI) Status(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.callCount
	if idx >= len(m.responses) {
		idx = len(m.responses) - 1
	}
	m.callCount++
	return m.responses[idx], m.errors[idx]
}

// capturingRecorder records transitions for assertion in tests.
type capturingRecorder struct {
	mu          sync.Mutex
	transitions []statemachine.Transition
}

func (r *capturingRecorder) Record(_ context.Context, _, _ string, t statemachine.Transition) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions = append(r.transitions, t)
	return nil
}

func (r *capturingRecorder) captured() []statemachine.Transition {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]statemachine.Transition, len(r.transitions))
	copy(result, r.transitions)
	return result
}

func newTestWatcher(pollInterval time.Duration, api StatusAPI, rec statemachine.Recorder) *LeaderWatcher {
	logger, _ := zap.NewDevelopment()
	return New(pollInterval, rec, "etcd-main-0", "default", api, logger)
}

func waitForTransitions(t *testing.T, rec *capturingRecorder, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if len(rec.captured()) >= n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d transitions, got %d", n, len(rec.captured()))
		case <-time.After(time.Millisecond):
		}
	}
}

func TestGainsLeadership(t *testing.T) {
	const memberID uint64 = 10
	const otherID uint64 = 20

	api := &mockStatusAPI{}
	api.addResponse(memberID, otherID)   // not leader
	api.addResponse(memberID, memberID)  // became leader
	api.addResponse(memberID, memberID)  // stay leader

	rec := &capturingRecorder{}
	watcher := newTestWatcher(1*time.Millisecond, api, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go watcher.Run(ctx, "localhost:2379")

	waitForTransitions(t, rec, 1, 150*time.Millisecond)

	transitions := rec.captured()
	if len(transitions) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(transitions))
	}
	if transitions[0].State != statemachine.StateStarted {
		t.Errorf("expected state Started, got %s", transitions[0].State)
	}
	if transitions[0].Reason != statemachine.ReasonGainedClusterLeadership {
		t.Errorf("expected reason GainedClusterLeadership, got %s", transitions[0].Reason)
	}
	if transitions[0].SubState == nil || *transitions[0].SubState != statemachine.SubStateLeader {
		t.Error("expected subState Leader")
	}
}

func TestLosesLeadership(t *testing.T) {
	const memberID uint64 = 10
	const otherID uint64 = 20

	api := &mockStatusAPI{}
	api.addResponse(memberID, memberID)  // is leader
	api.addResponse(memberID, otherID)   // lost leadership
	api.addResponse(memberID, otherID)   // stay follower

	rec := &capturingRecorder{}
	watcher := newTestWatcher(1*time.Millisecond, api, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go watcher.Run(ctx, "localhost:2379")

	waitForTransitions(t, rec, 1, 150*time.Millisecond)

	transitions := rec.captured()
	if len(transitions) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(transitions))
	}
	if transitions[0].Reason != statemachine.ReasonLostClusterLeadership {
		t.Errorf("expected reason LostClusterLeadership, got %s", transitions[0].Reason)
	}
	if transitions[0].SubState == nil || *transitions[0].SubState != statemachine.SubStateFollower {
		t.Error("expected subState Follower")
	}
}

func TestNoChangeNoTransition(t *testing.T) {
	const memberID uint64 = 10

	api := &mockStatusAPI{}
	api.addResponse(memberID, memberID) // leader
	api.addResponse(memberID, memberID) // still leader
	api.addResponse(memberID, memberID) // still leader

	rec := &capturingRecorder{}
	watcher := newTestWatcher(5*time.Millisecond, api, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	watcher.Run(ctx, "localhost:2379")

	if len(rec.captured()) != 0 {
		t.Errorf("expected 0 transitions, got %d", len(rec.captured()))
	}
}

func TestTransientErrorContinues(t *testing.T) {
	const memberID uint64 = 10

	api := &mockStatusAPI{}
	api.addError(errors.New("connection refused"))
	api.addResponse(memberID, memberID+1)  // follower
	api.addResponse(memberID, memberID)    // becomes leader
	api.addResponse(memberID, memberID)    // stay leader

	rec := &capturingRecorder{}
	watcher := newTestWatcher(1*time.Millisecond, api, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go watcher.Run(ctx, "localhost:2379")

	waitForTransitions(t, rec, 1, 150*time.Millisecond)

	transitions := rec.captured()
	if len(transitions) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(transitions))
	}
	if transitions[0].Reason != statemachine.ReasonGainedClusterLeadership {
		t.Errorf("expected reason GainedClusterLeadership, got %s", transitions[0].Reason)
	}
}

func TestContextCancelStops(t *testing.T) {
	api := &mockStatusAPI{}
	api.addResponse(10, 20)

	rec := &capturingRecorder{}
	watcher := newTestWatcher(5*time.Millisecond, api, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		watcher.Run(ctx, "localhost:2379")
		close(done)
	}()

	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}
