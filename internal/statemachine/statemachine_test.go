// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"sync"
	"testing"

	"github.com/gardener/etcd-steward/internal/errors"
)

func TestInitialState(t *testing.T) {
	sm := New()
	if sm.State() != StateUnknown {
		t.Fatalf("expected initial state %q, got %q", StateUnknown, sm.State())
	}
	if len(sm.Transitions()) != 0 {
		t.Fatalf("expected zero transitions, got %d", len(sm.Transitions()))
	}
}

func TestUnknownToNew(t *testing.T) {
	sm := New()
	tr, err := sm.Trigger(ActionStartAsNew)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr.From != StateUnknown {
		t.Fatalf("expected From %q, got %q", StateUnknown, tr.From)
	}
	if tr.To != StateNew {
		t.Fatalf("expected To %q, got %q", StateNew, tr.To)
	}
	if tr.Action != ActionStartAsNew {
		t.Fatalf("expected Action %q, got %q", ActionStartAsNew, tr.Action)
	}
	if sm.State() != StateNew {
		t.Fatalf("expected state %q after transition, got %q", StateNew, sm.State())
	}
}

func TestUnknownToFollower(t *testing.T) {
	sm := New()
	tr, err := sm.Trigger(ActionStartAsFollower)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr.From != StateUnknown || tr.To != StateFollower {
		t.Fatalf("expected Unknown->Follower, got %q->%q", tr.From, tr.To)
	}
	if sm.State() != StateFollower {
		t.Fatalf("expected state %q, got %q", StateFollower, sm.State())
	}
}

func TestUnknownToPendingLearner(t *testing.T) {
	sm := New()
	tr, err := sm.Trigger(ActionStartAsPendingLearner)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr.From != StateUnknown || tr.To != StatePendingLearner {
		t.Fatalf("expected Unknown->PendingLearner, got %q->%q", tr.From, tr.To)
	}
	if sm.State() != StatePendingLearner {
		t.Fatalf("expected state %q, got %q", StatePendingLearner, sm.State())
	}
}

func TestFullLearnerPath(t *testing.T) {
	sm := New()

	steps := []struct {
		action     Action
		wantFrom   State
		wantTo     State
	}{
		{ActionStartAsNew, StateUnknown, StateNew},
		{ActionAddedToCluster, StateNew, StatePendingLearner},
		{ActionLearnerJoined, StatePendingLearner, StateLearner},
		{ActionPromoted, StateLearner, StateFollower},
		{ActionWonElection, StateFollower, StateLeader},
	}

	for i, s := range steps {
		tr, err := sm.Trigger(s.action)
		if err != nil {
			t.Fatalf("step %d (%s): unexpected error: %v", i, s.action, err)
		}
		if tr.From != s.wantFrom {
			t.Fatalf("step %d (%s): expected From %q, got %q", i, s.action, s.wantFrom, tr.From)
		}
		if tr.To != s.wantTo {
			t.Fatalf("step %d (%s): expected To %q, got %q", i, s.action, s.wantTo, tr.To)
		}
	}

	if sm.State() != StateLeader {
		t.Fatalf("expected final state %q, got %q", StateLeader, sm.State())
	}
	if len(sm.Transitions()) != 5 {
		t.Fatalf("expected 5 transitions, got %d", len(sm.Transitions()))
	}
}

func TestInvalidTransition(t *testing.T) {
	sm := New()

	// Promoted is not valid from Unknown.
	_, err := sm.Trigger(ActionPromoted)
	if err == nil {
		t.Fatal("expected error for invalid transition")
	}

	var stewardErr *errors.Error
	ok := errors.As(err, &stewardErr)
	if !ok {
		t.Fatalf("expected *errors.Error, got %T", err)
	}
	if stewardErr.Code() != errors.ErrCodeInvalidTransition {
		t.Fatalf("expected ErrCodeInvalidTransition, got %d", stewardErr.Code())
	}

	// State must remain unchanged.
	if sm.State() != StateUnknown {
		t.Fatalf("expected state to remain %q after invalid transition, got %q", StateUnknown, sm.State())
	}
}

func TestTransitionHistory(t *testing.T) {
	sm := New()

	actions := []Action{ActionStartAsNew, ActionAddedToCluster, ActionLearnerJoined}
	for _, a := range actions {
		if _, err := sm.Trigger(a); err != nil {
			t.Fatalf("trigger %s: %v", a, err)
		}
	}

	history := sm.Transitions()
	if len(history) != 3 {
		t.Fatalf("expected 3 transitions, got %d", len(history))
	}

	// Verify the returned slice is a copy and not a reference to internal state.
	history[0].Action = "tampered"
	original := sm.Transitions()
	if original[0].Action == "tampered" {
		t.Fatal("Transitions() must return a copy, not a reference to internal state")
	}

	// Verify ordering.
	expectedActions := []Action{ActionStartAsNew, ActionAddedToCluster, ActionLearnerJoined}
	for i, ea := range expectedActions {
		if original[i].Action != ea {
			t.Fatalf("transition %d: expected action %q, got %q", i, ea, original[i].Action)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	sm := New()

	// Move to Follower so we can toggle Leader<->Follower.
	if _, err := sm.Trigger(ActionStartAsFollower); err != nil {
		t.Fatalf("setup: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Read state — must not panic.
			_ = sm.State()
			_ = sm.Transitions()

			// Attempt a transition; we don't care whether it succeeds,
			// only that there are no data races.
			_, err := sm.Trigger(ActionWonElection)
			if err != nil {
				// Also try the reverse transition.
				_, err = sm.Trigger(ActionLostElection)
				if err != nil {
					errCh <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	// Ensure the final state is either Follower or Leader (both valid).
	s := sm.State()
	if s != StateFollower && s != StateLeader {
		t.Fatalf("expected state Follower or Leader, got %q", s)
	}
}
