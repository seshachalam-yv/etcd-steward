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
	sm := New(true)
	cur := sm.Current()
	if cur.State != "" {
		t.Fatalf("expected initial state to be empty, got %q", cur.State)
	}
	if cur.SubState != "" {
		t.Fatalf("expected initial sub-state to be empty, got %q", cur.SubState)
	}
	if len(sm.Transitions()) != 0 {
		t.Fatalf("expected zero transitions, got %d", len(sm.Transitions()))
	}
}

func TestSingleNode_Bootstrap(t *testing.T) {
	// New -> Initializing/DBValidationFull -> Started/Leader (clean exit, validation succeeds)
	sm := New(true)

	steps := []struct {
		reason      Reason
		message     string
		wantState   State
		wantSubState SubState
	}{
		{ReasonNewSingleNodeClusterCreated, "single-node cluster created", StateNew, SubStateNone},
		{ReasonDetectedPreviousUncleanExit, "unclean exit detected", StateInitializing, SubStateDBValidationFull},
		{ReasonDBValidationSucceeded, "validation passed", StateStarted, SubStateLeader},
	}

	for i, s := range steps {
		tr, err := sm.Trigger(s.reason, s.message)
		if err != nil {
			t.Fatalf("step %d (%s): unexpected error: %v", i, s.reason, err)
		}
		if tr.State != s.wantState {
			t.Fatalf("step %d (%s): expected State %q, got %q", i, s.reason, s.wantState, tr.State)
		}
		if tr.SubState != s.wantSubState {
			t.Fatalf("step %d (%s): expected SubState %q, got %q", i, s.reason, s.wantSubState, tr.SubState)
		}
		if tr.Reason != s.reason {
			t.Fatalf("step %d: expected Reason %q, got %q", i, s.reason, tr.Reason)
		}
		if tr.Message != s.message {
			t.Fatalf("step %d: expected Message %q, got %q", i, s.message, tr.Message)
		}
		if tr.TransitionTime.IsZero() {
			t.Fatalf("step %d: TransitionTime must not be zero", i)
		}
	}

	cur := sm.Current()
	if cur.State != StateStarted || cur.SubState != SubStateLeader {
		t.Fatalf("expected final state (Started, Leader), got (%q, %q)", cur.State, cur.SubState)
	}
	if len(sm.Transitions()) != 3 {
		t.Fatalf("expected 3 transitions, got %d", len(sm.Transitions()))
	}
}

func TestSingleNode_RestoreFromBackup(t *testing.T) {
	// New -> Initializing/DBValidationFull -> Initializing/Restoration -> Started/Leader
	sm := New(true)

	steps := []struct {
		reason       Reason
		wantState    State
		wantSubState SubState
	}{
		{ReasonNewSingleNodeClusterCreated, StateNew, SubStateNone},
		{ReasonDetectedPreviousUncleanExit, StateInitializing, SubStateDBValidationFull},
		{ReasonDBValidationFailed, StateInitializing, SubStateRestoration},
		{ReasonRestorationSucceeded, StateStarted, SubStateLeader},
	}

	for i, s := range steps {
		_, err := sm.Trigger(s.reason, "")
		if err != nil {
			t.Fatalf("step %d (%s): unexpected error: %v", i, s.reason, err)
		}
		cur := sm.Current()
		if cur.State != s.wantState || cur.SubState != s.wantSubState {
			t.Fatalf("step %d (%s): expected (%q, %q), got (%q, %q)", i, s.reason, s.wantState, s.wantSubState, cur.State, cur.SubState)
		}
	}

	if len(sm.Transitions()) != 4 {
		t.Fatalf("expected 4 transitions, got %d", len(sm.Transitions()))
	}
}

func TestSingleNode_SanityValidationSucceeds(t *testing.T) {
	// New -> Initializing/DBValidationSanity -> Started/Leader
	sm := New(true)

	if _, err := sm.Trigger(ReasonNewSingleNodeClusterCreated, ""); err != nil {
		t.Fatalf("step 0: %v", err)
	}
	if _, err := sm.Trigger(ReasonDetectedPreviousCleanExit, ""); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	if _, err := sm.Trigger(ReasonDBValidationSucceeded, ""); err != nil {
		t.Fatalf("step 2: %v", err)
	}

	cur := sm.Current()
	if cur.State != StateStarted || cur.SubState != SubStateLeader {
		t.Fatalf("expected (Started, Leader), got (%q, %q)", cur.State, cur.SubState)
	}
}

func TestSingleNode_SanityValidationFails(t *testing.T) {
	// New -> Initializing/DBValidationSanity -> Initializing/Restoration (single-node)
	sm := New(true)

	if _, err := sm.Trigger(ReasonNewSingleNodeClusterCreated, ""); err != nil {
		t.Fatalf("step 0: %v", err)
	}
	if _, err := sm.Trigger(ReasonDetectedPreviousCleanExit, ""); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	if _, err := sm.Trigger(ReasonDBValidationFailed, ""); err != nil {
		t.Fatalf("step 2: %v", err)
	}

	cur := sm.Current()
	if cur.State != StateInitializing || cur.SubState != SubStateRestoration {
		t.Fatalf("expected (Initializing, Restoration), got (%q, %q)", cur.State, cur.SubState)
	}
}

func TestMultiNode_ScaleUp(t *testing.T) {
	// New -> Starting/PendingLearner -> Starting/Learner -> Started/Follower
	sm := New(false)

	steps := []struct {
		reason       Reason
		wantState    State
		wantSubState SubState
	}{
		{ReasonClusterScaledUp, StateNew, SubStateNone},
		{ReasonWaitingToJoinAsLearner, StateStarting, SubStatePendingLearner},
		{ReasonJoinedAsLearner, StateStarting, SubStateLearner},
		{ReasonPromotedAsVotingMember, StateStarted, SubStateFollower},
	}

	for i, s := range steps {
		_, err := sm.Trigger(s.reason, "")
		if err != nil {
			t.Fatalf("step %d (%s): unexpected error: %v", i, s.reason, err)
		}
		cur := sm.Current()
		if cur.State != s.wantState || cur.SubState != s.wantSubState {
			t.Fatalf("step %d (%s): expected (%q, %q), got (%q, %q)", i, s.reason, s.wantState, s.wantSubState, cur.State, cur.SubState)
		}
	}

	if len(sm.Transitions()) != 4 {
		t.Fatalf("expected 4 transitions, got %d", len(sm.Transitions()))
	}
}

func TestMultiNode_LeaderElection(t *testing.T) {
	// Started/Follower -> Started/Leader -> Started/Follower
	sm := New(false)

	// Bring to Started/Follower first.
	setup := []Reason{
		ReasonClusterScaledUp,
		ReasonWaitingToJoinAsLearner,
		ReasonJoinedAsLearner,
		ReasonPromotedAsVotingMember,
	}
	for _, r := range setup {
		if _, err := sm.Trigger(r, ""); err != nil {
			t.Fatalf("setup (%s): %v", r, err)
		}
	}

	// Gain leadership.
	tr, err := sm.Trigger(ReasonGainedClusterLeadership, "won election")
	if err != nil {
		t.Fatalf("gain leadership: %v", err)
	}
	if tr.State != StateStarted || tr.SubState != SubStateLeader {
		t.Fatalf("expected (Started, Leader), got (%q, %q)", tr.State, tr.SubState)
	}

	// Lose leadership.
	tr, err = sm.Trigger(ReasonLostClusterLeadership, "lost election")
	if err != nil {
		t.Fatalf("lose leadership: %v", err)
	}
	if tr.State != StateStarted || tr.SubState != SubStateFollower {
		t.Fatalf("expected (Started, Follower), got (%q, %q)", tr.State, tr.SubState)
	}
}

func TestMultiNode_RestartWithCorruption(t *testing.T) {
	// Multi-node: DB validation failure resets to New.
	sm := New(false)

	steps := []struct {
		reason       Reason
		wantState    State
		wantSubState SubState
	}{
		{ReasonClusterScaledUp, StateNew, SubStateNone},
		{ReasonDetectedPreviousUncleanExit, StateInitializing, SubStateDBValidationFull},
		{ReasonDBValidationFailed, StateNew, SubStateNone},
	}

	for i, s := range steps {
		_, err := sm.Trigger(s.reason, "")
		if err != nil {
			t.Fatalf("step %d (%s): unexpected error: %v", i, s.reason, err)
		}
		cur := sm.Current()
		if cur.State != s.wantState || cur.SubState != s.wantSubState {
			t.Fatalf("step %d (%s): expected (%q, %q), got (%q, %q)", i, s.reason, s.wantState, s.wantSubState, cur.State, cur.SubState)
		}
	}
}

func TestMultiNode_ValidationSucceeds(t *testing.T) {
	// Multi-node: DB validation success leads to Follower (not Leader).
	sm := New(false)

	if _, err := sm.Trigger(ReasonClusterScaledUp, ""); err != nil {
		t.Fatalf("step 0: %v", err)
	}
	if _, err := sm.Trigger(ReasonDetectedPreviousCleanExit, ""); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	if _, err := sm.Trigger(ReasonDBValidationSucceeded, ""); err != nil {
		t.Fatalf("step 2: %v", err)
	}

	cur := sm.Current()
	if cur.State != StateStarted || cur.SubState != SubStateFollower {
		t.Fatalf("expected (Started, Follower), got (%q, %q)", cur.State, cur.SubState)
	}
}

func TestInvalidTransition(t *testing.T) {
	sm := New(true)

	// WaitingToJoinAsLearner is not valid from initial state ("", "").
	_, err := sm.Trigger(ReasonWaitingToJoinAsLearner, "")
	if err == nil {
		t.Fatal("expected error for invalid transition from initial state")
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
	cur := sm.Current()
	if cur.State != "" || cur.SubState != "" {
		t.Fatalf("expected state to remain empty after invalid transition, got (%q, %q)", cur.State, cur.SubState)
	}
}

func TestInvalidTransition_LeaderToLearner(t *testing.T) {
	// Started/Leader + WaitingToJoinAsLearner -> error
	sm := New(true)

	// Bring to Started/Leader.
	setup := []Reason{
		ReasonNewSingleNodeClusterCreated,
		ReasonDetectedPreviousCleanExit,
		ReasonDBValidationSucceeded,
	}
	for _, r := range setup {
		if _, err := sm.Trigger(r, ""); err != nil {
			t.Fatalf("setup (%s): %v", r, err)
		}
	}

	cur := sm.Current()
	if cur.State != StateStarted || cur.SubState != SubStateLeader {
		t.Fatalf("expected (Started, Leader), got (%q, %q)", cur.State, cur.SubState)
	}

	_, err := sm.Trigger(ReasonWaitingToJoinAsLearner, "")
	if err == nil {
		t.Fatal("expected error for invalid transition Started/Leader + WaitingToJoinAsLearner")
	}
}

func TestTransitionHistory(t *testing.T) {
	sm := New(true)

	reasons := []Reason{
		ReasonNewSingleNodeClusterCreated,
		ReasonDetectedPreviousUncleanExit,
		ReasonDBValidationSucceeded,
	}
	for _, r := range reasons {
		if _, err := sm.Trigger(r, "msg-"+string(r)); err != nil {
			t.Fatalf("trigger %s: %v", r, err)
		}
	}

	history := sm.Transitions()
	if len(history) != 3 {
		t.Fatalf("expected 3 transitions, got %d", len(history))
	}

	// Verify the returned slice is a copy.
	history[0].Reason = "tampered"
	original := sm.Transitions()
	if original[0].Reason == "tampered" {
		t.Fatal("Transitions() must return a copy, not a reference to internal state")
	}

	// Verify ordering and content.
	for i, r := range reasons {
		if original[i].Reason != r {
			t.Fatalf("transition %d: expected reason %q, got %q", i, r, original[i].Reason)
		}
		expectedMsg := "msg-" + string(r)
		if original[i].Message != expectedMsg {
			t.Fatalf("transition %d: expected message %q, got %q", i, expectedMsg, original[i].Message)
		}
		if original[i].TransitionTime.IsZero() {
			t.Fatalf("transition %d: TransitionTime must not be zero", i)
		}
	}

	// Verify specific states in history.
	if original[0].State != StateNew {
		t.Fatalf("transition 0: expected State %q, got %q", StateNew, original[0].State)
	}
	if original[1].State != StateInitializing || original[1].SubState != SubStateDBValidationFull {
		t.Fatalf("transition 1: expected (Initializing, DBValidationFull), got (%q, %q)", original[1].State, original[1].SubState)
	}
	if original[2].State != StateStarted || original[2].SubState != SubStateLeader {
		t.Fatalf("transition 2: expected (Started, Leader), got (%q, %q)", original[2].State, original[2].SubState)
	}
}

func TestConcurrentAccess(t *testing.T) {
	// Use multi-node to allow Follower<->Leader toggling.
	sm := New(false)

	// Bring to Started/Follower.
	setup := []Reason{
		ReasonClusterScaledUp,
		ReasonWaitingToJoinAsLearner,
		ReasonJoinedAsLearner,
		ReasonPromotedAsVotingMember,
	}
	for _, r := range setup {
		if _, err := sm.Trigger(r, ""); err != nil {
			t.Fatalf("setup (%s): %v", r, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Read state - must not panic.
			_ = sm.Current()
			_ = sm.Transitions()

			// Attempt a transition; we don't care whether it succeeds,
			// only that there are no data races.
			_, err := sm.Trigger(ReasonGainedClusterLeadership, "")
			if err != nil {
				// Also try the reverse transition.
				_, err = sm.Trigger(ReasonLostClusterLeadership, "")
				if err != nil {
					errCh <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	// Ensure the final state is either Follower or Leader (both valid).
	cur := sm.Current()
	if cur.State != StateStarted {
		t.Fatalf("expected State 'Started', got %q", cur.State)
	}
	if cur.SubState != SubStateFollower && cur.SubState != SubStateLeader {
		t.Fatalf("expected SubState 'Follower' or 'Leader', got %q", cur.SubState)
	}
}

func TestSingleNode_ClusterScaledUpAlsoWorks(t *testing.T) {
	// Both ReasonClusterScaledUp and ReasonNewSingleNodeClusterCreated
	// should transition from initial state to (New, "").
	sm := New(true)

	tr, err := sm.Trigger(ReasonClusterScaledUp, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr.State != StateNew || tr.SubState != SubStateNone {
		t.Fatalf("expected (New, ''), got (%q, %q)", tr.State, tr.SubState)
	}
}

func TestMultiNode_FullValidationFailResetsToNew(t *testing.T) {
	// Multi-node: DBValidationFull + DBValidationFailed -> New (not Restoration)
	sm := New(false)

	if _, err := sm.Trigger(ReasonClusterScaledUp, ""); err != nil {
		t.Fatalf("step 0: %v", err)
	}
	if _, err := sm.Trigger(ReasonDetectedPreviousUncleanExit, ""); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	tr, err := sm.Trigger(ReasonDBValidationFailed, "data corrupt")
	if err != nil {
		t.Fatalf("step 2: %v", err)
	}
	if tr.State != StateNew || tr.SubState != SubStateNone {
		t.Fatalf("expected (New, ''), got (%q, %q)", tr.State, tr.SubState)
	}
}

func TestMultiNode_SanityValidationFailResetsToNew(t *testing.T) {
	// Multi-node: DBValidationSanity + DBValidationFailed -> New
	sm := New(false)

	if _, err := sm.Trigger(ReasonClusterScaledUp, ""); err != nil {
		t.Fatalf("step 0: %v", err)
	}
	if _, err := sm.Trigger(ReasonDetectedPreviousCleanExit, ""); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	tr, err := sm.Trigger(ReasonDBValidationFailed, "sanity check failed")
	if err != nil {
		t.Fatalf("step 2: %v", err)
	}
	if tr.State != StateNew || tr.SubState != SubStateNone {
		t.Fatalf("expected (New, ''), got (%q, %q)", tr.State, tr.SubState)
	}
}
