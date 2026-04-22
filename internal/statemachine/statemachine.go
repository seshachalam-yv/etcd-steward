// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"fmt"
	"sync"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
)

// ruleKey uniquely identifies a transition rule by the source (State, SubState) pair
// and the triggering Reason.
type ruleKey struct {
	state    State
	subState SubState
	reason   Reason
}

// ruleTarget is the destination (State, SubState) pair for a transition.
type ruleTarget struct {
	state    State
	subState SubState
}

// StateMachine tracks the lifecycle state of an etcd member using (State, SubState)
// pairs and enforces valid transitions based on DEP-04 rules. Some transitions
// depend on cluster topology (single-node vs multi-node). All methods are safe
// for concurrent use.
type StateMachine struct {
	mu           sync.RWMutex
	current      MemberState
	transitions  []Transition
	isSingleNode bool
	rules        map[ruleKey]ruleTarget
}

// New creates a StateMachine that starts in an uninitialized state (empty State
// and SubState). The isSingleNode parameter controls topology-dependent transitions.
func New(isSingleNode bool) *StateMachine {
	sm := &StateMachine{
		current:      MemberState{},
		transitions:  make([]Transition, 0),
		isSingleNode: isSingleNode,
	}
	sm.rules = sm.buildRules()
	return sm
}

// buildRules constructs the complete set of allowed transitions per DEP-04.
// Topology-dependent transitions (DBValidationFailed and DBValidationSucceeded
// from Initializing sub-states) are resolved based on isSingleNode.
func (sm *StateMachine) buildRules() map[ruleKey]ruleTarget {
	rules := map[ruleKey]ruleTarget{
		// (nil) -> (New, "")
		{state: "", subState: "", reason: ReasonClusterScaledUp}:              {state: StateNew, subState: SubStateNone},
		{state: "", subState: "", reason: ReasonNewSingleNodeClusterCreated}:  {state: StateNew, subState: SubStateNone},

		// (New, "") -> (Initializing, DBValidationSanity|DBValidationFull)
		{state: StateNew, subState: SubStateNone, reason: ReasonDetectedPreviousCleanExit}:   {state: StateInitializing, subState: SubStateDBValidationSanity},
		{state: StateNew, subState: SubStateNone, reason: ReasonDetectedPreviousUncleanExit}: {state: StateInitializing, subState: SubStateDBValidationFull},

		// (New, "") -> (Starting, PendingLearner)
		{state: StateNew, subState: SubStateNone, reason: ReasonWaitingToJoinAsLearner}: {state: StateStarting, subState: SubStatePendingLearner},

		// (Initializing, Restoration) -> (Started, Leader)
		{state: StateInitializing, subState: SubStateRestoration, reason: ReasonRestorationSucceeded}: {state: StateStarted, subState: SubStateLeader},

		// (Starting, PendingLearner) -> (Starting, Learner)
		{state: StateStarting, subState: SubStatePendingLearner, reason: ReasonJoinedAsLearner}: {state: StateStarting, subState: SubStateLearner},

		// (Starting, Learner) -> (Started, Follower)
		{state: StateStarting, subState: SubStateLearner, reason: ReasonPromotedAsVotingMember}: {state: StateStarted, subState: SubStateFollower},

		// (Started, Follower) -> (Started, Leader)
		{state: StateStarted, subState: SubStateFollower, reason: ReasonGainedClusterLeadership}: {state: StateStarted, subState: SubStateLeader},

		// (Started, Leader) -> (Started, Follower)
		{state: StateStarted, subState: SubStateLeader, reason: ReasonLostClusterLeadership}: {state: StateStarted, subState: SubStateFollower},
	}

	// Topology-dependent transitions for DBValidationFailed.
	if sm.isSingleNode {
		// Single-node: validation failure leads to restoration.
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationSanity, reason: ReasonDBValidationFailed}] = ruleTarget{state: StateInitializing, subState: SubStateRestoration}
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationFull, reason: ReasonDBValidationFailed}] = ruleTarget{state: StateInitializing, subState: SubStateRestoration}
	} else {
		// Multi-node: validation failure resets to New so the member can rejoin.
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationSanity, reason: ReasonDBValidationFailed}] = ruleTarget{state: StateNew, subState: SubStateNone}
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationFull, reason: ReasonDBValidationFailed}] = ruleTarget{state: StateNew, subState: SubStateNone}
	}

	// Topology-dependent transitions for DBValidationSucceeded.
	if sm.isSingleNode {
		// Single-node: after successful validation the member becomes Leader.
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationSanity, reason: ReasonDBValidationSucceeded}] = ruleTarget{state: StateStarted, subState: SubStateLeader}
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationFull, reason: ReasonDBValidationSucceeded}] = ruleTarget{state: StateStarted, subState: SubStateLeader}
	} else {
		// Multi-node: after successful validation the member becomes Follower.
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationSanity, reason: ReasonDBValidationSucceeded}] = ruleTarget{state: StateStarted, subState: SubStateFollower}
		rules[ruleKey{state: StateInitializing, subState: SubStateDBValidationFull, reason: ReasonDBValidationSucceeded}] = ruleTarget{state: StateStarted, subState: SubStateFollower}
	}

	return rules
}

// Current returns the current (State, SubState) pair.
func (sm *StateMachine) Current() MemberState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.current
}

// Trigger attempts to execute a transition triggered by the given reason.
// On success it records the transition and returns it.
// On failure it returns an error with ErrCodeInvalidTransition.
func (sm *StateMachine) Trigger(reason Reason, message string) (Transition, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := ruleKey{state: sm.current.State, subState: sm.current.SubState, reason: reason}
	target, ok := sm.rules[key]
	if !ok {
		return Transition{}, errors.New(
			errors.ErrCodeInvalidTransition,
			fmt.Sprintf("no transition from state (%q, %q) with reason %q", sm.current.State, sm.current.SubState, reason),
		)
	}

	t := Transition{
		State:          target.state,
		SubState:       target.subState,
		Reason:         reason,
		TransitionTime: time.Now(),
		Message:        message,
	}
	sm.current = MemberState{State: target.state, SubState: target.subState}
	sm.transitions = append(sm.transitions, t)
	return t, nil
}

// Transitions returns a copy of the transition history.
func (sm *StateMachine) Transitions() []Transition {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	out := make([]Transition, len(sm.transitions))
	copy(out, sm.transitions)
	return out
}
