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

// ruleKey uniquely identifies a transition rule by the source state and the triggering action.
type ruleKey struct {
	from   State
	action Action
}

// StateMachine tracks the lifecycle state of an etcd member and enforces
// valid transitions. All methods are safe for concurrent use.
type StateMachine struct {
	mu          sync.RWMutex
	current     State
	transitions []Transition
	rules       map[ruleKey]State
}

// New creates a StateMachine that starts in StateUnknown with the default
// transition rules.
func New() *StateMachine {
	return &StateMachine{
		current:     StateUnknown,
		transitions: make([]Transition, 0),
		rules:       defaultRules(),
	}
}

// defaultRules returns the complete set of allowed transitions.
func defaultRules() map[ruleKey]State {
	return map[ruleKey]State{
		{from: StateUnknown, action: ActionStartAsNew}:             StateNew,
		{from: StateUnknown, action: ActionStartAsFollower}:        StateFollower,
		{from: StateUnknown, action: ActionStartAsPendingLearner}: StatePendingLearner,
		{from: StateNew, action: ActionAddedToCluster}:             StatePendingLearner,
		{from: StatePendingLearner, action: ActionLearnerJoined}:   StateLearner,
		{from: StateLearner, action: ActionPromoted}:               StateFollower,
		{from: StateFollower, action: ActionWonElection}:           StateLeader,
		{from: StateLeader, action: ActionLostElection}:            StateFollower,
	}
}

// Trigger attempts to execute the given action against the current state.
// On success it records the transition and returns it.
// On failure it returns an error with ErrCodeInvalidTransition.
func (sm *StateMachine) Trigger(action Action) (Transition, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := ruleKey{from: sm.current, action: action}
	target, ok := sm.rules[key]
	if !ok {
		return Transition{}, errors.New(
			errors.ErrCodeInvalidTransition,
			fmt.Sprintf("no transition from state %q with action %q", sm.current, action),
		)
	}

	t := Transition{
		From:   sm.current,
		To:     target,
		Action: action,
		Time:   time.Now(),
	}
	sm.current = target
	sm.transitions = append(sm.transitions, t)
	return t, nil
}

// State returns the current state.
func (sm *StateMachine) State() State {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.current
}

// Transitions returns a copy of the transition history.
func (sm *StateMachine) Transitions() []Transition {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	out := make([]Transition, len(sm.transitions))
	copy(out, sm.transitions)
	return out
}
