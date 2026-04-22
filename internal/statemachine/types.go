// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import "time"

// State represents the current state of an etcd member in its lifecycle.
type State string

const (
	// StateUnknown indicates the member state is not yet determined.
	StateUnknown State = "Unknown"
	// StateNew indicates a freshly created member that has not joined any cluster.
	StateNew State = "New"
	// StatePendingLearner indicates the member has been added to the cluster as a learner but has not yet started syncing.
	StatePendingLearner State = "PendingLearner"
	// StateLearner indicates the member is syncing data as a non-voting learner.
	StateLearner State = "Learner"
	// StateFollower indicates the member is a voting follower.
	StateFollower State = "Follower"
	// StateLeader indicates the member is the elected leader.
	StateLeader State = "Leader"
)

// Action represents an event that triggers a state transition.
type Action string

const (
	// ActionStartAsNew triggers a transition from Unknown to New for a brand-new member.
	ActionStartAsNew Action = "StartAsNew"
	// ActionStartAsFollower triggers a transition from Unknown to Follower for a member recovering from a snapshot.
	ActionStartAsFollower Action = "StartAsFollower"
	// ActionStartAsPendingLearner triggers a transition from Unknown to PendingLearner for a member joining as learner.
	ActionStartAsPendingLearner Action = "StartAsPendingLearner"
	// ActionAddedToCluster triggers a transition from New to PendingLearner when the member is added to the cluster.
	ActionAddedToCluster Action = "AddedToCluster"
	// ActionLearnerJoined triggers a transition from PendingLearner to Learner when the learner starts syncing.
	ActionLearnerJoined Action = "LearnerJoined"
	// ActionPromoted triggers a transition from Learner to Follower when the learner is promoted to a voting member.
	ActionPromoted Action = "Promoted"
	// ActionWonElection triggers a transition from Follower to Leader when the member wins a leader election.
	ActionWonElection Action = "WonElection"
	// ActionLostElection triggers a transition from Leader to Follower when the member loses its leader status.
	ActionLostElection Action = "LostElection"
)

// Transition records a single state transition.
type Transition struct {
	// From is the state before the transition.
	From State
	// To is the state after the transition.
	To State
	// Action is the event that triggered the transition.
	Action Action
	// Time is the wall-clock time when the transition occurred.
	Time time.Time
}
