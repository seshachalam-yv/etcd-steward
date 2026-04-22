// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import "time"

// State represents the high-level lifecycle state of an etcd member.
type State string

const (
	// StateNew indicates a freshly created member that has not yet been initialized.
	StateNew State = "New"
	// StateInitializing indicates the member is performing DB validation or restoration.
	StateInitializing State = "Initializing"
	// StateStarting indicates the member is joining the cluster as a learner.
	StateStarting State = "Starting"
	// StateStarted indicates the member has fully started and is participating in the cluster.
	StateStarted State = "Started"
)

// SubState represents the detailed sub-state within a high-level State.
type SubState string

const (
	// SubStateNone indicates no sub-state (used with StateNew).
	SubStateNone SubState = ""
	// SubStateDBValidationSanity indicates a sanity-level DB validation is in progress.
	SubStateDBValidationSanity SubState = "DBValidationSanity"
	// SubStateDBValidationFull indicates a full DB validation is in progress.
	SubStateDBValidationFull SubState = "DBValidationFull"
	// SubStateRestoration indicates a DB restoration from backup is in progress.
	SubStateRestoration SubState = "Restoration"
	// SubStatePendingLearner indicates the member is waiting to be added as a learner.
	SubStatePendingLearner SubState = "PendingLearner"
	// SubStateLearner indicates the member has joined as a non-voting learner.
	SubStateLearner SubState = "Learner"
	// SubStateFollower indicates the member is a voting follower.
	SubStateFollower SubState = "Follower"
	// SubStateLeader indicates the member is the elected leader.
	SubStateLeader SubState = "Leader"
)

// Reason represents the reason/event that triggers a state transition.
type Reason string

const (
	// ReasonClusterScaledUp indicates a new member was added due to cluster scale-up.
	ReasonClusterScaledUp Reason = "ClusterScaledUp"
	// ReasonNewSingleNodeClusterCreated indicates a brand-new single-node cluster was created.
	ReasonNewSingleNodeClusterCreated Reason = "NewSingleNodeClusterCreated"
	// ReasonDetectedPreviousCleanExit indicates the member detected a previous clean shutdown.
	ReasonDetectedPreviousCleanExit Reason = "DetectedPreviousCleanExit"
	// ReasonDetectedPreviousUncleanExit indicates the member detected a previous unclean shutdown.
	ReasonDetectedPreviousUncleanExit Reason = "DetectedPreviousUncleanExit"
	// ReasonDBValidationFailed indicates DB validation failed.
	ReasonDBValidationFailed Reason = "DBValidationFailed"
	// ReasonDBValidationSucceeded indicates DB validation succeeded.
	ReasonDBValidationSucceeded Reason = "DBValidationSucceeded"
	// ReasonRestorationSucceeded indicates DB restoration from backup succeeded.
	ReasonRestorationSucceeded Reason = "RestorationSucceeded"
	// ReasonWaitingToJoinAsLearner indicates the member is waiting to join the cluster as a learner.
	ReasonWaitingToJoinAsLearner Reason = "WaitingToJoinAsLearner"
	// ReasonJoinedAsLearner indicates the member has joined the cluster as a learner.
	ReasonJoinedAsLearner Reason = "JoinedAsLearner"
	// ReasonPromotedAsVotingMember indicates the learner has been promoted to a voting member.
	ReasonPromotedAsVotingMember Reason = "PromotedAsVotingMember"
	// ReasonGainedClusterLeadership indicates the member won the leader election.
	ReasonGainedClusterLeadership Reason = "GainedClusterLeadership"
	// ReasonLostClusterLeadership indicates the member lost its leader status.
	ReasonLostClusterLeadership Reason = "LostClusterLeadership"
	// ReasonDBCorruptionDetected indicates DB corruption was detected at runtime.
	ReasonDBCorruptionDetected Reason = "DBCorruptionDetected"
)

// MemberState represents the composite state of an etcd member as a (State, SubState) pair.
type MemberState struct {
	// State is the high-level lifecycle state.
	State State `json:"state"`
	// SubState is the detailed sub-state within the high-level state.
	SubState SubState `json:"subState,omitempty"`
}

// Transition records a single state transition with its reason and metadata.
type Transition struct {
	// State is the state after the transition.
	State State `json:"state"`
	// SubState is the sub-state after the transition.
	SubState SubState `json:"subState,omitempty"`
	// Reason is the event that triggered the transition.
	Reason Reason `json:"reason"`
	// TransitionTime is the wall-clock time when the transition occurred.
	TransitionTime time.Time `json:"transitionTime"`
	// Message is an optional human-readable message describing the transition.
	Message string `json:"message,omitempty"`
}
