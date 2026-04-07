// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// State is the top-level state of an etcd member per DEP-04.
type State string

const (
	// StateNew indicates an etcd member that has just been created and not yet processed.
	StateNew State = "New"
	// StateInitializing indicates an etcd member undergoing initialization (DB validation/restoration).
	StateInitializing State = "Initializing"
	// StateStarting indicates an etcd member that is in the process of joining the cluster.
	StateStarting State = "Starting"
	// StateStarted indicates an etcd member that has joined the cluster and is fully operational.
	StateStarted State = "Started"
)

// SubState is the sub-state within a top-level state.
type SubState string

const (
	// SubStateNew is the initial sub-state before any processing has occurred.
	SubStateNew SubState = "New"
	// SubStateDBValidationSanity indicates a sanity DB validation is in progress.
	SubStateDBValidationSanity SubState = "DBValidationSanity"
	// SubStateDBValidationFull indicates a full DB validation is in progress.
	SubStateDBValidationFull SubState = "DBValidationFull"
	// SubStateRestoration indicates an etcd data restoration is in progress.
	SubStateRestoration SubState = "Restoration"
	// SubStatePendingLearner indicates the member is waiting to be added as a learner.
	SubStatePendingLearner SubState = "PendingLearner"
	// SubStateLearner indicates the member has been added to the cluster as a non-voting learner.
	SubStateLearner SubState = "Learner"
	// SubStateFollower indicates the member is an active voting follower in the cluster.
	SubStateFollower SubState = "Follower"
	// SubStateLeader indicates the member is currently the elected cluster leader.
	SubStateLeader SubState = "Leader"
)

// Reason is the reason code for a state transition, per DEP-04.
type Reason string

const (
	// ReasonClusterScaledUp indicates the cluster was scaled up and this member was added.
	ReasonClusterScaledUp Reason = "ClusterScaledUp"
	// ReasonNewSingleNodeClusterCreated indicates a brand-new single-node cluster was created.
	ReasonNewSingleNodeClusterCreated Reason = "NewSingleNodeClusterCreated"
	// ReasonDetectedPreviousCleanExit indicates the member data directory shows a prior clean shutdown.
	ReasonDetectedPreviousCleanExit Reason = "DetectedPreviousCleanExit"
	// ReasonDetectedPreviousUncleanExit indicates the member data directory shows a prior unclean shutdown.
	ReasonDetectedPreviousUncleanExit Reason = "DetectedPreviousUncleanExit"
	// ReasonDBValidationFailed indicates that a DB validation step failed.
	ReasonDBValidationFailed Reason = "DBValidationFailed"
	// ReasonDBValidationSucceeded indicates that a DB validation step passed.
	ReasonDBValidationSucceeded Reason = "DBValidationSucceeded"
	// ReasonRestorationSucceeded indicates that a data restoration completed successfully.
	ReasonRestorationSucceeded Reason = "RestorationSucceeded"
	// ReasonWaitingToJoinAsLearner indicates the member is waiting for the cluster to accept it as a learner.
	ReasonWaitingToJoinAsLearner Reason = "WaitingToJoinAsLearner"
	// ReasonJoinedAsLearner indicates the member successfully joined the cluster as a non-voting learner.
	ReasonJoinedAsLearner Reason = "JoinedAsLearner"
	// ReasonPromotedAsVotingMember indicates the member was promoted from learner to a full voting member.
	ReasonPromotedAsVotingMember Reason = "PromotedAsVotingMember"
	// ReasonGainedClusterLeadership indicates the member was elected as the new cluster leader.
	ReasonGainedClusterLeadership Reason = "GainedClusterLeadership"
	// ReasonLostClusterLeadership indicates the member stepped down or lost its leader role.
	ReasonLostClusterLeadership Reason = "LostClusterLeadership"
	// ReasonDataLossRecoveryStarted indicates that data-loss recovery has begun for the member.
	ReasonDataLossRecoveryStarted Reason = "DataLossRecoveryStarted"
)

// Transition captures a single state transition of an etcd member.
type Transition struct {
	// State is the top-level state the member transitioned to.
	State State `json:"state"`
	// SubState is the optional sub-state within the top-level state.
	SubState *SubState `json:"subState,omitempty"`
	// Reason is the reason code for the transition.
	Reason Reason `json:"reason"`
	// TransitionTime is when the transition occurred.
	TransitionTime metav1.Time `json:"transitionTime"`
	// Message is an optional human-readable description of the transition.
	Message string `json:"message,omitempty"`
}
