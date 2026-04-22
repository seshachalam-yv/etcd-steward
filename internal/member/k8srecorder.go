// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
	"github.com/gardener/etcd-steward/internal/statemachine"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// etcdMemberGVR is the GroupVersionResource for the EtcdMember custom resource.
var etcdMemberGVR = schema.GroupVersionResource{
	Group:    "druid.gardener.cloud",
	Version:  "v1alpha1",
	Resource: "etcdmembers",
}

// K8sStateRecorder implements StateRecorder by patching the EtcdMember status
// sub-resource via the Kubernetes dynamic client. It writes both the runtime
// data (id, dbSize, dbSizeInUse, state, subState) and triggers state machine
// transitions when the member role changes.
type K8sStateRecorder struct {
	client dynamic.Interface
	sm     *statemachine.StateMachine
	logger *zap.Logger
}

// NewK8sStateRecorder creates a StateRecorder that writes to the EtcdMember
// status sub-resource and triggers state machine transitions on role changes.
func NewK8sStateRecorder(
	client dynamic.Interface,
	sm *statemachine.StateMachine,
	logger *zap.Logger,
) *K8sStateRecorder {
	return &K8sStateRecorder{
		client: client,
		sm:     sm,
		logger: logger,
	}
}

// RecordMemberState patches the EtcdMember status sub-resource with the current
// runtime data AND handles state machine transitions for role changes.
func (r *K8sStateRecorder) RecordMemberState(ctx context.Context, memberName, namespace string, info MemberInfo) error {
	// Attempt a state machine transition based on the member role.
	r.attemptTransition(info)

	// Get current state from state machine.
	current := r.sm.Current()

	// Build the status patch that matches the EtcdMember CRD schema.
	statusPatch := map[string]interface{}{
		"id":          fmt.Sprintf("%d", info.ID),
		"dbSize":      info.DBSize,
		"dbSizeInUse": info.DBSizeInUse,
	}

	// Set state and subState from the state machine if it has been initialized.
	if current.State != "" {
		statusPatch["state"] = string(current.State)
	}
	if current.SubState != "" {
		statusPatch["subState"] = string(current.SubState)
	}

	// Build the transitions array from the state machine history.
	smTransitions := r.sm.Transitions()
	if len(smTransitions) > 0 {
		transitions := make([]map[string]interface{}, 0, len(smTransitions))
		for _, t := range smTransitions {
			entry := map[string]interface{}{
				"state":          string(t.State),
				"reason":         string(t.Reason),
				"transitionTime": t.TransitionTime.UTC().Format(time.RFC3339),
			}
			if t.SubState != statemachine.SubStateNone {
				entry["subState"] = string(t.SubState)
			}
			if t.Message != "" {
				entry["message"] = t.Message
			}
			transitions = append(transitions, entry)
		}
		statusPatch["transitions"] = transitions
	}

	patch := map[string]interface{}{
		"status": statusPatch,
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return errors.Wrap(errors.ErrCodeInternal, "failed to marshal EtcdMember status patch", err)
	}

	_, err = r.client.Resource(etcdMemberGVR).Namespace(namespace).Patch(
		ctx,
		memberName,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"status",
	)
	if err != nil {
		return errors.Wrap(
			errors.ErrCodeNetwork,
			fmt.Sprintf("failed to patch EtcdMember %s/%s status", namespace, memberName),
			err,
		)
	}

	return nil
}

// attemptTransition tries to trigger a state machine transition based on the
// member role. Transitions that are invalid from the current state (e.g.,
// repeated Leader -> Leader) are silently ignored.
func (r *K8sStateRecorder) attemptTransition(info MemberInfo) {
	var reason statemachine.Reason
	switch info.Role {
	case "Leader":
		reason = statemachine.ReasonGainedClusterLeadership
	case "Follower":
		reason = statemachine.ReasonLostClusterLeadership
	default:
		// For Learner or other roles, no transition to attempt.
		return
	}

	if _, err := r.sm.Trigger(reason, ""); err != nil {
		// Transition not valid from current state -- expected for repeated
		// same-role updates. Debug-level only to avoid log spam.
		r.logger.Debug("state machine transition not applicable",
			zap.String("role", info.Role),
			zap.String("reason", string(reason)),
			zap.Error(err),
		)
	}
}

// Compile-time assertion that K8sStateRecorder implements StateRecorder.
var _ StateRecorder = (*K8sStateRecorder)(nil)
