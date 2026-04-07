// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package statemachine provides types and a Kubernetes recorder for DEP-04 state machine transitions.
package statemachine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// maxTransitions is the maximum number of transitions retained in EtcdMember.status.transitions.
const maxTransitions = 100

// etcdMemberGVR is the GroupVersionResource for EtcdMember CRD objects.
var etcdMemberGVR = schema.GroupVersionResource{
	Group:    "druid.gardener.cloud",
	Version:  "v1alpha1",
	Resource: "etcdmembers",
}

// Recorder records state transitions to the EtcdMember resource's status.
type Recorder interface {
	// Record writes a transition synchronously to EtcdMember.status.transitions.
	Record(ctx context.Context, memberName, namespace string, t Transition) error
}

// patchTransition is the JSON representation of a Transition used in the status patch.
type patchTransition struct {
	State          string  `json:"state"`
	SubState       *string `json:"subState,omitempty"`
	Reason         string  `json:"reason"`
	TransitionTime string  `json:"transitionTime"`
	Message        *string `json:"message,omitempty"`
}

// statusPatch is the JSON representation of EtcdMember.status used in the patch payload.
type statusPatch struct {
	Transitions []patchTransition `json:"transitions"`
}

// memberPatch is the top-level JSON patch payload for the EtcdMember status subresource.
type memberPatch struct {
	Status statusPatch `json:"status"`
}

// K8sRecorder writes Transition records to EtcdMember.status.transitions via the dynamic client.
type K8sRecorder struct {
	client dynamic.Interface
}

// NewK8sRecorder creates a Recorder that writes transitions to EtcdMember resources
// using the provided dynamic client.
func NewK8sRecorder(dynamicClient dynamic.Interface) Recorder {
	return &K8sRecorder{client: dynamicClient}
}

// Record fetches the current EtcdMember, appends the new transition (pruning to maxTransitions),
// and patches the status subresource.
func (r *K8sRecorder) Record(ctx context.Context, memberName, namespace string, t Transition) error {
	resource := r.client.Resource(etcdMemberGVR).Namespace(namespace)

	// GET current object to read existing transitions.
	obj, err := resource.Get(ctx, memberName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get EtcdMember %s/%s: %w", namespace, memberName, err)
	}

	// Extract existing transitions from the unstructured object.
	existing, err := extractTransitions(obj.Object)
	if err != nil {
		return fmt.Errorf("failed to extract transitions from EtcdMember %s/%s: %w", namespace, memberName, err)
	}

	// Build the new patchTransition entry.
	pt := buildPatchTransition(t)

	// Append and prune to maxTransitions.
	updated := append(existing, pt)
	if len(updated) > maxTransitions {
		updated = updated[len(updated)-maxTransitions:]
	}

	// Build patch payload.
	payload := memberPatch{
		Status: statusPatch{
			Transitions: updated,
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal status patch for EtcdMember %s/%s: %w", namespace, memberName, err)
	}

	// Patch the status subresource.
	_, err = resource.Patch(ctx, memberName, types.MergePatchType, data, metav1.PatchOptions{}, "status")
	if err != nil {
		return fmt.Errorf("failed to patch EtcdMember %s/%s status: %w", namespace, memberName, err)
	}
	return nil
}

// buildPatchTransition converts a Transition to the JSON-serialisable patchTransition.
// If t.TransitionTime is zero, time.Now() is used.
func buildPatchTransition(t Transition) patchTransition {
	ts := t.TransitionTime.Time
	if ts.IsZero() {
		ts = time.Now()
	}

	pt := patchTransition{
		State:          string(t.State),
		Reason:         string(t.Reason),
		TransitionTime: ts.UTC().Format(time.RFC3339),
	}
	if t.SubState != nil {
		s := string(*t.SubState)
		pt.SubState = &s
	}
	if t.Message != "" {
		msg := t.Message
		pt.Message = &msg
	}
	return pt
}

// extractTransitions reads the status.transitions slice from an unstructured EtcdMember object.
// It tolerates a missing or empty status.transitions field by returning an empty slice.
func extractTransitions(obj map[string]interface{}) ([]patchTransition, error) {
	status, ok := obj["status"]
	if !ok {
		return nil, nil
	}
	statusMap, ok := status.(map[string]interface{})
	if !ok {
		return nil, nil
	}
	transitionsRaw, ok := statusMap["transitions"]
	if !ok {
		return nil, nil
	}

	// Re-encode to JSON and decode into []patchTransition for a safe round-trip.
	raw, err := json.Marshal(transitionsRaw)
	if err != nil {
		return nil, fmt.Errorf("marshal existing transitions: %w", err)
	}
	var result []patchTransition
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("unmarshal existing transitions: %w", err)
	}
	return result, nil
}

// NoopRecorder discards all transitions. Useful for unit tests and environments
// where EtcdMember resources are not available.
type NoopRecorder struct{}

// NewNoopRecorder creates a Recorder that discards all transitions (for testing).
func NewNoopRecorder() Recorder {
	return &NoopRecorder{}
}

// Record does nothing and returns nil.
func (n *NoopRecorder) Record(_ context.Context, _, _ string, _ Transition) error {
	return nil
}
