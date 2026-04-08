// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// ConditionDataVolumeReadOnly is the condition type indicating the etcd data volume is read-only.
	ConditionDataVolumeReadOnly = "DataVolumeReadOnly"
)

// Condition represents a condition in EtcdMember.status.conditions.
type Condition struct {
	// Type is the condition type (e.g. "DataVolumeReadOnly").
	Type string
	// Status is "True", "False", or "Unknown".
	Status string
	// Reason is a machine-readable reason code.
	Reason string
	// Message is a human-readable description.
	Message string
	// LastTransitionTime is when the condition last transitioned.
	LastTransitionTime metav1.Time
}

// conditionPatch is the JSON representation of a condition used in merge-patch.
type conditionPatch struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message"`
	LastTransitionTime string `json:"lastTransitionTime"`
}

// conditionsStatusPatch is the patch body for EtcdMember.status.conditions.
type conditionsStatusPatch struct {
	Conditions []conditionPatch `json:"conditions"`
}

// conditionsMemberPatch is the top-level patch for the status subresource.
type conditionsMemberPatch struct {
	Status conditionsStatusPatch `json:"status"`
}

// SetCondition patches EtcdMember.status.conditions, creating or updating the given condition.
// If the condition type already exists with the same status, lastTransitionTime is preserved.
// If the status differs, lastTransitionTime is updated to now.
func (c *K8sMemberClient) SetCondition(ctx context.Context, memberName, namespace string, cond Condition) error {
	resource := c.client.Resource(etcdMemberGVR).Namespace(namespace)

	obj, err := resource.Get(ctx, memberName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get EtcdMember %s/%s: %w", namespace, memberName, err)
	}

	existing := extractConditions(obj.Object)

	updated := upsertCondition(existing, cond)

	patch := conditionsMemberPatch{
		Status: conditionsStatusPatch{Conditions: updated},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal conditions patch for EtcdMember %s/%s: %w", namespace, memberName, err)
	}

	_, err = resource.Patch(ctx, memberName, types.MergePatchType, data, metav1.PatchOptions{}, "status")
	if err != nil {
		return fmt.Errorf("failed to patch EtcdMember %s/%s conditions: %w", namespace, memberName, err)
	}
	return nil
}

// SetCondition does nothing.
func (n *NoopClient) SetCondition(_ context.Context, _, _ string, _ Condition) error {
	return nil
}

// extractConditions reads status.conditions from an unstructured EtcdMember object.
func extractConditions(obj map[string]interface{}) []conditionPatch {
	status, ok := obj["status"]
	if !ok {
		return nil
	}
	statusMap, ok := status.(map[string]interface{})
	if !ok {
		return nil
	}
	raw, ok := statusMap["conditions"]
	if !ok {
		return nil
	}

	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var result []conditionPatch
	if err := json.Unmarshal(b, &result); err != nil {
		return nil
	}
	return result
}

// upsertCondition inserts or updates a condition in the slice.
// If an existing entry with the same Type is found:
//   - Same Status → keep old LastTransitionTime (idempotent)
//   - Different Status → update LastTransitionTime to now
//
// If no existing entry found, set LastTransitionTime to cond.LastTransitionTime (or now).
func upsertCondition(existing []conditionPatch, cond Condition) []conditionPatch {
	ts := cond.LastTransitionTime.UTC().Format(time.RFC3339)
	if cond.LastTransitionTime.IsZero() {
		ts = time.Now().UTC().Format(time.RFC3339)
	}

	newEntry := conditionPatch{
		Type:               cond.Type,
		Status:             cond.Status,
		Reason:             cond.Reason,
		Message:            cond.Message,
		LastTransitionTime: ts,
	}

	for i, e := range existing {
		if e.Type == cond.Type {
			if e.Status == cond.Status {
				// Same status — preserve original LastTransitionTime.
				newEntry.LastTransitionTime = e.LastTransitionTime
			}
			// Replace in place.
			updated := make([]conditionPatch, len(existing))
			copy(updated, existing)
			updated[i] = newEntry
			return updated
		}
	}

	// New condition — append.
	return append(existing, newEntry)
}

// ConditionForTest is a test-helper type that mirrors conditionPatch for white-box testing.
// It is exported solely to allow _test packages to exercise upsertCondition logic.
type ConditionForTest struct {
	Type               string
	Status             string
	Reason             string
	Message            string
	LastTransitionTime string
}

// UpsertConditionForTest exposes upsertCondition for testing.
// It converts ConditionForTest ↔ conditionPatch so tests can exercise the merge logic
// without depending on the K8s dynamic client.
func UpsertConditionForTest(existing []ConditionForTest, cond Condition) []ConditionForTest {
	patches := make([]conditionPatch, len(existing))
	for i, e := range existing {
		patches[i] = conditionPatch(e)
	}
	result := upsertCondition(patches, cond)
	out := make([]ConditionForTest, len(result))
	for i, r := range result {
		out[i] = ConditionForTest(r)
	}
	return out
}
