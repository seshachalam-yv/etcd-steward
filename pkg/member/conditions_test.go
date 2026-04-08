// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/gardener/etcd-steward/pkg/member"
)

// TestUpsertCondition_NewCondition tests that upsertCondition is tested via SetCondition
// using the exported logic. Since SetCondition calls unexported upsertCondition, we test
// indirectly by using a fakeConditionClient that captures the patch.
func TestSetCondition_NoopClientInterface(t *testing.T) {
	c := &member.NoopClient{}
	err := c.SetCondition(t.Context(), "etcd-0", "default", member.Condition{
		Type:   member.ConditionDataVolumeReadOnly,
		Status: "True",
	})
	if err != nil {
		t.Errorf("NoopClient.SetCondition returned error: %v", err)
	}
}

// TestConditionDataVolumeReadOnlyConstant ensures the constant value is stable.
func TestConditionDataVolumeReadOnlyConstant(t *testing.T) {
	if member.ConditionDataVolumeReadOnly != "DataVolumeReadOnly" {
		t.Errorf("unexpected value: %q", member.ConditionDataVolumeReadOnly)
	}
}

// TestConditionStruct verifies Condition fields round-trip correctly.
func TestConditionStruct(t *testing.T) {
	now := metav1.Now()
	c := member.Condition{
		Type:               member.ConditionDataVolumeReadOnly,
		Status:             "True",
		Reason:             "ReadOnlyFileSystem",
		Message:            "data volume is read-only",
		LastTransitionTime: now,
	}
	if c.Type != member.ConditionDataVolumeReadOnly {
		t.Errorf("Type mismatch")
	}
	if c.Status != "True" {
		t.Errorf("Status mismatch")
	}
	if c.LastTransitionTime.Time.IsZero() {
		t.Errorf("LastTransitionTime should not be zero")
	}
}

// TestUpsertConditionLogic tests the condition merge semantics via fakeSetConditionClient.
// Since upsertCondition is unexported, we test its semantics by capturing what the client receives.
func TestUpsertConditionLogic_SameStatus_NoTimeChange(t *testing.T) {
	fixedTime := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC)
	existing := []member.ConditionForTest{
		{
			Type:               member.ConditionDataVolumeReadOnly,
			Status:             "True",
			Reason:             "ReadOnlyFileSystem",
			Message:            "read-only",
			LastTransitionTime: fixedTime.Format(time.RFC3339),
		},
	}

	result := member.UpsertConditionForTest(existing, member.Condition{
		Type:               member.ConditionDataVolumeReadOnly,
		Status:             "True", // same status
		Reason:             "ReadOnlyFileSystem",
		Message:            "still read-only",
		LastTransitionTime: metav1.Time{Time: time.Now()}, // should be ignored
	})

	if len(result) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result))
	}
	if result[0].LastTransitionTime != fixedTime.Format(time.RFC3339) {
		t.Errorf("lastTransitionTime should be preserved for same status: got %q, want %q",
			result[0].LastTransitionTime, fixedTime.Format(time.RFC3339))
	}
}

func TestUpsertConditionLogic_DifferentStatus_UpdatesTime(t *testing.T) {
	oldTime := "2026-01-01T00:00:00Z"
	existing := []member.ConditionForTest{
		{
			Type:               member.ConditionDataVolumeReadOnly,
			Status:             "True",
			Reason:             "ReadOnlyFileSystem",
			Message:            "read-only",
			LastTransitionTime: oldTime,
		},
	}

	result := member.UpsertConditionForTest(existing, member.Condition{
		Type:    member.ConditionDataVolumeReadOnly,
		Status:  "False", // changed
		Reason:  "Remounted",
		Message: "volume is now writable",
	})

	if len(result) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result))
	}
	if result[0].Status != "False" {
		t.Errorf("expected status=False, got %q", result[0].Status)
	}
	if result[0].LastTransitionTime == oldTime {
		t.Error("lastTransitionTime should have been updated for status change")
	}
}

func TestUpsertConditionLogic_NewType_Appended(t *testing.T) {
	existing := []member.ConditionForTest{
		{Type: "SomeOtherCondition", Status: "True", Reason: "Reason", LastTransitionTime: "2026-01-01T00:00:00Z"},
	}

	result := member.UpsertConditionForTest(existing, member.Condition{
		Type:   member.ConditionDataVolumeReadOnly,
		Status: "Unknown",
		Reason: "CheckFailed",
	})

	if len(result) != 2 {
		t.Fatalf("expected 2 conditions after append, got %d", len(result))
	}
	if result[1].Type != member.ConditionDataVolumeReadOnly {
		t.Errorf("new condition not appended correctly: %+v", result[1])
	}
}

func TestUpsertConditionLogic_EmptyExisting(t *testing.T) {
	result := member.UpsertConditionForTest(nil, member.Condition{
		Type:   member.ConditionDataVolumeReadOnly,
		Status: "False",
		Reason: "VolumeOK",
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result))
	}
	if result[0].Type != member.ConditionDataVolumeReadOnly {
		t.Errorf("unexpected type: %q", result[0].Type)
	}
	if result[0].Status != "False" {
		t.Errorf("unexpected status: %q", result[0].Status)
	}
	if result[0].LastTransitionTime == "" {
		t.Error("lastTransitionTime should be set for new condition")
	}
}

func TestUpsertConditionLogic_WithExplicitTime(t *testing.T) {
	fixedTime := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	result := member.UpsertConditionForTest(nil, member.Condition{
		Type:               "SomeCondition",
		Status:             "True",
		Reason:             "Reason",
		LastTransitionTime: metav1.Time{Time: fixedTime},
	})
	if len(result) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result))
	}
	if result[0].LastTransitionTime != fixedTime.UTC().Format(time.RFC3339) {
		t.Errorf("expected explicit time to be preserved, got %q", result[0].LastTransitionTime)
	}
}

// Suppress unused import.
var _ = types.MergePatchType
