// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gardener/etcd-steward/internal/statemachine"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// createFakeEtcdMember creates a fake dynamic client with a pre-existing EtcdMember object.
func createFakeEtcdMember(name, namespace string) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "druid.gardener.cloud",
		Version: "v1alpha1",
		Kind:    "EtcdMember",
	})
	existing.SetName(name)
	existing.SetNamespace(namespace)
	return dynamicfake.NewSimpleDynamicClient(scheme, existing)
}

// extractPatchStatus extracts the status map from a patch action.
func extractPatchStatus(t *testing.T, actions []k8stesting.Action) map[string]interface{} {
	t.Helper()
	gvr := schema.GroupVersionResource{
		Group:    "druid.gardener.cloud",
		Version:  "v1alpha1",
		Resource: "etcdmembers",
	}
	for _, a := range actions {
		if a.GetVerb() == "patch" && a.GetResource() == gvr {
			patchAct, ok := a.(interface{ GetPatch() []byte })
			if !ok {
				continue
			}
			var patchBody map[string]interface{}
			if err := json.Unmarshal(patchAct.GetPatch(), &patchBody); err != nil {
				t.Fatalf("failed to unmarshal patch: %v", err)
			}
			status, ok := patchBody["status"].(map[string]interface{})
			if !ok {
				t.Fatal("expected status field in patch")
			}
			return status
		}
	}
	t.Fatal("expected a patch action on etcdmembers resource")
	return nil
}

func TestK8sStateRecorder_WritesRuntimeData(t *testing.T) {
	fakeClient := createFakeEtcdMember("etcd-main-0", "shoot--project--name")
	sm := statemachine.New(true) // single-node

	// Drive the state machine to Started/Leader so status has state/subState.
	sm.Trigger(statemachine.ReasonNewSingleNodeClusterCreated, "")
	sm.Trigger(statemachine.ReasonDetectedPreviousUncleanExit, "")
	sm.Trigger(statemachine.ReasonDBValidationSucceeded, "")

	recorder := NewK8sStateRecorder(fakeClient, sm, zap.NewNop())

	info := MemberInfo{
		ID:          128088275939295631,
		Name:        "etcd-main-0",
		Role:        "Leader",
		DBSize:      24576,
		DBSizeInUse: 20480,
		IsHealthy:   true,
	}

	err := recorder.RecordMemberState(context.Background(), "etcd-main-0", "shoot--project--name", info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := extractPatchStatus(t, fakeClient.Actions())

	// Verify runtime data is present.
	if status["id"] != "128088275939295631" {
		t.Fatalf("expected id '128088275939295631', got %v", status["id"])
	}

	// JSON numbers become float64 when unmarshalling to interface{}.
	if dbSize, ok := status["dbSize"].(float64); !ok || int64(dbSize) != 24576 {
		t.Fatalf("expected dbSize 24576, got %v", status["dbSize"])
	}
	if dbSizeInUse, ok := status["dbSizeInUse"].(float64); !ok || int64(dbSizeInUse) != 20480 {
		t.Fatalf("expected dbSizeInUse 20480, got %v", status["dbSizeInUse"])
	}

	// Verify state machine state is present.
	if status["state"] != "Started" {
		t.Fatalf("expected state 'Started', got %v", status["state"])
	}
	if status["subState"] != "Leader" {
		t.Fatalf("expected subState 'Leader', got %v", status["subState"])
	}
}

func TestK8sStateRecorder_WritesTransitions(t *testing.T) {
	fakeClient := createFakeEtcdMember("etcd-main-0", "shoot--project--name")
	sm := statemachine.New(true) // single-node

	// Drive through multiple transitions.
	sm.Trigger(statemachine.ReasonNewSingleNodeClusterCreated, "steward starting")
	sm.Trigger(statemachine.ReasonDetectedPreviousUncleanExit, "starting validation")
	sm.Trigger(statemachine.ReasonDBValidationSucceeded, "data valid")

	recorder := NewK8sStateRecorder(fakeClient, sm, zap.NewNop())

	info := MemberInfo{
		ID:     1,
		Role:   "Leader",
		DBSize: 1024,
	}

	err := recorder.RecordMemberState(context.Background(), "etcd-main-0", "shoot--project--name", info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := extractPatchStatus(t, fakeClient.Actions())

	// Verify transitions array is present.
	transitions, ok := status["transitions"].([]interface{})
	if !ok {
		t.Fatal("expected transitions array in status")
	}
	if len(transitions) < 3 {
		t.Fatalf("expected at least 3 transitions, got %d", len(transitions))
	}

	// Check the first transition.
	first, ok := transitions[0].(map[string]interface{})
	if !ok {
		t.Fatal("expected transition entry to be a map")
	}
	if first["state"] != "New" {
		t.Fatalf("expected first transition state 'New', got %v", first["state"])
	}
	if first["reason"] != "NewSingleNodeClusterCreated" {
		t.Fatalf("expected first transition reason 'NewSingleNodeClusterCreated', got %v", first["reason"])
	}
}

func TestK8sStateRecorder_TriggersTransitionOnRoleChange(t *testing.T) {
	fakeClient := createFakeEtcdMember("etcd-main-0", "shoot--project--name")
	sm := statemachine.New(true) // single-node

	// Drive to Started/Leader.
	sm.Trigger(statemachine.ReasonNewSingleNodeClusterCreated, "")
	sm.Trigger(statemachine.ReasonDetectedPreviousUncleanExit, "")
	sm.Trigger(statemachine.ReasonDBValidationSucceeded, "")
	// SM is now at Started/Leader.

	recorder := NewK8sStateRecorder(fakeClient, sm, zap.NewNop())

	// Record as Follower -- should trigger LostClusterLeadership transition.
	info := MemberInfo{
		ID:     1,
		Role:   "Follower",
		DBSize: 1024,
	}

	err := recorder.RecordMemberState(context.Background(), "etcd-main-0", "shoot--project--name", info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := extractPatchStatus(t, fakeClient.Actions())

	// After transition, state should still be Started but subState should be Follower.
	if status["state"] != "Started" {
		t.Fatalf("expected state 'Started', got %v", status["state"])
	}
	if status["subState"] != "Follower" {
		t.Fatalf("expected subState 'Follower', got %v", status["subState"])
	}
}

func TestK8sStateRecorder_RepeatedSameRoleStillWritesData(t *testing.T) {
	fakeClient := createFakeEtcdMember("etcd-main-0", "shoot--project--name")
	sm := statemachine.New(true) // single-node

	// Drive to Started/Leader.
	sm.Trigger(statemachine.ReasonNewSingleNodeClusterCreated, "")
	sm.Trigger(statemachine.ReasonDetectedPreviousUncleanExit, "")
	sm.Trigger(statemachine.ReasonDBValidationSucceeded, "")

	recorder := NewK8sStateRecorder(fakeClient, sm, zap.NewNop())

	// Record Leader (no transition since already Leader).
	info := MemberInfo{
		ID:          42,
		Role:        "Leader",
		DBSize:      2048,
		DBSizeInUse: 1024,
	}

	err := recorder.RecordMemberState(context.Background(), "etcd-main-0", "shoot--project--name", info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := extractPatchStatus(t, fakeClient.Actions())

	// Even though no transition happened, runtime data MUST be written.
	if status["id"] != "42" {
		t.Fatalf("expected id '42', got %v", status["id"])
	}
	if dbSize, ok := status["dbSize"].(float64); !ok || int64(dbSize) != 2048 {
		t.Fatalf("expected dbSize 2048, got %v", status["dbSize"])
	}
	if dbSizeInUse, ok := status["dbSizeInUse"].(float64); !ok || int64(dbSizeInUse) != 1024 {
		t.Fatalf("expected dbSizeInUse 1024, got %v", status["dbSizeInUse"])
	}
}

func TestK8sStateRecorder_NoStateMachineStateYet(t *testing.T) {
	fakeClient := createFakeEtcdMember("etcd-main-0", "shoot--project--name")
	sm := statemachine.New(true) // single-node, no transitions triggered

	recorder := NewK8sStateRecorder(fakeClient, sm, zap.NewNop())

	// Record with empty state machine (no transitions yet).
	info := MemberInfo{
		ID:     99,
		Role:   "Learner",
		DBSize: 512,
	}

	err := recorder.RecordMemberState(context.Background(), "etcd-main-0", "shoot--project--name", info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := extractPatchStatus(t, fakeClient.Actions())

	// Runtime data should still be written.
	if status["id"] != "99" {
		t.Fatalf("expected id '99', got %v", status["id"])
	}

	// State should not be present since state machine has not been initialized.
	if _, exists := status["state"]; exists {
		t.Fatal("expected state to be absent when state machine is uninitialized")
	}

	// Transitions should not be present since none have occurred.
	if _, exists := status["transitions"]; exists {
		t.Fatal("expected transitions to be absent when none have occurred")
	}
}

// Verify K8sStateRecorder implements StateRecorder at compile time.
var _ StateRecorder = (*K8sStateRecorder)(nil)
