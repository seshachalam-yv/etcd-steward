// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

// mockDynamicClient implements dynamic.Interface for testing.
type mockDynamicClient struct {
	resource *mockResourceClient
}

func (m *mockDynamicClient) Resource(_ schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return m.resource
}

// mockResourceClient implements dynamic.NamespaceableResourceInterface for testing.
type mockResourceClient struct {
	getObject  *unstructured.Unstructured
	patchCalls []patchCall
	patchError error
}

type patchCall struct {
	name        string
	patchType   types.PatchType
	data        []byte
	subresource string
}

func (m *mockResourceClient) Namespace(_ string) dynamic.ResourceInterface {
	return m
}

func (m *mockResourceClient) Get(_ context.Context, _ string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	if m.getObject == nil {
		obj := &unstructured.Unstructured{}
		obj.SetName("test-member")
		obj.SetNamespace("default")
		return obj, nil
	}
	return m.getObject.DeepCopy(), nil
}

func (m *mockResourceClient) Patch(_ context.Context, name string, pt types.PatchType, data []byte, _ metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error) {
	sub := ""
	if len(subresources) > 0 {
		sub = subresources[0]
	}
	m.patchCalls = append(m.patchCalls, patchCall{
		name:        name,
		patchType:   pt,
		data:        data,
		subresource: sub,
	})
	if m.patchError != nil {
		return nil, m.patchError
	}
	result := &unstructured.Unstructured{}
	if err := result.UnmarshalJSON(data); err != nil {
		return &unstructured.Unstructured{}, nil
	}
	return result, nil
}

func (m *mockResourceClient) Create(_ context.Context, _ *unstructured.Unstructured, _ metav1.CreateOptions, _ ...string) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (m *mockResourceClient) Update(_ context.Context, _ *unstructured.Unstructured, _ metav1.UpdateOptions, _ ...string) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (m *mockResourceClient) UpdateStatus(_ context.Context, _ *unstructured.Unstructured, _ metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (m *mockResourceClient) Delete(_ context.Context, _ string, _ metav1.DeleteOptions, _ ...string) error {
	panic("not implemented")
}
func (m *mockResourceClient) DeleteCollection(_ context.Context, _ metav1.DeleteOptions, _ metav1.ListOptions) error {
	panic("not implemented")
}
func (m *mockResourceClient) List(_ context.Context, _ metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	panic("not implemented")
}
func (m *mockResourceClient) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	panic("not implemented")
}
func (m *mockResourceClient) Apply(_ context.Context, _ string, _ *unstructured.Unstructured, _ metav1.ApplyOptions, _ ...string) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (m *mockResourceClient) ApplyStatus(_ context.Context, _ string, _ *unstructured.Unstructured, _ metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	panic("not implemented")
}

func newMockRecorder() (*K8sRecorder, *mockResourceClient) {
	rc := &mockResourceClient{}
	mc := &mockDynamicClient{resource: rc}
	r := &K8sRecorder{client: mc}
	return r, rc
}

func decodePatch(t *testing.T, data []byte) memberPatch {
	t.Helper()
	var p memberPatch
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("failed to decode patch: %v", err)
	}
	return p
}

func TestRecord_SingleTransition(t *testing.T) {
	recorder, rc := newMockRecorder()

	sub := SubStateFollower
	tr := Transition{
		State:          StateStarted,
		SubState:       &sub,
		Reason:         ReasonPromotedAsVotingMember,
		TransitionTime: metav1.NewTime(time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)),
		Message:        "member promoted",
	}

	err := recorder.Record(context.Background(), "etcd-main-0", "default", tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}

	call := rc.patchCalls[0]
	if call.name != "etcd-main-0" {
		t.Errorf("expected patch name etcd-main-0, got %s", call.name)
	}
	if call.patchType != types.MergePatchType {
		t.Errorf("expected MergePatchType, got %v", call.patchType)
	}
	if call.subresource != "status" {
		t.Errorf("expected status subresource, got %q", call.subresource)
	}

	p := decodePatch(t, call.data)
	if len(p.Status.Transitions) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(p.Status.Transitions))
	}

	got := p.Status.Transitions[0]
	if got.State != "Started" {
		t.Errorf("expected state Started, got %s", got.State)
	}
	if got.SubState == nil || *got.SubState != "Follower" {
		t.Errorf("expected subState Follower, got %v", got.SubState)
	}
	if got.Reason != "PromotedAsVotingMember" {
		t.Errorf("expected reason PromotedAsVotingMember, got %s", got.Reason)
	}
	if got.TransitionTime != "2025-01-02T03:04:05Z" {
		t.Errorf("expected transitionTime 2025-01-02T03:04:05Z, got %s", got.TransitionTime)
	}
	if got.Message == nil || *got.Message != "member promoted" {
		t.Errorf("expected message 'member promoted', got %v", got.Message)
	}
}

func TestRecord_UsesCurrentTimeWhenTransitionTimeIsZero(t *testing.T) {
	recorder, rc := newMockRecorder()

	before := time.Now().UTC().Truncate(time.Second)

	tr := Transition{
		State:  StateNew,
		Reason: ReasonClusterScaledUp,
	}

	err := recorder.Record(context.Background(), "etcd-main-0", "default", tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}

	p := decodePatch(t, rc.patchCalls[0].data)
	if len(p.Status.Transitions) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(p.Status.Transitions))
	}

	parsed, parseErr := time.Parse(time.RFC3339, p.Status.Transitions[0].TransitionTime)
	if parseErr != nil {
		t.Fatalf("failed to parse transition time: %v", parseErr)
	}

	after := time.Now().UTC().Add(time.Second)
	if parsed.UTC().Before(before) {
		t.Errorf("transition time %v is before %v", parsed, before)
	}
	if parsed.UTC().After(after) {
		t.Errorf("transition time %v is after %v", parsed, after)
	}
}

func TestRecord_PrunesToMaxTransitions(t *testing.T) {
	recorder, rc := newMockRecorder()

	// Pre-populate getObject with 100 existing transitions.
	existing := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		existing[i] = map[string]interface{}{
			"state":          "Started",
			"reason":         "PromotedAsVotingMember",
			"transitionTime": time.Now().UTC().Format(time.RFC3339),
		}
	}
	obj := &unstructured.Unstructured{}
	obj.SetName("etcd-main-0")
	obj.SetNamespace("default")
	_ = unstructured.SetNestedSlice(obj.Object, existing, "status", "transitions")
	rc.getObject = obj

	tr := Transition{
		State:          StateStarted,
		Reason:         ReasonGainedClusterLeadership,
		TransitionTime: metav1.NewTime(time.Now().UTC()),
		Message:        "new leader",
	}

	err := recorder.Record(context.Background(), "etcd-main-0", "default", tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}

	p := decodePatch(t, rc.patchCalls[0].data)
	if len(p.Status.Transitions) != maxTransitions {
		t.Fatalf("expected %d transitions, got %d", maxTransitions, len(p.Status.Transitions))
	}

	last := p.Status.Transitions[maxTransitions-1]
	if last.Reason != "GainedClusterLeadership" {
		t.Errorf("expected last reason GainedClusterLeadership, got %s", last.Reason)
	}
	if last.Message == nil || *last.Message != "new leader" {
		t.Errorf("expected last message 'new leader', got %v", last.Message)
	}
}

func TestNoOpRecorder_ReturnsNil(t *testing.T) {
	recorder := NewNoopRecorder()

	err := recorder.Record(context.Background(), "etcd-main-0", "default", Transition{
		State:  StateNew,
		Reason: ReasonClusterScaledUp,
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRecord_SubStateOmittedWhenNil(t *testing.T) {
	recorder, rc := newMockRecorder()

	tr := Transition{
		State:          StateNew,
		SubState:       nil,
		Reason:         ReasonNewSingleNodeClusterCreated,
		TransitionTime: metav1.NewTime(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)),
	}

	err := recorder.Record(context.Background(), "etcd-main-0", "default", tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}

	// Verify the raw JSON does not contain "subState".
	raw := rc.patchCalls[0].data
	var rawMap map[string]interface{}
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		t.Fatalf("failed to unmarshal raw patch: %v", err)
	}
	statusMap, ok := rawMap["status"].(map[string]interface{})
	if !ok {
		t.Fatal("expected status map in patch")
	}
	transitions, ok := statusMap["transitions"].([]interface{})
	if !ok {
		t.Fatal("expected transitions array in patch")
	}
	if len(transitions) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(transitions))
	}
	firstTrans, ok := transitions[0].(map[string]interface{})
	if !ok {
		t.Fatal("expected first transition to be a map")
	}
	if _, hasSubState := firstTrans["subState"]; hasSubState {
		t.Error("subState should be absent from JSON when nil")
	}
}
