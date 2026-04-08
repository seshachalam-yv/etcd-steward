// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	"github.com/gardener/etcd-steward/pkg/statemachine"
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
	obj := &unstructured.Unstructured{}
	obj.SetName("test-member")
	obj.SetNamespace("default")
	return obj, nil
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

func newMockClient() (*K8sMemberClient, *mockResourceClient) {
	rc := &mockResourceClient{}
	mc := &mockDynamicClient{resource: rc}
	c := &K8sMemberClient{client: mc}
	return c, rc
}

func TestUpdateStatus_SetsIDAndClusterID(t *testing.T) {
	client, rc := newMockClient()

	id := "abc123"
	clusterID := "def456"

	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{
		MemberID:  &id,
		ClusterID: &clusterID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}

	call := rc.patchCalls[0]
	if call.name != "etcd-main-0" {
		t.Errorf("expected name etcd-main-0, got %s", call.name)
	}
	if call.patchType != types.MergePatchType {
		t.Errorf("expected MergePatchType, got %v", call.patchType)
	}
	if call.subresource != "status" {
		t.Errorf("expected status subresource, got %q", call.subresource)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(call.data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}

	statusMap, ok := patch["status"].(map[string]interface{})
	if !ok {
		t.Fatal("expected status map in patch")
	}
	if statusMap["id"] != "abc123" {
		t.Errorf("expected id abc123, got %v", statusMap["id"])
	}
	if statusMap["clusterID"] != "def456" {
		t.Errorf("expected clusterID def456, got %v", statusMap["clusterID"])
	}
}

func TestUpdateStatus_OnlyNonNilFieldsIncluded(t *testing.T) {
	client, rc := newMockClient()

	id := "only-id"
	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{
		MemberID: &id,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}

	statusMap := patch["status"].(map[string]interface{})
	if len(statusMap) != 1 {
		t.Errorf("expected 1 field in status, got %d", len(statusMap))
	}
	if _, has := statusMap["clusterID"]; has {
		t.Error("clusterID should be absent when nil")
	}
}

func TestUpdateStatus_EmptyOpts(t *testing.T) {
	client, rc := newMockClient()

	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}

	statusMap := patch["status"].(map[string]interface{})
	if len(statusMap) != 0 {
		t.Errorf("expected empty status map, got %d fields", len(statusMap))
	}
}

func TestRemoveCreateAsLearnerAnnotation(t *testing.T) {
	client, rc := newMockClient()

	err := client.RemoveCreateAsLearnerAnnotation(context.Background(), "etcd-main-0", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}

	call := rc.patchCalls[0]
	if call.name != "etcd-main-0" {
		t.Errorf("expected name etcd-main-0, got %s", call.name)
	}
	if call.patchType != types.MergePatchType {
		t.Errorf("expected MergePatchType, got %v", call.patchType)
	}
	// Must NOT use the status subresource
	if call.subresource != "" {
		t.Errorf("expected empty subresource, got %q", call.subresource)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(call.data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}

	metaMap, ok := patch["metadata"].(map[string]interface{})
	if !ok {
		t.Fatal("expected metadata map in patch")
	}

	annotations, ok := metaMap["annotations"].(map[string]interface{})
	if !ok {
		t.Fatal("expected annotations map")
	}

	val, exists := annotations[CreateAsLearnerAnnotation]
	if !exists {
		t.Error("annotation key must be present in patch")
	}
	if val != nil {
		t.Errorf("annotation value must be null, got %v", val)
	}
}

// errPatchResourceClient always returns an error from Patch.
type errPatchResourceClient struct {
	mockResourceClient
}

func (e *errPatchResourceClient) Namespace(_ string) dynamic.ResourceInterface {
	return e
}

func (e *errPatchResourceClient) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, _ metav1.PatchOptions, _ ...string) (*unstructured.Unstructured, error) {
	return nil, fmt.Errorf("patch failed: not found")
}

type errDynamicClient struct {
	resource *errPatchResourceClient
}

func (e *errDynamicClient) Resource(_ schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return e.resource
}

func newErrMockClient() *K8sMemberClient {
	rc := &errPatchResourceClient{}
	mc := &errDynamicClient{resource: rc}
	return &K8sMemberClient{client: mc}
}

func TestUpdateStatus_PatchError(t *testing.T) {
	client := newErrMockClient()

	id := "abc123"
	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{
		MemberID: &id,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !containsString(err.Error(), "failed to patch") {
		t.Errorf("expected error to contain 'failed to patch', got: %v", err)
	}
}

func TestRemoveCreateAsLearnerAnnotation_PatchError(t *testing.T) {
	client := newErrMockClient()

	err := client.RemoveCreateAsLearnerAnnotation(context.Background(), "etcd-main-0", "default")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !containsString(err.Error(), "failed to patch") {
		t.Errorf("expected error to contain 'failed to patch', got: %v", err)
	}
}

func TestNoopClient_UpdateStatus(t *testing.T) {
	c := &NoopClient{}
	err := c.UpdateStatus(context.Background(), "member", "ns", UpdateStatusOpts{})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestNoopClient_RemoveCreateAsLearnerAnnotation(t *testing.T) {
	c := &NoopClient{}
	err := c.RemoveCreateAsLearnerAnnotation(context.Background(), "member", "ns")
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestNewK8sClient_ReturnsClient(t *testing.T) {
	rc := &mockResourceClient{}
	mc := &mockDynamicClient{resource: rc}
	c := NewK8sClient(mc)
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestUpdateStatus_WithLastTransition(t *testing.T) {
	client, rc := newMockClient()

	id := "abc123"
	tr := &statemachine.Transition{
		State:  statemachine.StateStarted,
		Reason: statemachine.ReasonJoinedAsLearner,
	}

	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{
		MemberID:       &id,
		LastTransition: tr,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}

	statusMap := patch["status"].(map[string]interface{})
	if _, has := statusMap["lastTransition"]; !has {
		t.Error("expected lastTransition in patch status")
	}
}

func TestUpdateStatus_PeerTLSEnabled_True(t *testing.T) {
	client, rc := newMockClient()

	enabled := true
	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{
		PeerTLSEnabled: &enabled,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}

	statusMap := patch["status"].(map[string]interface{})
	val, has := statusMap["peerTLSEnabled"]
	if !has {
		t.Fatal("expected peerTLSEnabled in patch status")
	}
	if val != true {
		t.Errorf("expected peerTLSEnabled=true, got %v", val)
	}
}

func TestUpdateStatus_PeerTLSEnabled_Nil(t *testing.T) {
	client, rc := newMockClient()

	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}

	statusMap := patch["status"].(map[string]interface{})
	if _, has := statusMap["peerTLSEnabled"]; has {
		t.Error("peerTLSEnabled should be absent when nil")
	}
}

func TestPatchStatus_CallsStatusSubresource(t *testing.T) {
	client, rc := newMockClient()

	data := []byte(`{"status":{"dbSize":"1Gi"}}`)
	err := client.PatchStatus(context.Background(), "etcd-main-0", "default", types.MergePatchType, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}
	call := rc.patchCalls[0]
	if call.subresource != "status" {
		t.Errorf("expected status subresource, got %q", call.subresource)
	}
	if call.patchType != types.MergePatchType {
		t.Errorf("expected MergePatchType, got %v", call.patchType)
	}
}

func TestNoopClient_PatchStatus(t *testing.T) {
	c := &NoopClient{}
	err := c.PatchStatus(context.Background(), "member", "ns", types.MergePatchType, []byte(`{}`))
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestEtcdMemberGVR_Values(t *testing.T) {
	gvr := EtcdMemberGVR()
	if gvr.Group != "druid.gardener.cloud" {
		t.Errorf("unexpected Group: %q", gvr.Group)
	}
	if gvr.Version != "v1alpha1" {
		t.Errorf("unexpected Version: %q", gvr.Version)
	}
	if gvr.Resource != "etcdmembers" {
		t.Errorf("unexpected Resource: %q", gvr.Resource)
	}
}

func TestUpdateStatus_WithLastRestoration(t *testing.T) {
	client, rc := newMockClient()

	now := metav1.Now()
	end := metav1.Now()
	msg := "restored successfully"
	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{
		LastRestoration: &LastRestorationStatus{
			Type:      "FromSnapshot",
			Status:    "Succeeded",
			StartTime: now,
			EndTime:   &end,
			Message:   &msg,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}
	statusMap := patch["status"].(map[string]interface{})
	restoration, ok := statusMap["lastRestoration"].(map[string]interface{})
	if !ok {
		t.Fatal("expected lastRestoration in status patch")
	}
	if restoration["type"] != "FromSnapshot" {
		t.Errorf("expected type=FromSnapshot, got %v", restoration["type"])
	}
	if restoration["status"] != "Succeeded" {
		t.Errorf("expected status=Succeeded, got %v", restoration["status"])
	}
	if restoration["message"] != "restored successfully" {
		t.Errorf("expected message='restored successfully', got %v", restoration["message"])
	}
	if _, has := restoration["endTime"]; !has {
		t.Error("expected endTime in lastRestoration")
	}
}

func TestUpdateStatus_WithLastRestoration_NoEndTime(t *testing.T) {
	client, rc := newMockClient()

	now := metav1.Now()
	err := client.UpdateStatus(context.Background(), "etcd-main-0", "default", UpdateStatusOpts{
		LastRestoration: &LastRestorationStatus{
			Type:      "FromLeader",
			Status:    "InProgress",
			StartTime: now,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}
	statusMap := patch["status"].(map[string]interface{})
	restoration := statusMap["lastRestoration"].(map[string]interface{})
	if _, has := restoration["endTime"]; has {
		t.Error("endTime should be absent when nil")
	}
	if _, has := restoration["message"]; has {
		t.Error("message should be absent when nil")
	}
}

func TestSetCondition_PatchesConditionsViaStatus(t *testing.T) {
	// The mockResourceClient.Get returns an empty object (no conditions).
	// SetCondition should upsert the new condition and patch the status.
	client, rc := newMockClient()

	err := client.SetCondition(context.Background(), "etcd-main-0", "default", Condition{
		Type:    ConditionDataVolumeReadOnly,
		Status:  "True",
		Reason:  "ReadOnlyFS",
		Message: "volume is read-only",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}
	call := rc.patchCalls[0]
	if call.subresource != "status" {
		t.Errorf("expected status subresource for conditions patch, got %q", call.subresource)
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(call.data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}
	statusMap, ok := patch["status"].(map[string]interface{})
	if !ok {
		t.Fatal("expected status map in patch")
	}
	conditions, ok := statusMap["conditions"].([]interface{})
	if !ok {
		t.Fatalf("expected conditions array in status, got %T", statusMap["conditions"])
	}
	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}
	cond := conditions[0].(map[string]interface{})
	if cond["type"] != ConditionDataVolumeReadOnly {
		t.Errorf("unexpected condition type: %v", cond["type"])
	}
	if cond["status"] != "True" {
		t.Errorf("unexpected condition status: %v", cond["status"])
	}
}

// conditionGetResourceClient extends mockResourceClient to return a specific Get result.
type conditionGetResourceClient struct {
	mockResourceClient
	getResult *unstructured.Unstructured
}

func (c *conditionGetResourceClient) Namespace(_ string) dynamic.ResourceInterface {
	return c
}

func (c *conditionGetResourceClient) Get(_ context.Context, _ string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	if c.getResult != nil {
		return c.getResult, nil
	}
	obj := &unstructured.Unstructured{}
	return obj, nil
}

type conditionGetDynamicClient struct {
	resource *conditionGetResourceClient
}

func (c *conditionGetDynamicClient) Resource(_ schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return c.resource
}

func TestSetCondition_ExistingConditions_Upserted(t *testing.T) {
	// Pre-populate the object with an existing condition.
	existing := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"status": map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":               ConditionDataVolumeReadOnly,
						"status":             "False",
						"reason":             "VolumeOK",
						"message":            "all good",
						"lastTransitionTime": "2026-01-01T00:00:00Z",
					},
				},
			},
		},
	}

	rc := &conditionGetResourceClient{getResult: existing}
	c := &K8sMemberClient{client: &conditionGetDynamicClient{resource: rc}}

	err := c.SetCondition(context.Background(), "etcd-main-0", "default", Condition{
		Type:    ConditionDataVolumeReadOnly,
		Status:  "True", // changed
		Reason:  "ReadOnlyFS",
		Message: "volume is now read-only",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rc.patchCalls) != 1 {
		t.Fatalf("expected 1 patch call, got %d", len(rc.patchCalls))
	}

	var patch map[string]interface{}
	if err := json.Unmarshal(rc.patchCalls[0].data, &patch); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}
	conditions := patch["status"].(map[string]interface{})["conditions"].([]interface{})
	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition after upsert, got %d", len(conditions))
	}
	cond := conditions[0].(map[string]interface{})
	if cond["status"] != "True" {
		t.Errorf("expected updated status=True, got %v", cond["status"])
	}
	// Time must have changed since status changed.
	if cond["lastTransitionTime"] == "2026-01-01T00:00:00Z" {
		t.Error("lastTransitionTime should have been updated on status change")
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
