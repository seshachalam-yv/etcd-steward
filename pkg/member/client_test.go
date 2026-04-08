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
