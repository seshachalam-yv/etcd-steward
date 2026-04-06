// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"encoding/json"
	"testing"

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
