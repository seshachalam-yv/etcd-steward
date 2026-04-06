// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package member provides a client for managing EtcdMember Kubernetes resources.
package member

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/gardener/etcd-steward/pkg/statemachine"
)

const (
	// OwnedByLabelKey is the label key for the owning Etcd resource.
	OwnedByLabelKey = "druid.gardener.cloud/owned-by"
	// CreateAsLearnerAnnotation is set by etcd-druid on EtcdMember during scale-up.
	CreateAsLearnerAnnotation = "druid.gardener.cloud/create-as-learner"
)

// etcdMemberGVR is the GroupVersionResource for EtcdMember CRD objects.
var etcdMemberGVR = schema.GroupVersionResource{
	Group:    "druid.gardener.cloud",
	Version:  "v1alpha1",
	Resource: "etcdmembers",
}

// UpdateStatusOpts holds the fields to update in EtcdMember.status.
type UpdateStatusOpts struct {
	// MemberID is the etcd member ID (optional).
	MemberID *string
	// ClusterID is the etcd cluster ID (optional).
	ClusterID *string
	// LastTransition is the last state transition to record (optional).
	LastTransition *statemachine.Transition
}

// Client provides operations on EtcdMember resources.
type Client interface {
	// UpdateStatus patches the status fields of an EtcdMember.
	UpdateStatus(ctx context.Context, memberName, namespace string, opts UpdateStatusOpts) error
	// RemoveCreateAsLearnerAnnotation removes the create-as-learner annotation from an EtcdMember.
	RemoveCreateAsLearnerAnnotation(ctx context.Context, memberName, namespace string) error
}

// K8sMemberClient implements Client using the Kubernetes dynamic client.
type K8sMemberClient struct {
	client dynamic.Interface
}

// NewK8sClient creates a Client that operates on EtcdMember resources
// using the provided dynamic client.
func NewK8sClient(dynamicClient dynamic.Interface) Client {
	return &K8sMemberClient{client: dynamicClient}
}

// UpdateStatus patches the status fields of an EtcdMember.
// Only non-nil fields in opts are included in the patch.
func (c *K8sMemberClient) UpdateStatus(ctx context.Context, memberName, namespace string, opts UpdateStatusOpts) error {
	statusFields := make(map[string]interface{})
	if opts.MemberID != nil {
		statusFields["id"] = *opts.MemberID
	}
	if opts.ClusterID != nil {
		statusFields["clusterID"] = *opts.ClusterID
	}
	if opts.LastTransition != nil {
		statusFields["lastTransition"] = opts.LastTransition
	}

	payload := map[string]interface{}{
		"status": statusFields,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal status patch for EtcdMember %s/%s: %w", namespace, memberName, err)
	}

	_, err = c.client.Resource(etcdMemberGVR).Namespace(namespace).Patch(
		ctx,
		memberName,
		types.MergePatchType,
		data,
		metav1.PatchOptions{},
		"status",
	)
	if err != nil {
		return fmt.Errorf("failed to patch EtcdMember %s/%s status: %w", namespace, memberName, err)
	}
	return nil
}

// RemoveCreateAsLearnerAnnotation removes the create-as-learner annotation from an EtcdMember
// by patching the metadata with a null annotation value (JSON merge patch removal semantics).
func (c *K8sMemberClient) RemoveCreateAsLearnerAnnotation(ctx context.Context, memberName, namespace string) error {
	payload := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				CreateAsLearnerAnnotation: nil,
			},
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal annotation patch for EtcdMember %s/%s: %w", namespace, memberName, err)
	}

	_, err = c.client.Resource(etcdMemberGVR).Namespace(namespace).Patch(
		ctx,
		memberName,
		types.MergePatchType,
		data,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to patch EtcdMember %s/%s annotations: %w", namespace, memberName, err)
	}
	return nil
}

// NoopClient is a no-op implementation of Client for testing.
type NoopClient struct{}

// UpdateStatus does nothing.
func (n *NoopClient) UpdateStatus(_ context.Context, _, _ string, _ UpdateStatusOpts) error {
	return nil
}

// RemoveCreateAsLearnerAnnotation does nothing.
func (n *NoopClient) RemoveCreateAsLearnerAnnotation(_ context.Context, _, _ string) error {
	return nil
}
