// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package member provides a client for managing EtcdMember Kubernetes resources.
package member

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// EtcdMemberGVR returns the GroupVersionResource for EtcdMember objects.
// Exported so test packages can create fake dynamic clients with the correct GVR.
func EtcdMemberGVR() schema.GroupVersionResource {
	return etcdMemberGVR
}

// UpdateStatusOpts holds the fields to update in EtcdMember.status.
type UpdateStatusOpts struct {
	// MemberID is the etcd member ID (optional).
	MemberID *string
	// ClusterID is the etcd cluster ID (optional).
	ClusterID *string
	// LastTransition is the last state transition to record (optional).
	LastTransition *statemachine.Transition
	// LastRestoration carries the outcome of the most recent restoration operation (optional).
	// When set, the EtcdMember.status.lastRestoration field is updated.
	LastRestoration *LastRestorationStatus
	// PeerTLSEnabled indicates whether peer TLS is enabled for this member (optional).
	// Set once at startup based on the etcd configuration.
	PeerTLSEnabled *bool
}

// LastRestorationStatus is the status of a completed restoration operation written to EtcdMember.status.
type LastRestorationStatus struct {
	// Type is "FromSnapshot" or "FromLeader".
	Type string
	// Status is "Succeeded", "Failed", or "InProgress".
	Status string
	// StartTime is when the restoration began.
	StartTime metav1.Time
	// EndTime is when the restoration completed (nil if still in progress).
	EndTime *metav1.Time
	// Message is an optional human-readable result message.
	Message *string
}

// Client provides operations on EtcdMember resources.
type Client interface {
	// UpdateStatus patches the status fields of an EtcdMember.
	UpdateStatus(ctx context.Context, memberName, namespace string, opts UpdateStatusOpts) error
	// RemoveCreateAsLearnerAnnotation removes the create-as-learner annotation from an EtcdMember.
	RemoveCreateAsLearnerAnnotation(ctx context.Context, memberName, namespace string) error
	// PatchStatus applies a raw JSON patch to the EtcdMember status subresource.
	// The patchType must be types.MergePatchType or types.StrategicMergePatchType.
	// Used by StatusReconciler to batch-patch multiple status fields.
	PatchStatus(ctx context.Context, memberName, namespace string, patchType types.PatchType, data []byte) error
	// SetCondition creates or updates a condition in EtcdMember.status.conditions.
	// If the condition type already exists with the same status, lastTransitionTime is preserved.
	SetCondition(ctx context.Context, memberName, namespace string, condition Condition) error
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
	if opts.LastRestoration != nil {
		r := map[string]interface{}{
			"type":      opts.LastRestoration.Type,
			"status":    opts.LastRestoration.Status,
			"startTime": opts.LastRestoration.StartTime,
		}
		if opts.LastRestoration.EndTime != nil {
			r["endTime"] = opts.LastRestoration.EndTime
		}
		if opts.LastRestoration.Message != nil {
			r["message"] = *opts.LastRestoration.Message
		}
		statusFields["lastRestoration"] = r
	}
	if opts.PeerTLSEnabled != nil {
		statusFields["peerTLSEnabled"] = *opts.PeerTLSEnabled
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
		// EtcdMember may not exist yet during early startup while etcd-druid creates it.
		// Treat not-found as a transient condition — the StatusReconciler will retry.
		if apierrors.IsNotFound(err) {
			return nil
		}
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

// PatchStatus applies a raw JSON patch directly to the EtcdMember status subresource.
func (c *K8sMemberClient) PatchStatus(ctx context.Context, memberName, namespace string, patchType types.PatchType, data []byte) error {
	_, err := c.client.Resource(etcdMemberGVR).Namespace(namespace).Patch(
		ctx,
		memberName,
		patchType,
		data,
		metav1.PatchOptions{},
		"status",
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to patch EtcdMember %s/%s status: %w", namespace, memberName, err)
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

// PatchStatus does nothing.
func (n *NoopClient) PatchStatus(_ context.Context, _, _ string, _ types.PatchType, _ []byte) error {
	return nil
}
