// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gardener/etcd-steward/internal/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// etcdMemberGVR is the GroupVersionResource for the EtcdMember custom resource.
var etcdMemberGVR = schema.GroupVersionResource{
	Group:    "druid.gardener.cloud",
	Version:  "v1alpha1",
	Resource: "etcdmembers",
}

// Recorder records state transitions to an external system.
type Recorder interface {
	// Record persists a transition for the given member.
	Record(ctx context.Context, memberName, namespace string, t Transition) error
}

// K8sRecorder records state transitions by patching the EtcdMember status
// via the Kubernetes dynamic client.
type K8sRecorder struct {
	client dynamic.Interface
}

// NewK8sRecorder creates a Recorder backed by the given dynamic client.
func NewK8sRecorder(client dynamic.Interface) *K8sRecorder {
	return &K8sRecorder{client: client}
}

// Record patches the EtcdMember status sub-resource with the transition details.
func (r *K8sRecorder) Record(ctx context.Context, memberName, namespace string, t Transition) error {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"state": string(t.To),
			"lastTransition": map[string]interface{}{
				"from":   string(t.From),
				"to":     string(t.To),
				"action": string(t.Action),
				"time":   t.Time.UTC().Format("2006-01-02T15:04:05Z"),
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return errors.Wrap(errors.ErrCodeInternal, "failed to marshal status patch", err)
	}

	_, err = r.client.Resource(etcdMemberGVR).Namespace(namespace).Patch(
		ctx,
		memberName,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"status",
	)
	if err != nil {
		return errors.Wrap(
			errors.ErrCodeNetwork,
			fmt.Sprintf("failed to patch EtcdMember %s/%s status", namespace, memberName),
			err,
		)
	}
	return nil
}
