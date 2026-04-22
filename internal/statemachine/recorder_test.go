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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestK8sRecorder_Record(t *testing.T) {
	scheme := runtime.NewScheme()
	gvr := schema.GroupVersionResource{
		Group:    "druid.gardener.cloud",
		Version:  "v1alpha1",
		Resource: "etcdmembers",
	}

	// Pre-create the EtcdMember object so the patch has a target.
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvr.Group,
		Version: gvr.Version,
		Kind:    "EtcdMember",
	})
	existing.SetName("etcd-main-0")
	existing.SetNamespace("shoot--project--name")

	fakeClient := dynamicfake.NewSimpleDynamicClient(scheme, existing)
	recorder := NewK8sRecorder(fakeClient)

	tr := Transition{
		From:   StateUnknown,
		To:     StateNew,
		Action: ActionStartAsNew,
		Time:   time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
	}

	err := recorder.Record(context.Background(), "etcd-main-0", "shoot--project--name", tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the patch was issued.
	actions := fakeClient.Actions()
	found := false
	for _, a := range actions {
		if a.GetVerb() == "patch" && a.GetResource() == gvr {
			patchAction, ok := a.(patchAction)
			if !ok {
				continue
			}
			var patchBody map[string]interface{}
			if err := json.Unmarshal(patchAction.GetPatch(), &patchBody); err != nil {
				t.Fatalf("failed to unmarshal patch: %v", err)
			}
			status, ok := patchBody["status"].(map[string]interface{})
			if !ok {
				t.Fatal("expected status field in patch")
			}
			if status["state"] != "New" {
				t.Fatalf("expected state 'New', got %v", status["state"])
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected a patch action on etcdmembers resource")
	}
}

// patchAction is satisfied by k8s.io/client-go/testing.PatchActionImpl.
type patchAction interface {
	GetPatch() []byte
}

func TestNewK8sRecorder(t *testing.T) {
	scheme := runtime.NewScheme()
	fakeClient := dynamicfake.NewSimpleDynamicClient(scheme)
	recorder := NewK8sRecorder(fakeClient)

	if recorder == nil {
		t.Fatal("expected non-nil recorder")
	}
	if recorder.client == nil {
		t.Fatal("expected non-nil client in recorder")
	}
}

// Verify K8sRecorder implements Recorder at compile time.
var _ Recorder = (*K8sRecorder)(nil)

// Verify the module-level GVR matches expectations.
func TestEtcdMemberGVR(t *testing.T) {
	if etcdMemberGVR.Group != "druid.gardener.cloud" {
		t.Fatalf("expected group 'druid.gardener.cloud', got %q", etcdMemberGVR.Group)
	}
	if etcdMemberGVR.Version != "v1alpha1" {
		t.Fatalf("expected version 'v1alpha1', got %q", etcdMemberGVR.Version)
	}
	if etcdMemberGVR.Resource != "etcdmembers" {
		t.Fatalf("expected resource 'etcdmembers', got %q", etcdMemberGVR.Resource)
	}
}

// Suppress unused import warnings for metav1 — it is used by the test setup.
var _ = metav1.Now
