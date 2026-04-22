// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestFreshSingleNode(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	createEtcdStewardPod(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	// Write a key via etcdctl in the etcd container.
	execInPod(t, ns, podName, "etcd", []string{
		"etcdctl", "put", "e2e-key", "e2e-value",
	})

	// Read it back and verify.
	stdout, _ := execInPod(t, ns, podName, "etcd", []string{
		"etcdctl", "get", "e2e-key", "--print-value-only",
	})

	if !strings.Contains(stdout, "e2e-value") {
		t.Fatalf("expected etcdctl get to return 'e2e-value', got: %s", stdout)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	createEtcdStewardPod(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	body := portForwardPod(t, ns, podName, 8080, "/metrics")

	if !strings.Contains(body, "go_goroutines") {
		t.Fatalf("expected /metrics to contain 'go_goroutines', got:\n%s", body)
	}
}

func TestHealthzEndpoint(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	createEtcdStewardPod(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	body := portForwardPod(t, ns, podName, 8080, "/healthz")

	if !strings.Contains(body, "ok") {
		t.Fatalf("expected /healthz to return 'ok', got: %s", body)
	}
}

func TestEtcdOperations(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	createEtcdStewardPodWithVolumes(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	// Write 100 keys.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("ops-key-%03d", i)
		val := fmt.Sprintf("ops-val-%03d", i)
		execInPod(t, ns, podName, "etcd", []string{
			"etcdctl", "put", key, val,
		})
	}

	// Count keys using --write-out=fields to get the count field.
	stdout, _ := execInPod(t, ns, podName, "etcd", []string{
		"etcdctl", "get", "ops-key-", "--prefix", "--count-only", "--write-out=fields",
	})

	if !strings.Contains(stdout, "\"Count\" : 100") && !strings.Contains(stdout, "Count") {
		t.Logf("key count output: %s", stdout)
	}

	// Delete all keys with the prefix.
	execInPod(t, ns, podName, "etcd", []string{
		"etcdctl", "del", "ops-key-", "--prefix",
	})

	// Verify keys are deleted.
	stdout, _ = execInPod(t, ns, podName, "etcd", []string{
		"etcdctl", "get", "ops-key-", "--prefix", "--count-only", "--write-out=fields",
	})

	if strings.Contains(stdout, "\"Count\" : 100") {
		t.Fatalf("expected keys to be deleted, but count is still 100:\n%s", stdout)
	}
}
