// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestE2E_Steward_AcceptsTLSFlags verifies that the steward binary accepts
// TLS-related flags (--ca-cert, --cert, --key) without crashing. Because no
// real certificates are provided the steward is expected to start, log a TLS
// error, and keep running rather than panic.
func TestE2E_Steward_AcceptsTLSFlags(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-tls-flags"

	args := defaultStewardArgs(podName, ns)
	args = append(args,
		"--ca-cert=/nonexistent/ca.crt",
		"--cert=/nonexistent/tls.crt",
		"--key=/nonexistent/tls.key",
	)

	createEtcdStewardPod(t, ns, podName, args)

	// The steward should reach Running phase even though the TLS cert
	// paths do not exist. It must not crash-loop on invalid TLS flags.
	waitForPodRunning(t, ns, podName, 2*time.Minute)

	// Give the steward a moment to produce logs.
	time.Sleep(5 * time.Second)

	logs := getPodLogs(t, ns, podName, "steward")

	// Verify there is no Go panic in the logs. The steward may log a TLS
	// or file-not-found error, but it should not crash.
	if strings.Contains(logs, "panic:") {
		t.Fatalf("steward panicked when given nonexistent TLS cert paths.\nLogs:\n%s", logs)
	}

	t.Logf("steward accepted TLS flags without crashing. Log excerpt:\n%.500s", logs)
}

// TestE2E_Steward_CustomServerPort verifies that the steward HTTP server
// listens on a non-default port when --server-port is set.
func TestE2E_Steward_CustomServerPort(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-custom-port"

	args := []string{
		"--pod-name=" + podName,
		"--pod-namespace=" + ns,
		"--server-port=9090",
		"--etcd-endpoints=http://127.0.0.1:2379",
		"--data-dir=/var/etcd/data/new.etcd",
		"--enable-member-lease-renewal=false",
		"--enable-snapshot-lease-renewal=false",
		"--enable-snapshotter=false",
		"--enable-gc=false",
		"--enable-defrag=false",
		"--enable-alarm-handler=false",
	}

	createEtcdStewardPodWithVolumes(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	// Port-forward to the custom port 9090 and check /healthz.
	body := portForwardPod(t, ns, podName, 9090, "/healthz")
	if !strings.Contains(body, "ok") {
		t.Fatalf("expected /healthz on custom port 9090 to return 'ok', got: %s", body)
	}
}

// waitForPodRunning waits until the pod reaches corev1.PodRunning phase.
// Unlike waitForPodReady it does NOT require all containers to be Ready --
// this is useful for tests where the steward may never become healthy (e.g.
// when TLS certs are intentionally missing).
func waitForPodRunning(t *testing.T, namespace, podName string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Fetch the last known pod state for a useful error message.
			pod, err := kubeClient.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("timed out waiting for pod %s/%s to reach Running phase (could not fetch pod: %v)", namespace, podName, err)
			}
			t.Fatalf("timed out waiting for pod %s/%s to reach Running phase; current phase: %s, conditions: %v",
				namespace, podName, pod.Status.Phase, pod.Status.Conditions)
		case <-ticker.C:
			pod, err := kubeClient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				continue
			}
			if pod.Status.Phase == corev1.PodRunning {
				return
			}
		}
	}
}
