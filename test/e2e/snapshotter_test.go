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

func TestProducesFullSnapshots(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	// Enable snapshotter with a short interval for testing.
	for i, arg := range args {
		if strings.HasPrefix(arg, "--enable-snapshotter=") {
			args[i] = "--enable-snapshotter=true"
		}
	}
	args = append(args, "--full-snapshot-interval=30s")

	createEtcdStewardPodWithVolumes(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	// Write some data so there is content to snapshot.
	for i := 0; i < 10; i++ {
		execInPod(t, ns, podName, "etcd", []string{
			"etcdctl", "put", fmt.Sprintf("snap-key-%d", i), fmt.Sprintf("snap-val-%d", i),
		})
	}

	// Wait for the steward to log a full snapshot completion.
	waitForLogEntry(t, ns, podName, "steward", "full snapshot completed", 3*time.Minute)
}

func TestDeltaSnapshotsAccumulate(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	for i, arg := range args {
		if strings.HasPrefix(arg, "--enable-snapshotter=") {
			args[i] = "--enable-snapshotter=true"
		}
	}
	args = append(args, "--full-snapshot-interval=5m")
	args = append(args, "--delta-snapshot-interval=10s")

	createEtcdStewardPodWithVolumes(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	// Write some data to trigger delta snapshots.
	for i := 0; i < 20; i++ {
		execInPod(t, ns, podName, "etcd", []string{
			"etcdctl", "put", fmt.Sprintf("delta-key-%d", i), fmt.Sprintf("delta-val-%d", i),
		})
	}

	// Wait for the steward to log a delta snapshot completion.
	waitForLogEntry(t, ns, podName, "steward", "delta snapshot completed", 3*time.Minute)
}

func TestEtcdDataPersistence(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	createEtcdStewardPodWithVolumes(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	// Write 50 keys.
	for i := 0; i < 50; i++ {
		execInPod(t, ns, podName, "etcd", []string{
			"etcdctl", "put", fmt.Sprintf("persist-key-%03d", i), fmt.Sprintf("persist-val-%03d", i),
		})
	}

	// Check endpoint status for DBSize.
	stdout, _ := execInPod(t, ns, podName, "etcd", []string{
		"etcdctl", "endpoint", "status", "--write-out=table",
	})

	if !strings.Contains(stdout, "DB SIZE") && !strings.Contains(strings.ToLower(stdout), "db size") {
		t.Logf("endpoint status output: %s", stdout)
	}

	// Verify data still readable.
	stdout, _ = execInPod(t, ns, podName, "etcd", []string{
		"etcdctl", "get", "persist-key-000", "--print-value-only",
	})

	if !strings.Contains(stdout, "persist-val-000") {
		t.Fatalf("expected persist-val-000, got: %s", stdout)
	}
}

func TestStewardBootstrapLogs(t *testing.T) {
	ns := setupNamespace(t)
	podName := "etcd-steward-0"

	args := defaultStewardArgs(podName, ns)
	createEtcdStewardPod(t, ns, podName, args)
	waitForPodReady(t, ns, podName, 2*time.Minute)
	waitForStewardHealthy(t, ns, podName, 30*time.Second)

	// Verify the steward logs contain expected startup messages.
	logs := getPodLogs(t, ns, podName, "steward")

	expectedSequence := []string{
		"starting HTTP server",
	}

	for _, expected := range expectedSequence {
		if !strings.Contains(logs, expected) {
			t.Fatalf("expected steward logs to contain %q, logs:\n%s", expected, logs)
		}
	}
}
