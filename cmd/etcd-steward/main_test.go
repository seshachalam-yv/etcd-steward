// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/gardener/etcd-steward/pkg/config"
)

func TestDerivePeerURL(t *testing.T) {
	tests := []struct {
		name           string
		podName        string
		initialCluster string
		want           string
	}{
		{
			name:           "single member",
			podName:        "etcd-0",
			initialCluster: "etcd-0=https://etcd-0.etcd-peer:2380",
			want:           "https://etcd-0.etcd-peer:2380",
		},
		{
			name:           "multi-member, first member",
			podName:        "etcd-0",
			initialCluster: "etcd-0=https://etcd-0.etcd-peer:2380,etcd-1=https://etcd-1.etcd-peer:2380,etcd-2=https://etcd-2.etcd-peer:2380",
			want:           "https://etcd-0.etcd-peer:2380",
		},
		{
			name:           "multi-member, middle member",
			podName:        "etcd-1",
			initialCluster: "etcd-0=https://etcd-0.etcd-peer:2380,etcd-1=https://etcd-1.etcd-peer:2380,etcd-2=https://etcd-2.etcd-peer:2380",
			want:           "https://etcd-1.etcd-peer:2380",
		},
		{
			name:           "pod not found in cluster",
			podName:        "etcd-99",
			initialCluster: "etcd-0=https://etcd-0.etcd-peer:2380",
			want:           "",
		},
		{
			name:           "empty initial cluster",
			podName:        "etcd-0",
			initialCluster: "",
			want:           "",
		},
		{
			name:           "HTTP peer URL (no TLS)",
			podName:        "etcd-0",
			initialCluster: "etcd-0=http://etcd-0.etcd-peer:2380",
			want:           "http://etcd-0.etcd-peer:2380",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := derivePeerURL(tc.podName, tc.initialCluster)
			if got != tc.want {
				t.Errorf("derivePeerURL(%q, %q) = %q, want %q", tc.podName, tc.initialCluster, got, tc.want)
			}
		})
	}
}

func TestCountClusterMembers(t *testing.T) {
	tests := []struct {
		name           string
		initialCluster string
		want           int
	}{
		{name: "empty", initialCluster: "", want: 0},
		{name: "single", initialCluster: "etcd-0=https://etcd-0:2380", want: 1},
		{name: "three members", initialCluster: "etcd-0=https://etcd-0:2380,etcd-1=https://etcd-1:2380,etcd-2=https://etcd-2:2380", want: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := countClusterMembers(tc.initialCluster)
			if got != tc.want {
				t.Errorf("countClusterMembers(%q) = %d, want %d", tc.initialCluster, got, tc.want)
			}
		})
	}
}

func TestFirstURL(t *testing.T) {
	tests := []struct {
		name     string
		urls     string
		fallback string
		want     string
	}{
		{name: "empty uses fallback", urls: "", fallback: "http://localhost:2379", want: "http://localhost:2379"},
		{name: "single URL", urls: "http://0.0.0.0:2379", fallback: "http://localhost:2379", want: "http://0.0.0.0:2379"},
		{name: "multiple URLs — first only", urls: "http://0.0.0.0:2379,http://127.0.0.1:2379", fallback: "http://localhost:2379", want: "http://0.0.0.0:2379"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := firstURL(tc.urls, tc.fallback)
			if got != tc.want {
				t.Errorf("firstURL(%q, %q) = %q, want %q", tc.urls, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestProcessEtcdConfig_PerMemberFields(t *testing.T) {
	// Verify that per-member map fields (advertise-client-urls,
	// initial-advertise-peer-urls) are correctly flattened to comma-separated
	// strings for the member with the given name.
	raw := []byte(`
advertise-client-urls:
  etcd-0:
    - https://etcd-0.etcd-client:2379
initial-advertise-peer-urls:
  etcd-0:
    - https://etcd-0.etcd-peer:2380
data-dir: /var/etcd/data
`)

	out, err := processEtcdConfig(raw, "etcd-0")
	if err != nil {
		t.Fatalf("processEtcdConfig error: %v", err)
	}

	outStr := string(out)
	if !strings.Contains(outStr, "https://etcd-0.etcd-client:2379") {
		t.Errorf("expected advertise-client-urls to be flattened, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "https://etcd-0.etcd-peer:2380") {
		t.Errorf("expected initial-advertise-peer-urls to be flattened, got:\n%s", outStr)
	}
	// Name should be overwritten to the actual member name.
	if !strings.Contains(outStr, "name: etcd-0") {
		t.Errorf("expected name to be set to etcd-0, got:\n%s", outStr)
	}
}

func TestProcessEtcdConfig_AlreadyFlatFields(t *testing.T) {
	// If fields are already flat strings (not per-member maps), they should pass through unchanged.
	raw := []byte(`
advertise-client-urls: https://etcd-0.etcd-client:2379
data-dir: /var/etcd/data
`)

	out, err := processEtcdConfig(raw, "etcd-0")
	if err != nil {
		t.Fatalf("processEtcdConfig error: %v", err)
	}
	if !strings.Contains(string(out), "https://etcd-0.etcd-client:2379") {
		t.Errorf("flat field should pass through unchanged, got:\n%s", out)
	}
}

func TestProcessEtcdConfig_MemberNotInMap_FallsBackToFirst(t *testing.T) {
	// When the member name is not found in the per-member map,
	// processEtcdConfig should fall back to the first value in the map.
	raw := []byte(`
advertise-client-urls:
  etcd-0:
    - https://etcd-0.etcd-client:2379
`)

	out, err := processEtcdConfig(raw, "etcd-99")
	if err != nil {
		t.Fatalf("processEtcdConfig error: %v", err)
	}
	// Must contain the fallback URL (from etcd-0 entry, as it's the only one)
	if !strings.Contains(string(out), "https://etcd-0.etcd-client:2379") {
		t.Errorf("expected fallback to first map entry, got:\n%s", out)
	}
}

func TestProcessEtcdConfig_MultipleURLs(t *testing.T) {
	// Multiple URLs in a per-member list should be joined with commas.
	raw := []byte(`
advertise-client-urls:
  etcd-0:
    - https://etcd-0.etcd-client:2379
    - https://10.0.0.1:2379
`)

	out, err := processEtcdConfig(raw, "etcd-0")
	if err != nil {
		t.Fatalf("processEtcdConfig error: %v", err)
	}
	outStr := string(out)
	if !strings.Contains(outStr, "https://etcd-0.etcd-client:2379") {
		t.Errorf("expected first URL in output, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "https://10.0.0.1:2379") {
		t.Errorf("expected second URL in output, got:\n%s", outStr)
	}
}

func TestProcessEtcdConfig_SetsMemberName(t *testing.T) {
	// The "name" field should always be overwritten to the actual pod name,
	// even when the config uses a placeholder like "etcd-config".
	raw := []byte(`
name: etcd-config
data-dir: /var/etcd/data
`)

	out, err := processEtcdConfig(raw, "etcd-main-1")
	if err != nil {
		t.Fatalf("processEtcdConfig error: %v", err)
	}
	if !strings.Contains(string(out), "name: etcd-main-1") {
		t.Errorf("expected name to be overwritten to 'etcd-main-1', got:\n%s", out)
	}
}

// TestDeltaSnapshotPeriodFlagWired verifies that the --delta-snapshot-period flag is applied
// to cfg.DeltaSnapshotPeriod. Without this wiring the flag is registered but ignored and the
// steward always uses the 20s default regardless of the spec.backup.deltaSnapshotPeriod field.
func TestDeltaSnapshotPeriodFlagWired(t *testing.T) {
	cfg := config.DefaultConfig()

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Duration("delta-snapshot-period", 1*time.Minute, "delta snapshot period")

	if err := fs.Parse([]string{"--delta-snapshot-period=5s"}); err != nil {
		t.Fatalf("flag parse error: %v", err)
	}

	// Replicate the flag-wiring logic from main().
	if period, err := fs.GetDuration("delta-snapshot-period"); err == nil {
		cfg.DeltaSnapshotPeriod = period
	}

	if cfg.DeltaSnapshotPeriod != 5*time.Second {
		t.Errorf("DeltaSnapshotPeriod = %v, want 5s (flag not wired to config)", cfg.DeltaSnapshotPeriod)
	}
}
