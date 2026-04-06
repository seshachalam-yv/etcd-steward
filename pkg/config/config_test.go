// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"ServerPort", cfg.ServerPort, 8080},
		{"DataDir", cfg.DataDir, "/var/etcd/data"},
		{"EnableMemberLeaseRenewal", cfg.EnableMemberLeaseRenewal, true},
		{"MemberLeaseRenewalInterval", cfg.MemberLeaseRenewalInterval, 10 * time.Second},
		{"CompressionPolicy", cfg.CompressionPolicy, "zstd"},
		{"GarbageCollectionPeriod", cfg.GarbageCollectionPeriod, 12 * time.Hour},
		{"MaxFullSnapshots", cfg.MaxFullSnapshots, 7},
		{"GCPolicy", cfg.GCPolicy, "LimitBased"},
		{"EnableRestoration", cfg.EnableRestoration, true},
		{"DeltaSnapshotPeriod", cfg.DeltaSnapshotPeriod, 20 * time.Second},
		{"MaxDeltaSnapshotSize", cfg.MaxDeltaSnapshotSize, int64(100 * 1024 * 1024)},
		{"MaxDeltaEvents", cfg.MaxDeltaEvents, int64(1_000_000)},
		{"AlarmCheckInterval", cfg.AlarmCheckInterval, 30 * time.Second},
		{"CompactRevisionLag", cfg.CompactRevisionLag, int64(1000)},
		{"FullSnapshotSchedule", cfg.FullSnapshotSchedule, "0 */24 * * *"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("DefaultConfig().%s = %v, want %v", tc.name, tc.got, tc.want)
			}
		})
	}
}

func TestValidate_MissingRequiredFields(t *testing.T) {
	cfg := DefaultConfig()
	// All required fields are empty by default.

	errs := cfg.Validate()
	if len(errs) == 0 {
		t.Fatal("expected validation errors for empty config, got none")
	}

	// Should have errors for: PodName, PodNamespace, EtcdEndpoint, EtcdPeerURL, InitialCluster.
	expectedCount := 5
	if len(errs) != expectedCount {
		t.Errorf("expected %d validation errors, got %d: %v", expectedCount, len(errs), errs)
	}
}

func TestValidate_AllFieldsSet(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PodName = "etcd-main-0"
	cfg.PodNamespace = "default"
	cfg.EtcdEndpoint = "http://localhost:2379"
	cfg.EtcdPeerURL = "https://etcd-main-0:2380"
	cfg.InitialCluster = "etcd-main-0=https://etcd-main-0:2380"

	errs := cfg.Validate()
	if len(errs) != 0 {
		t.Errorf("expected 0 validation errors, got %d: %v", len(errs), errs)
	}
}

func TestDeriveEtcdName(t *testing.T) {
	tests := []struct {
		podName  string
		wantName string
	}{
		{"etcd-main-0", "etcd-main"},
		{"etcd-events-2", "etcd-events"},
		{"single", "single"},
		{"a-b-c-3", "a-b-c"},
	}

	for _, tc := range tests {
		t.Run(tc.podName, func(t *testing.T) {
			cfg := Config{PodName: tc.podName}
			cfg.DeriveEtcdName()
			if cfg.EtcdName != tc.wantName {
				t.Errorf("DeriveEtcdName() = %q, want %q", cfg.EtcdName, tc.wantName)
			}
		})
	}
}

func TestDeriveEtcdName_PresetNotOverwritten(t *testing.T) {
	cfg := Config{PodName: "etcd-main-0", EtcdName: "custom-name"}
	cfg.DeriveEtcdName()
	if cfg.EtcdName != "custom-name" {
		t.Errorf("DeriveEtcdName() overwrote preset EtcdName: got %q, want %q", cfg.EtcdName, "custom-name")
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `podName: etcd-main-0
podNamespace: garden
etcdEndpoint: http://localhost:2379
etcdPeerURL: https://etcd-main-0:2380
initialCluster: etcd-main-0=https://etcd-main-0:2380
serverPort: 9090
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile error: %v", err)
	}

	if cfg.PodName != "etcd-main-0" {
		t.Errorf("PodName = %q, want %q", cfg.PodName, "etcd-main-0")
	}
	if cfg.PodNamespace != "garden" {
		t.Errorf("PodNamespace = %q, want %q", cfg.PodNamespace, "garden")
	}
	if cfg.ServerPort != 9090 {
		t.Errorf("ServerPort = %d, want %d", cfg.ServerPort, 9090)
	}
	// Check that defaults are preserved for unset fields.
	if cfg.CompressionPolicy != "zstd" {
		t.Errorf("CompressionPolicy = %q, want %q (default)", cfg.CompressionPolicy, "zstd")
	}
	// EtcdName should be derived.
	if cfg.EtcdName != "etcd-main" {
		t.Errorf("EtcdName = %q, want %q", cfg.EtcdName, "etcd-main")
	}
}

func TestLoadFromFile_NotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
}

func TestLoadFromFile_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")

	if err := os.WriteFile(path, []byte("{{invalid yaml"), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadFromFile(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}
