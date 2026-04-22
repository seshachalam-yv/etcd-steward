// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
	"github.com/spf13/pflag"
)

func TestDefaultConfig_HasSensibleDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ServerPort != 8080 {
		t.Fatalf("expected ServerPort 8080, got %d", cfg.ServerPort)
	}
	if cfg.DataDir != "/var/etcd/data/new.etcd" {
		t.Fatalf("expected DataDir '/var/etcd/data/new.etcd', got %q", cfg.DataDir)
	}
	if cfg.FullSnapshotInterval != 24*time.Hour {
		t.Fatalf("expected FullSnapshotInterval 24h, got %s", cfg.FullSnapshotInterval)
	}
	if cfg.DeltaSnapshotInterval != 5*time.Minute {
		t.Fatalf("expected DeltaSnapshotInterval 5m, got %s", cfg.DeltaSnapshotInterval)
	}
	if cfg.MaxFullSnapshots != 7 {
		t.Fatalf("expected MaxFullSnapshots 7, got %d", cfg.MaxFullSnapshots)
	}
	if !cfg.EnableSnapshotter {
		t.Fatal("expected EnableSnapshotter true")
	}
	if !cfg.EnableGC {
		t.Fatal("expected EnableGC true")
	}
	if cfg.EmbeddedEtcdQuotaBytes != 8*1024*1024*1024 {
		t.Fatalf("expected EmbeddedEtcdQuotaBytes 8GiB, got %d", cfg.EmbeddedEtcdQuotaBytes)
	}
	if cfg.RestorationTempDir != "/var/etcd/data/restoration.tmp" {
		t.Fatalf("expected RestorationTempDir '/var/etcd/data/restoration.tmp', got %q", cfg.RestorationTempDir)
	}
}

func TestConfig_FlagOverridesFile(t *testing.T) {
	// Write a YAML config file with pod-name set.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := []byte("pod-name: from-file\nserver-port: 9090\n")
	if err := os.WriteFile(cfgPath, content, 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg := DefaultConfig()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(cfg, fs)

	// Simulate the user explicitly passing --pod-name on the command line.
	if err := fs.Parse([]string{"--pod-name=from-flag"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := LoadFromFile(cfg, cfgPath, fs); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	// Flag value must win.
	if cfg.PodName != "from-flag" {
		t.Fatalf("expected PodName 'from-flag', got %q", cfg.PodName)
	}
	// File value should apply for server-port since it was not set via flag.
	if cfg.ServerPort != 9090 {
		t.Fatalf("expected ServerPort 9090 from file, got %d", cfg.ServerPort)
	}
}

func TestConfig_ValidateMissingPodName(t *testing.T) {
	cfg := validConfig()
	cfg.PodName = ""

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected validation error for missing PodName")
	}
	if err.Code() != errors.ErrCodeMissingRequired {
		t.Fatalf("expected ErrCodeMissingRequired, got %d", err.Code())
	}
}

func TestConfig_ValidateMissingPodNamespace(t *testing.T) {
	cfg := validConfig()
	cfg.PodNamespace = ""

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected validation error for missing PodNamespace")
	}
	if err.Code() != errors.ErrCodeMissingRequired {
		t.Fatalf("expected ErrCodeMissingRequired, got %d", err.Code())
	}
}

func TestConfig_ValidateEmptyEndpoints(t *testing.T) {
	cfg := validConfig()
	cfg.EtcdEndpoints = nil

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected validation error for empty EtcdEndpoints")
	}
	if err.Code() != errors.ErrCodeMissingRequired {
		t.Fatalf("expected ErrCodeMissingRequired, got %d", err.Code())
	}
}

func TestConfig_ValidateInvalidPort(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{name: "zero", port: 0},
		{name: "negative", port: -1},
		{name: "too large", port: 70000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.ServerPort = tc.port

			err := Validate(cfg)
			if err == nil {
				t.Fatalf("expected validation error for port %d", tc.port)
			}
			if err.Code() != errors.ErrCodeInvalidConfig {
				t.Fatalf("expected ErrCodeInvalidConfig, got %d", err.Code())
			}
		})
	}
}

func TestConfig_ValidateSnapshotIntervalConflict(t *testing.T) {
	cfg := validConfig()
	cfg.FullSnapshotInterval = 1 * time.Minute
	cfg.DeltaSnapshotInterval = 5 * time.Minute

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected validation error when FullSnapshotInterval < DeltaSnapshotInterval")
	}
	if err.Code() != errors.ErrCodeInvalidConfig {
		t.Fatalf("expected ErrCodeInvalidConfig, got %d", err.Code())
	}
}

// validConfig returns a Config that passes all validation checks.
func validConfig() *Config {
	cfg := DefaultConfig()
	cfg.PodName = "etcd-main-0"
	cfg.PodNamespace = "shoot--project--name"
	cfg.EtcdEndpoints = []string{"https://etcd-main-client:2379"}
	return cfg
}
