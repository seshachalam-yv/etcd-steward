// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package config provides configuration loading and validation for etcd-steward.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gardener/etcd-steward/pkg/snapstore"
	"sigs.k8s.io/yaml"
)

// Config holds all configuration for etcd-steward.
type Config struct {
	// Identity
	PodName      string `json:"podName"`
	PodNamespace string `json:"podNamespace"`
	EtcdName     string `json:"etcdName"` // Derived from PodName: strip "-<ordinal>" suffix.

	// Etcd connection
	EtcdEndpoint       string `json:"etcdEndpoint"`
	EtcdPeerURL        string `json:"etcdPeerURL"`
	InitialCluster     string `json:"initialCluster"`
	IsSingleNode       bool   `json:"isSingleNode"`
	DataDir            string `json:"dataDir"`
	RestorationTempDir string `json:"restorationTempDir"`

	// HTTP server
	ServerPort int `json:"serverPort"`

	// Member lease
	EnableMemberLeaseRenewal   bool          `json:"enableMemberLeaseRenewal"`
	MemberLeaseRenewalInterval time.Duration `json:"memberLeaseRenewalInterval"`

	// Snapshots
	EnableSnapshots      bool          `json:"enableSnapshots"`
	FullSnapshotSchedule string        `json:"fullSnapshotSchedule"`
	DeltaSnapshotPeriod  time.Duration `json:"deltaSnapshotPeriod"`
	MaxDeltaSnapshotSize int64         `json:"maxDeltaSnapshotSize"` // bytes
	MaxDeltaEvents       int64         `json:"maxDeltaEvents"`

	// Snapstore
	Snapstore snapstore.SnapstoreConfig `json:"snapstore"`

	// Compression
	CompressionPolicy string `json:"compressionPolicy"` // none, gzip, zstd

	// GC
	EnableGC                bool          `json:"enableGC"`
	GarbageCollectionPeriod time.Duration `json:"garbageCollectionPeriod"`
	MaxFullSnapshots        int           `json:"maxFullSnapshots"`
	GCPolicy                string        `json:"gcPolicy"`

	// Restoration
	EnableRestoration bool `json:"enableRestoration"`

	// Defrag
	EnableDefrag          bool   `json:"enableDefrag"`
	DefragSchedule        string `json:"defragSchedule"`
	EnableDistributedLock bool   `json:"enableDistributedLock"`

	// Alarm
	EnableAlarmHandler bool          `json:"enableAlarmHandler"`
	AlarmCheckInterval time.Duration `json:"alarmCheckInterval"`
	CompactRevisionLag int64         `json:"compactRevisionLag"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		DataDir:                    "/var/etcd/data",
		ServerPort:                 8080,
		EnableMemberLeaseRenewal:   true,
		MemberLeaseRenewalInterval: 10 * time.Second,
		CompressionPolicy:          "zstd",
		GarbageCollectionPeriod:    12 * time.Hour,
		MaxFullSnapshots:           7,
		GCPolicy:                   "LimitBased",
		EnableRestoration:          true,
		DeltaSnapshotPeriod:        20 * time.Second,
		MaxDeltaSnapshotSize:       100 * 1024 * 1024, // 100 MiB
		MaxDeltaEvents:             1_000_000,
		AlarmCheckInterval:         30 * time.Second,
		CompactRevisionLag:         1000,
		FullSnapshotSchedule:       "0 */24 * * *",
	}
}

// Validate returns a slice of validation errors for required fields.
func (c *Config) Validate() []error {
	var errs []error

	if c.PodName == "" {
		errs = append(errs, fmt.Errorf("PodName is required"))
	}
	if c.PodNamespace == "" {
		errs = append(errs, fmt.Errorf("PodNamespace is required"))
	}
	if c.EtcdEndpoint == "" {
		errs = append(errs, fmt.Errorf("EtcdEndpoint is required"))
	}
	if c.EtcdPeerURL == "" {
		errs = append(errs, fmt.Errorf("EtcdPeerURL is required"))
	}
	if c.InitialCluster == "" {
		errs = append(errs, fmt.Errorf("InitialCluster is required"))
	}

	return errs
}

// DeriveEtcdName derives the EtcdName from PodName by stripping the trailing "-<ordinal>" suffix.
func (c *Config) DeriveEtcdName() {
	if c.EtcdName != "" {
		return
	}
	name := c.PodName
	if idx := strings.LastIndex(name, "-"); idx > 0 {
		c.EtcdName = name[:idx]
	} else {
		c.EtcdName = name
	}
}

// LoadFromFile reads a YAML configuration file and unmarshals it into Config.
func LoadFromFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	cfg.DeriveEtcdName()
	return cfg, nil
}
