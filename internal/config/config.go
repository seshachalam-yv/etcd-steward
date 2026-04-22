// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package config

import "time"

// Config holds all configuration for the etcd-steward process.
type Config struct {
	// ServerPort is the HTTP server listen port.
	ServerPort int
	// PodName is the name of the pod running etcd-steward.
	PodName string
	// PodNamespace is the Kubernetes namespace of the pod.
	PodNamespace string

	// EtcdEndpoints is the list of etcd client endpoints.
	EtcdEndpoints []string
	// EtcdConnectionTimeout is the timeout for connecting to etcd.
	EtcdConnectionTimeout time.Duration

	// CACert is the path to the CA certificate for TLS.
	CACert string
	// Cert is the path to the client certificate for TLS.
	Cert string
	// Key is the path to the client private key for TLS.
	Key string

	// DataDir is the etcd data directory.
	DataDir string
	// InitialCluster is the initial etcd cluster configuration.
	InitialCluster string
	// InitialClusterToken is the initial etcd cluster token.
	InitialClusterToken string
	// InitialClusterState is the initial etcd cluster state (new or existing).
	InitialClusterState string
	// InitialAdvertisePeerURLs is the initial advertise peer URLs.
	InitialAdvertisePeerURLs string
	// ListenPeerURLs is the URLs on which etcd listens for peer traffic.
	ListenPeerURLs string
	// AdvertiseClientURLs is the URLs advertised to clients.
	AdvertiseClientURLs string
	// ListenClientURLs is the URLs on which etcd listens for client traffic.
	ListenClientURLs string
	// AutoCompactionMode is the etcd auto-compaction mode (periodic or revision).
	AutoCompactionMode string
	// AutoCompactionRetention is the etcd auto-compaction retention value.
	AutoCompactionRetention string

	// EmbeddedEtcdQuotaBytes is the etcd backend quota in bytes.
	EmbeddedEtcdQuotaBytes int64

	// EnableSnapshotter enables periodic snapshots.
	EnableSnapshotter bool
	// EnableGC enables garbage collection of old snapshots.
	EnableGC bool
	// EnableDefrag enables periodic defragmentation.
	EnableDefrag bool
	// EnableAlarmHandler enables the etcd alarm handler.
	EnableAlarmHandler bool
	// EnableMemberLeaseRenewal enables member lease renewal.
	EnableMemberLeaseRenewal bool
	// EnableSnapshotLeaseRenewal enables snapshot lease renewal.
	EnableSnapshotLeaseRenewal bool

	// FullSnapshotInterval is the interval between full snapshots.
	FullSnapshotInterval time.Duration
	// DeltaSnapshotInterval is the interval between delta snapshots.
	DeltaSnapshotInterval time.Duration
	// GCPeriod is the garbage collection period.
	GCPeriod time.Duration
	// DefragInterval is the interval between defragmentation runs.
	DefragInterval time.Duration
	// DefragTimeout is the timeout for a single defragmentation operation.
	DefragTimeout time.Duration
	// AlarmPollInterval is the polling interval for alarm checks.
	AlarmPollInterval time.Duration
	// K8sHeartbeatDuration is the heartbeat duration for Kubernetes lease renewal.
	K8sHeartbeatDuration time.Duration
	// LeaderElectionReelectionPeriod is the leader election re-election period.
	LeaderElectionReelectionPeriod time.Duration

	// MaxFullSnapshots is the maximum number of full snapshots to retain.
	MaxFullSnapshots int
	// CompactRevisionLag is the revision lag for compaction.
	CompactRevisionLag int64

	// FullSnapshotLeaseName is the name of the full snapshot lease.
	FullSnapshotLeaseName string
	// DeltaSnapshotLeaseName is the name of the delta snapshot lease.
	DeltaSnapshotLeaseName string
	// DefragSchedule is the cron schedule for defragmentation.
	DefragSchedule string
	// RestorationTempDir is the temporary directory used during restoration.
	RestorationTempDir string
	// ConfigFile is the path to the configuration file.
	ConfigFile string

	// StoreProvider is the snapshot store provider (Local, S3, GCS, ABS).
	StoreProvider string
	// StorePrefix is the key prefix under which snapshots are stored.
	StorePrefix string
	// StoreContainer is the bucket/container name (or root directory for Local).
	StoreContainer string

	// WrapperURL is the base URL for the etcd-wrapper sidecar HTTP API
	// (e.g. "http://localhost:9095"). The steward sends the etcd config
	// to POST /embedded-etcd and controls readiness via POST /readyz/set.
	WrapperURL string
}

// DefaultConfig returns a Config populated with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		ServerPort:            8080,
		EtcdConnectionTimeout: 30 * time.Second,

		DataDir:             "/var/etcd/data/new.etcd",
		InitialClusterState: "new",
		AutoCompactionMode:  "periodic",

		EmbeddedEtcdQuotaBytes: 8 * 1024 * 1024 * 1024, // 8 GiB

		EnableSnapshotter:          true,
		EnableGC:                   true,
		EnableDefrag:               true,
		EnableAlarmHandler:         true,
		EnableMemberLeaseRenewal:   true,
		EnableSnapshotLeaseRenewal: true,

		FullSnapshotInterval:           24 * time.Hour,
		DeltaSnapshotInterval:          5 * time.Minute,
		GCPeriod:                       1 * time.Hour,
		DefragInterval:                 24 * time.Hour,
		DefragTimeout:                  10 * time.Minute,
		AlarmPollInterval:              30 * time.Second,
		K8sHeartbeatDuration:           2 * time.Second,
		LeaderElectionReelectionPeriod: 5 * time.Second,

		MaxFullSnapshots:   7,
		CompactRevisionLag: 10000,

		RestorationTempDir: "/var/etcd/data/restoration.tmp",

		StoreProvider:  "Local",
		StoreContainer: "/var/etcd/data/snapshots",

		WrapperURL: "http://localhost:9095",
	}
}
