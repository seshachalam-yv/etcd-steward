// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"strings"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// BindFlags registers all config fields as pflags on the given FlagSet.
// Flag names use kebab-case and map 1:1 to Config fields.
func BindFlags(cfg *Config, fs *pflag.FlagSet) {
	fs.IntVar(&cfg.ServerPort, "server-port", cfg.ServerPort, "HTTP server listen port")
	fs.StringVar(&cfg.PodName, "pod-name", cfg.PodName, "name of the pod running etcd-steward")
	fs.StringVar(&cfg.PodNamespace, "pod-namespace", cfg.PodNamespace, "Kubernetes namespace of the pod")

	fs.StringSliceVar(&cfg.EtcdEndpoints, "etcd-endpoints", cfg.EtcdEndpoints, "comma-separated list of etcd client endpoints")
	fs.DurationVar(&cfg.EtcdConnectionTimeout, "etcd-connection-timeout", cfg.EtcdConnectionTimeout, "timeout for connecting to etcd")

	fs.StringVar(&cfg.CACert, "ca-cert", cfg.CACert, "path to the CA certificate for TLS")
	fs.StringVar(&cfg.Cert, "cert", cfg.Cert, "path to the client certificate for TLS")
	fs.StringVar(&cfg.Key, "key", cfg.Key, "path to the client private key for TLS")

	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "etcd data directory")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", cfg.InitialCluster, "initial etcd cluster configuration")
	fs.StringVar(&cfg.InitialClusterToken, "initial-cluster-token", cfg.InitialClusterToken, "initial etcd cluster token")
	fs.StringVar(&cfg.InitialClusterState, "initial-cluster-state", cfg.InitialClusterState, "initial etcd cluster state (new or existing)")
	fs.StringVar(&cfg.InitialAdvertisePeerURLs, "initial-advertise-peer-urls", cfg.InitialAdvertisePeerURLs, "initial advertise peer URLs")
	fs.StringVar(&cfg.ListenPeerURLs, "listen-peer-urls", cfg.ListenPeerURLs, "URLs on which etcd listens for peer traffic")
	fs.StringVar(&cfg.AdvertiseClientURLs, "advertise-client-urls", cfg.AdvertiseClientURLs, "URLs advertised to clients")
	fs.StringVar(&cfg.ListenClientURLs, "listen-client-urls", cfg.ListenClientURLs, "URLs on which etcd listens for client traffic")
	fs.StringVar(&cfg.AutoCompactionMode, "auto-compaction-mode", cfg.AutoCompactionMode, "etcd auto-compaction mode (periodic or revision)")
	fs.StringVar(&cfg.AutoCompactionRetention, "auto-compaction-retention", cfg.AutoCompactionRetention, "etcd auto-compaction retention value")

	fs.Int64Var(&cfg.EmbeddedEtcdQuotaBytes, "embedded-etcd-quota-bytes", cfg.EmbeddedEtcdQuotaBytes, "etcd backend quota in bytes")

	fs.BoolVar(&cfg.EnableSnapshotter, "enable-snapshotter", cfg.EnableSnapshotter, "enable periodic snapshots")
	fs.BoolVar(&cfg.EnableGC, "enable-gc", cfg.EnableGC, "enable garbage collection of old snapshots")
	fs.BoolVar(&cfg.EnableDefrag, "enable-defrag", cfg.EnableDefrag, "enable periodic defragmentation")
	fs.BoolVar(&cfg.EnableAlarmHandler, "enable-alarm-handler", cfg.EnableAlarmHandler, "enable the etcd alarm handler")
	fs.BoolVar(&cfg.EnableMemberLeaseRenewal, "enable-member-lease-renewal", cfg.EnableMemberLeaseRenewal, "enable member lease renewal")
	fs.BoolVar(&cfg.EnableSnapshotLeaseRenewal, "enable-snapshot-lease-renewal", cfg.EnableSnapshotLeaseRenewal, "enable snapshot lease renewal")

	fs.DurationVar(&cfg.FullSnapshotInterval, "full-snapshot-interval", cfg.FullSnapshotInterval, "interval between full snapshots")
	fs.DurationVar(&cfg.DeltaSnapshotInterval, "delta-snapshot-interval", cfg.DeltaSnapshotInterval, "interval between delta snapshots")
	fs.DurationVar(&cfg.GCPeriod, "gc-period", cfg.GCPeriod, "garbage collection period")
	fs.DurationVar(&cfg.DefragInterval, "defrag-interval", cfg.DefragInterval, "interval between defragmentation runs")
	fs.DurationVar(&cfg.DefragTimeout, "defrag-timeout", cfg.DefragTimeout, "timeout for a single defragmentation operation")
	fs.DurationVar(&cfg.AlarmPollInterval, "alarm-poll-interval", cfg.AlarmPollInterval, "polling interval for alarm checks")
	fs.DurationVar(&cfg.K8sHeartbeatDuration, "k8s-heartbeat-duration", cfg.K8sHeartbeatDuration, "heartbeat duration for Kubernetes lease renewal")
	fs.DurationVar(&cfg.LeaderElectionReelectionPeriod, "leader-election-reelection-period", cfg.LeaderElectionReelectionPeriod, "leader election re-election period")

	fs.IntVar(&cfg.MaxFullSnapshots, "max-full-snapshots", cfg.MaxFullSnapshots, "maximum number of full snapshots to retain")
	fs.Int64Var(&cfg.CompactRevisionLag, "compact-revision-lag", cfg.CompactRevisionLag, "revision lag for compaction")

	fs.StringVar(&cfg.FullSnapshotLeaseName, "full-snapshot-lease-name", cfg.FullSnapshotLeaseName, "name of the full snapshot lease")
	fs.StringVar(&cfg.DeltaSnapshotLeaseName, "delta-snapshot-lease-name", cfg.DeltaSnapshotLeaseName, "name of the delta snapshot lease")
	fs.StringVar(&cfg.DefragSchedule, "defrag-schedule", cfg.DefragSchedule, "cron schedule for defragmentation")
	fs.StringVar(&cfg.RestorationTempDir, "restoration-temp-dir", cfg.RestorationTempDir, "temporary directory used during restoration")
	fs.StringVar(&cfg.ConfigFile, "config-file", cfg.ConfigFile, "path to the configuration file")

	fs.StringVar(&cfg.StoreProvider, "store-provider", cfg.StoreProvider, "snapshot store provider (Local, S3, GCS, ABS)")
	fs.StringVar(&cfg.StorePrefix, "store-prefix", cfg.StorePrefix, "key prefix under which snapshots are stored")
	fs.StringVar(&cfg.StoreContainer, "store-container", cfg.StoreContainer, "bucket/container name or root directory for Local")
}

// LoadFromFile reads configuration from the file at the given path using viper.
// Values from the file are applied to cfg only when the corresponding flag has
// not been explicitly set on fs. Flags always win over file values.
func LoadFromFile(cfg *Config, path string, fs *pflag.FlagSet) error {
	if path == "" {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return errors.Wrap(errors.ErrCodeConfig, "failed to read config file "+path, err)
	}

	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(strings.NewReader(string(data))); err != nil {
		return errors.Wrap(errors.ErrCodeConfig, "failed to parse config file "+path, err)
	}

	// Apply file values only for flags that were NOT explicitly set on the command line.
	applyStringIfUnset(fs, v, "pod-name", &cfg.PodName)
	applyStringIfUnset(fs, v, "pod-namespace", &cfg.PodNamespace)
	applyIntIfUnset(fs, v, "server-port", &cfg.ServerPort)

	applyStringSliceIfUnset(fs, v, "etcd-endpoints", &cfg.EtcdEndpoints)
	applyDurationIfUnset(fs, v, "etcd-connection-timeout", &cfg.EtcdConnectionTimeout)

	applyStringIfUnset(fs, v, "ca-cert", &cfg.CACert)
	applyStringIfUnset(fs, v, "cert", &cfg.Cert)
	applyStringIfUnset(fs, v, "key", &cfg.Key)

	applyStringIfUnset(fs, v, "data-dir", &cfg.DataDir)
	applyStringIfUnset(fs, v, "initial-cluster", &cfg.InitialCluster)
	applyStringIfUnset(fs, v, "initial-cluster-token", &cfg.InitialClusterToken)
	applyStringIfUnset(fs, v, "initial-cluster-state", &cfg.InitialClusterState)
	applyStringIfUnset(fs, v, "initial-advertise-peer-urls", &cfg.InitialAdvertisePeerURLs)
	applyStringIfUnset(fs, v, "listen-peer-urls", &cfg.ListenPeerURLs)
	applyStringIfUnset(fs, v, "advertise-client-urls", &cfg.AdvertiseClientURLs)
	applyStringIfUnset(fs, v, "listen-client-urls", &cfg.ListenClientURLs)
	applyStringIfUnset(fs, v, "auto-compaction-mode", &cfg.AutoCompactionMode)
	applyStringIfUnset(fs, v, "auto-compaction-retention", &cfg.AutoCompactionRetention)

	applyInt64IfUnset(fs, v, "embedded-etcd-quota-bytes", &cfg.EmbeddedEtcdQuotaBytes)

	applyBoolIfUnset(fs, v, "enable-snapshotter", &cfg.EnableSnapshotter)
	applyBoolIfUnset(fs, v, "enable-gc", &cfg.EnableGC)
	applyBoolIfUnset(fs, v, "enable-defrag", &cfg.EnableDefrag)
	applyBoolIfUnset(fs, v, "enable-alarm-handler", &cfg.EnableAlarmHandler)
	applyBoolIfUnset(fs, v, "enable-member-lease-renewal", &cfg.EnableMemberLeaseRenewal)
	applyBoolIfUnset(fs, v, "enable-snapshot-lease-renewal", &cfg.EnableSnapshotLeaseRenewal)

	applyDurationIfUnset(fs, v, "full-snapshot-interval", &cfg.FullSnapshotInterval)
	applyDurationIfUnset(fs, v, "delta-snapshot-interval", &cfg.DeltaSnapshotInterval)
	applyDurationIfUnset(fs, v, "gc-period", &cfg.GCPeriod)
	applyDurationIfUnset(fs, v, "defrag-interval", &cfg.DefragInterval)
	applyDurationIfUnset(fs, v, "defrag-timeout", &cfg.DefragTimeout)
	applyDurationIfUnset(fs, v, "alarm-poll-interval", &cfg.AlarmPollInterval)
	applyDurationIfUnset(fs, v, "k8s-heartbeat-duration", &cfg.K8sHeartbeatDuration)
	applyDurationIfUnset(fs, v, "leader-election-reelection-period", &cfg.LeaderElectionReelectionPeriod)

	applyIntIfUnset(fs, v, "max-full-snapshots", &cfg.MaxFullSnapshots)
	applyInt64IfUnset(fs, v, "compact-revision-lag", &cfg.CompactRevisionLag)

	applyStringIfUnset(fs, v, "full-snapshot-lease-name", &cfg.FullSnapshotLeaseName)
	applyStringIfUnset(fs, v, "delta-snapshot-lease-name", &cfg.DeltaSnapshotLeaseName)
	applyStringIfUnset(fs, v, "defrag-schedule", &cfg.DefragSchedule)
	applyStringIfUnset(fs, v, "restoration-temp-dir", &cfg.RestorationTempDir)

	applyStringIfUnset(fs, v, "store-provider", &cfg.StoreProvider)
	applyStringIfUnset(fs, v, "store-prefix", &cfg.StorePrefix)
	applyStringIfUnset(fs, v, "store-container", &cfg.StoreContainer)

	return nil
}

// applyStringIfUnset sets *dst from viper if the flag was not explicitly provided.
func applyStringIfUnset(fs *pflag.FlagSet, v *viper.Viper, name string, dst *string) {
	if fs.Lookup(name) != nil && fs.Lookup(name).Changed {
		return
	}
	if v.IsSet(name) {
		*dst = v.GetString(name)
	}
}

// applyIntIfUnset sets *dst from viper if the flag was not explicitly provided.
func applyIntIfUnset(fs *pflag.FlagSet, v *viper.Viper, name string, dst *int) {
	if fs.Lookup(name) != nil && fs.Lookup(name).Changed {
		return
	}
	if v.IsSet(name) {
		*dst = v.GetInt(name)
	}
}

// applyInt64IfUnset sets *dst from viper if the flag was not explicitly provided.
func applyInt64IfUnset(fs *pflag.FlagSet, v *viper.Viper, name string, dst *int64) {
	if fs.Lookup(name) != nil && fs.Lookup(name).Changed {
		return
	}
	if v.IsSet(name) {
		*dst = v.GetInt64(name)
	}
}

// applyBoolIfUnset sets *dst from viper if the flag was not explicitly provided.
func applyBoolIfUnset(fs *pflag.FlagSet, v *viper.Viper, name string, dst *bool) {
	if fs.Lookup(name) != nil && fs.Lookup(name).Changed {
		return
	}
	if v.IsSet(name) {
		*dst = v.GetBool(name)
	}
}

// applyDurationIfUnset sets *dst from viper if the flag was not explicitly provided.
func applyDurationIfUnset(fs *pflag.FlagSet, v *viper.Viper, name string, dst *time.Duration) {
	if fs.Lookup(name) != nil && fs.Lookup(name).Changed {
		return
	}
	if v.IsSet(name) {
		*dst = v.GetDuration(name)
	}
}

// applyStringSliceIfUnset sets *dst from viper if the flag was not explicitly provided.
func applyStringSliceIfUnset(fs *pflag.FlagSet, v *viper.Viper, name string, dst *[]string) {
	if fs.Lookup(name) != nil && fs.Lookup(name).Changed {
		return
	}
	if v.IsSet(name) {
		*dst = v.GetStringSlice(name)
	}
}
