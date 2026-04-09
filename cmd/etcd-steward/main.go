// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package main is the entry point for etcd-steward.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/gardener/etcd-steward/pkg/alarm"
	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/config"
	"github.com/gardener/etcd-steward/pkg/etcdclient"
	"github.com/gardener/etcd-steward/pkg/gc"
	"github.com/gardener/etcd-steward/pkg/initializer"
	"github.com/gardener/etcd-steward/pkg/leaderwatch"
	"github.com/gardener/etcd-steward/pkg/lease"
	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/server"
	"github.com/gardener/etcd-steward/pkg/snapshotter"
	"github.com/gardener/etcd-steward/pkg/snapstore"
	"github.com/gardener/etcd-steward/pkg/statemachine"
	"github.com/gardener/etcd-steward/pkg/validator"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	// Handle subcommands.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "compact":
			runCompact(logger)
			return
		case "copy-backups":
			runCopyBackups(logger)
			return
		case "server":
			// fall through — parse flags below
		}
	}

	// Parse CLI flags (injected by etcd-druid StatefulSet builder).
	cfg := config.DefaultConfig()
	fs := pflag.NewFlagSet("etcd-steward", pflag.ContinueOnError)

	fs.IntVar(&cfg.ServerPort, "server-port", cfg.ServerPort, "HTTP server port")
	fs.StringVar(&cfg.EtcdEndpoint, "endpoints", "", "etcd client endpoint URL")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", "etcd initial-cluster string (name=url,...)")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "etcd data directory")
	// Peer URL is derived from initial-cluster + POD_NAME; also accept explicit flag.
	fs.StringVar(&cfg.EtcdPeerURL, "etcd-peer-url", "", "this member's peer URL (optional; derived from initial-cluster if not set)")
	// Cluster bootstrap / listen URLs (informational — etcd-steward passes to etcd process).
	listenPeerURLs := fs.String("listen-peer-urls", "", "etcd listen-peer-urls")
	listenClientURLs := fs.String("listen-client-urls", "", "etcd listen-client-urls")
	// TLS flags for etcd client.
	fs.String("cacert", "", "CA cert for etcd client TLS")
	fs.String("cert", "", "client cert for etcd client TLS")
	fs.String("key", "", "client key for etcd client TLS")
	// TLS flags for HTTP server.
	fs.String("server-cert", "", "TLS cert for etcd-steward HTTP server")
	fs.String("server-key", "", "TLS key for etcd-steward HTTP server")
	// Compaction / defrag.
	fs.StringVar(&cfg.DefragSchedule, "defrag-schedule", "", "cron schedule for defragmentation")
	fs.String("etcd-defrag-timeout", "15m", "timeout for etcd defragmentation")
	fs.String("auto-compaction-mode", "periodic", "etcd auto-compaction mode")
	fs.String("auto-compaction-retention", "30m", "etcd auto-compaction retention")
	// Misc
	fs.String("etcd-connection-timeout", "5m", "timeout for etcd connection")
	fs.Bool("enable-member-lease-renewal", cfg.EnableMemberLeaseRenewal, "enable K8s member lease renewal")
	fs.Duration("k8s-heartbeat-duration", 10*time.Second, "K8s heartbeat duration")
	fs.String("reelection-period", "", "leader reelection period")
	// Backup store flags (passed when backup is configured).
	fs.String("store-prefix", "", "snapstore prefix")
	fs.String("store-container", "", "snapstore container/bucket")
	fs.String("storage-provider", "", "snapstore provider (S3, GCS, ABS, Local)")
	fs.String("store-tempdir", "", "snapstore temp directory")
	// Snapshot schedule / retention flags (passed by etcd-druid when backup is configured).
	fs.String("schedule", "", "full snapshot cron schedule")
	fs.Duration("delta-snapshot-period", 1*time.Minute, "delta snapshot period")
	fs.Int64("delta-snapshot-memory-limit", 100*1024*1024, "delta snapshot memory limit in bytes")
	fs.Duration("delta-snapshot-retention-period", 0, "delta snapshot retention period")
	fs.String("garbage-collection-policy", "Exponential", "snapshot garbage collection policy")
	fs.Duration("garbage-collection-period", 12*time.Hour, "snapshot garbage collection period")
	fs.Int64("max-backups", 7, "maximum number of backups (LimitBased GC policy)")
	fs.Bool("compress-snapshots", false, "enable snapshot compression")
	fs.String("compression-policy", "gzip", "snapshot compression policy")
	fs.String("etcd-snapshot-timeout", "10m", "timeout for etcd snapshot operation")

	// Parse args, skipping the "server" subcommand token if present.
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "server" {
		args = args[1:]
	}
	if err := fs.Parse(args); err != nil {
		logger.Fatal("failed to parse flags", zap.Error(err))
	}

	// Resolve identity from env vars (downward API).
	cfg.PodName = os.Getenv("POD_NAME")
	cfg.PodNamespace = os.Getenv("POD_NAMESPACE")

	// Also accept env var overrides for endpoint / peer URL / initial-cluster.
	if v := os.Getenv("ETCD_ENDPOINT"); v != "" && cfg.EtcdEndpoint == "" {
		cfg.EtcdEndpoint = v
	}
	if v := os.Getenv("ETCD_PEER_URL"); v != "" && cfg.EtcdPeerURL == "" {
		cfg.EtcdPeerURL = v
	}
	if v := os.Getenv("INITIAL_CLUSTER"); v != "" && cfg.InitialCluster == "" {
		cfg.InitialCluster = v
	}

	// Derive EtcdPeerURL from initial-cluster + POD_NAME if not set explicitly.
	if cfg.EtcdPeerURL == "" && cfg.PodName != "" && cfg.InitialCluster != "" {
		cfg.EtcdPeerURL = derivePeerURL(cfg.PodName, cfg.InitialCluster)
	}

	// Derive IsSingleNode from replica count encoded in initial-cluster.
	cfg.IsSingleNode = countClusterMembers(cfg.InitialCluster) == 1

	// Configure snapstore from flags if storage-provider is set.
	provider, _ := fs.GetString("storage-provider")
	if provider != "" {
		container, _ := fs.GetString("store-container")
		prefix, _ := fs.GetString("store-prefix")
		tempDir, _ := fs.GetString("store-tempdir")
		cfg.Snapstore = snapstore.SnapstoreConfig{
			Provider:  provider,
			Container: container,
			Prefix:    prefix,
			TempDir:   tempDir,
		}
		cfg.EnableSnapshots = true
		cfg.EnableGC = true
		cfg.EnableRestoration = true
	}

	// Configure enable-member-lease-renewal from flag.
	if enableLease, err := fs.GetBool("enable-member-lease-renewal"); err == nil {
		cfg.EnableMemberLeaseRenewal = enableLease
	}

	// Configure delta-snapshot-period from flag.
	if period, err := fs.GetDuration("delta-snapshot-period"); err == nil {
		cfg.DeltaSnapshotPeriod = period
	}

	// Configure compression policy: only override default when compress-snapshots is true
	// and a compression-policy flag was explicitly provided.
	if compressEnabled, err := fs.GetBool("compress-snapshots"); err == nil && compressEnabled {
		if policy, err := fs.GetString("compression-policy"); err == nil && policy != "" {
			cfg.CompressionPolicy = policy
		}
	} else if !compressEnabled {
		cfg.CompressionPolicy = "none"
	}

	// Capture listen URLs for etcd config YAML.
	capturedListenPeerURLs := *listenPeerURLs
	capturedListenClientURLs := *listenClientURLs
	if capturedListenPeerURLs != "" {
		logger.Info("listen-peer-urls", zap.String("urls", capturedListenPeerURLs))
	}
	if capturedListenClientURLs != "" {
		logger.Info("listen-client-urls", zap.String("urls", capturedListenClientURLs))
	}

	cfg.DeriveEtcdName()

	if errs := cfg.Validate(); len(errs) > 0 {
		for _, e := range errs {
			logger.Error("config validation error", zap.Error(e))
		}
		os.Exit(1)
	}

	logger.Info("etcd-steward starting",
		zap.String("podName", cfg.PodName),
		zap.String("podNamespace", cfg.PodNamespace),
		zap.String("etcdEndpoint", cfg.EtcdEndpoint),
		zap.String("etcdPeerURL", cfg.EtcdPeerURL),
		zap.Bool("isSingleNode", cfg.IsSingleNode),
	)

	// Setup context with signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, initiating shutdown", zap.String("signal", sig.String()))
		cancel()
	}()

	// Build K8s clients.
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Fatal("failed to get in-cluster config", zap.Error(err))
	}

	dynamicClient, err := dynamic.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("failed to create dynamic client", zap.Error(err))
	}

	k8sClientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("failed to create kubernetes clientset", zap.Error(err))
	}

	// Build etcd client.
	etcdClientCfg := clientv3.Config{
		Endpoints:   []string{cfg.EtcdEndpoint},
		DialTimeout: 5 * time.Second,
	}

	// Configure TLS if cacert/cert/key flags are provided.
	caCertPath, _ := fs.GetString("cacert")
	certPath, _ := fs.GetString("cert")
	keyPath, _ := fs.GetString("key")
	if caCertPath != "" && certPath != "" && keyPath != "" {
		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			logger.Fatal("failed to read CA cert", zap.String("path", caCertPath), zap.Error(err))
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			logger.Fatal("failed to parse CA cert", zap.String("path", caCertPath))
		}
		clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			logger.Fatal("failed to load client cert/key", zap.Error(err))
		}
		etcdClientCfg.TLS = &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caPool,
		}
	}

	etcdClient, err := clientv3.New(etcdClientCfg)
	if err != nil {
		logger.Fatal("failed to create etcd client", zap.Error(err))
	}
	defer etcdClient.Close() //nolint:errcheck

	// Build components.
	recorder := statemachine.NewK8sRecorder(dynamicClient)
	memberClient := member.NewK8sClient(dynamicClient)
	clusterClient := etcdclient.NewClusterClient(etcdClient)

	// Determine validation mode.
	valMode := validator.DetermineMode(cfg.DataDir)

	// Build snapstore and compressor (if backup configured).
	var store snapstore.Snapstore
	var comp compression.Compressor
	if cfg.Snapstore.Provider != "" {
		store, err = snapstore.NewSnapstore(cfg.Snapstore)
		if err != nil {
			logger.Fatal("failed to create snapstore", zap.Error(err))
		}
		comp, err = compression.NewCompressor(cfg.CompressionPolicy)
		if err != nil {
			logger.Fatal("failed to create compressor", zap.Error(err))
		}
	}

	// Check for create-as-learner annotation.
	hasLearnerAnnotation := os.Getenv("CREATE_AS_LEARNER") == "true"

	// Build initializer.
	init := initializer.New(
		cfg.PodName, cfg.PodNamespace, cfg.EtcdPeerURL,
		cfg.DataDir, cfg.RestorationTempDir, cfg.InitialCluster,
		cfg.IsSingleNode,
		hasLearnerAnnotation,
		recorder,
		memberClient,
		clusterClient,
		nil, // etcdStatusAPI — will be nil until etcd is started
		cfg.EtcdEndpoint,
		logger.Named("initializer"),
		store,
		comp,
	)

	// Build snapshotter (if backup configured).
	var snap *snapshotter.Snapshotter
	if store != nil {
		snap = snapshotter.New(
			store, comp,
			cfg.EtcdName, cfg.PodNamespace,
			etcdClient, etcdClient, etcdClient,
			func() bool {
				return !init.IsLearner()
			},
			logger.Named("snapshotter"),
		)
	}

	// Build HTTP server.
	// configFn returns the etcd YAML config that etcd-wrapper passes to embed.ConfigFromFile.
	// When etcd-druid mounts the config at /var/etcd/config/etcd.conf.yaml, we process it:
	// per-member fields (advertise-client-urls, initial-advertise-peer-urls) are keyed by member
	// name; we extract the value for our pod name and flatten to a string.
	const etcdConfigFilePath = "/var/etcd/config/etcd.conf.yaml"
	configFn := func() ([]byte, error) {
		raw, err := os.ReadFile(etcdConfigFilePath)
		if err == nil {
			return processEtcdConfig(raw, cfg.PodName)
		}
		// Fallback: build minimal config from CLI flags (used in tests / non-druid deployments).
		advertisePeerURLs := cfg.EtcdPeerURL
		if advertisePeerURLs == "" {
			advertisePeerURLs = "http://localhost:2380"
		}
		advertiseClientURLs := cfg.EtcdEndpoint
		if advertiseClientURLs == "" {
			advertiseClientURLs = "http://localhost:2379"
		}
		lpu := capturedListenPeerURLs
		if lpu == "" {
			lpu = "http://localhost:2380"
		}
		// Use only the first URL from listen-client-urls to avoid "address already in use" when
		// the flag contains both 0.0.0.0:port and 127.0.0.1:port (0.0.0.0 already covers all).
		lcu := firstURL(capturedListenClientURLs, "http://localhost:2379")
		yamlContent := fmt.Sprintf(
			"name: %s\ndata-dir: %s\ninitial-cluster: %s\ninitial-advertise-peer-urls: %s\nadvertise-client-urls: %s\nlisten-peer-urls: %s\nlisten-client-urls: %s\n",
			cfg.PodName, cfg.DataDir, cfg.InitialCluster,
			advertisePeerURLs, advertiseClientURLs,
			lpu, lcu,
		)
		return []byte(yamlContent), nil
	}

	var snapIface server.Snapshotter
	if snap != nil {
		snapIface = snap
	}

	// Get TLS cert/key for HTTPS server if provided.
	serverCert, _ := fs.GetString("server-cert")
	serverKey, _ := fs.GetString("server-key")

	srv := server.NewServerWithTLS(
		cfg.ServerPort,
		serverCert, serverKey,
		func() initializer.InitializationStatus { return init.GetStatus() },
		func(srvCtx context.Context, mode string) error { return init.Start(srvCtx, mode) },
		configFn,
		snapIface,
		store,
		logger.Named("server"),
	)

	// Start all goroutines.
	var wg sync.WaitGroup

	// HTTP server — must start before init so etcd-wrapper can poll /initialization/status.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Run(ctx); err != nil {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	// Leader watcher.
	watcher := leaderwatch.New(
		5*time.Second,
		recorder,
		cfg.PodName, cfg.PodNamespace,
		etcdClient,
		logger.Named("leaderwatch"),
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		watcher.Run(ctx, cfg.EtcdEndpoint)
	}()

	// peerTLSEnabled is true when the peer URL uses the https scheme.
	// Used both for lease annotation and EtcdMember status.
	peerTLSEnabled := strings.HasPrefix(cfg.EtcdPeerURL, "https://")

	// Member lease renewal.
	if cfg.EnableMemberLeaseRenewal {
		leaseRenewer := lease.New(
			cfg.PodName,
			cfg.PodNamespace,
			cfg.PodName,
			cfg.MemberLeaseRenewalInterval,
			k8sClientset.CoordinationV1(),
			func() (string, string, string) {
				memberID, clusterID := init.GetMemberAndClusterID(ctx)
				role := string(watcher.GetCurrentRole())
				return memberID, clusterID, role
			},
			peerTLSEnabled,
			logger.Named("lease"),
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			leaseRenewer.Run(ctx)
		}()
	}

	// Snapshot schedule.
	if snap != nil && cfg.EnableSnapshots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap.RunFullSnapshotSchedule(ctx, cfg.FullSnapshotSchedule)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap.RunDeltaSnapshotLoop(ctx, cfg.DeltaSnapshotPeriod)
		}()
	}

	// GC.
	if store != nil && cfg.EnableGC {
		collector := gc.New(store, cfg.MaxFullSnapshots, logger.Named("gc"))
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.Run(ctx, cfg.GarbageCollectionPeriod, func() bool {
				return watcher.GetCurrentRole() == leaderwatch.RoleLeader
			})
		}()
	}

	// Alarm handler + status reconciler.
	statusReconciler := member.NewStatusReconciler(
		memberClient,
		cfg.PodName, cfg.PodNamespace,
		30*time.Second,
		logger.Named("status-reconciler"),
	)

	// Register leaderwatch as a provider (always active).
	statusReconciler.RegisterProvider("leaderwatch", watcher)

	if snap != nil {
		statusReconciler.RegisterProvider("snapshotter", snap)
	}

	if cfg.EnableAlarmHandler {
		alarmHandler := alarm.New(
			etcdClient,
			etcdClient,
			cfg.EtcdEndpoint,
			cfg.CompactRevisionLag,
			cfg.AlarmCheckInterval,
			cfg.PodNamespace, cfg.EtcdName,
			logger.Named("alarm"),
		)
		statusReconciler.RegisterProvider("alarm", alarmHandler)
		wg.Add(1)
		go func() {
			defer wg.Done()
			alarmHandler.Run(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := statusReconciler.Run(ctx); err != nil {
			logger.Error("member status reconciler error", zap.Error(err))
		}
	}()

	// Auto-start initialization with the determined mode.
	if err := init.Start(ctx, string(valMode)); err != nil {
		logger.Error("failed to start initialization", zap.Error(err))
	}

	// Write PeerTLSEnabled to EtcdMember status once at startup.
	// Peer TLS is active when the peer URL uses the https scheme.
	if err := memberClient.UpdateStatus(ctx, cfg.PodName, cfg.PodNamespace, member.UpdateStatusOpts{
		PeerTLSEnabled: &peerTLSEnabled,
	}); err != nil {
		logger.Warn("failed to set peerTLSEnabled on EtcdMember", zap.Error(err))
	}

	// Wait for context cancellation.
	<-ctx.Done()

	// Write exit marker.
	if err := validator.WriteExitMarker(cfg.DataDir, "terminated"); err != nil {
		logger.Error("failed to write exit marker", zap.Error(err))
	}

	logger.Info("waiting for goroutines to finish")
	wg.Wait()
	logger.Info("etcd-steward shutdown complete")
}

// derivePeerURL finds this member's peer URL from the initial-cluster string.
// initial-cluster format: "name1=url1,name2=url2,..."
func derivePeerURL(podName, initialCluster string) string {
	for _, part := range splitComma(initialCluster) {
		idx := indexByte(part, '=')
		if idx < 0 {
			continue
		}
		name := part[:idx]
		url := part[idx+1:]
		if name == podName {
			return url
		}
	}
	return ""
}

// countClusterMembers counts the number of members in initial-cluster.
func countClusterMembers(initialCluster string) int {
	if initialCluster == "" {
		return 0
	}
	return len(splitComma(initialCluster))
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func runCompact(logger *zap.Logger) {
	logger.Info("compact subcommand not yet fully implemented")
	os.Exit(0)
}

func runCopyBackups(logger *zap.Logger) {
	logger.Info("copy-backups subcommand not yet fully implemented")
	os.Exit(0)
}

// firstURL returns the first comma-separated URL from urls, or fallback if urls is empty.
func firstURL(urls, fallback string) string {
	if urls == "" {
		return fallback
	}
	idx := indexByte(urls, ',')
	if idx < 0 {
		return urls
	}
	return urls[:idx]
}

// processEtcdConfig converts the druid-generated ConfigMap YAML to a flat etcd config YAML.
//
// etcd-druid generates a ConfigMap with per-member maps for certain fields:
//
//	advertise-client-urls:
//	  <member-name>:
//	  - url
//
// embed.ConfigFromFile expects a flat scalar for these fields. This function extracts the
// value for memberName and rewrites those fields as flat comma-joined strings.
func processEtcdConfig(raw []byte, memberName string) ([]byte, error) {
	// Parse YAML into a generic map.
	var cfg map[string]interface{}
	if err := unmarshalYAML(raw, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse etcd config: %w", err)
	}

	// Fields that etcd-druid stores as per-member maps.
	perMemberFields := []string{"advertise-client-urls", "initial-advertise-peer-urls"}
	for _, field := range perMemberFields {
		v, ok := cfg[field]
		if !ok {
			continue
		}
		memberMap, ok := v.(map[string]interface{})
		if !ok {
			// Already a scalar — nothing to do.
			continue
		}
		// Extract the list for this member and join to a comma-separated string.
		memberURLs, ok := memberMap[memberName]
		if !ok {
			// Fallback: try first value in map.
			for _, val := range memberMap {
				memberURLs = val
				break
			}
		}
		if memberURLs == nil {
			continue
		}
		switch urls := memberURLs.(type) {
		case []interface{}:
			parts := make([]string, 0, len(urls))
			for _, u := range urls {
				if s, ok := u.(string); ok {
					parts = append(parts, s)
				}
			}
			cfg[field] = strings.Join(parts, ",")
		case string:
			cfg[field] = urls
		}
	}

	// Set name to the actual member name (druid uses "etcd-config" as placeholder).
	cfg["name"] = memberName

	// Re-marshal to YAML.
	return marshalYAML(cfg)
}
