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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/gardener/etcd-steward/cmd/etcd-steward/compact"
	"github.com/gardener/etcd-steward/cmd/etcd-steward/copybackups"
	"github.com/gardener/etcd-steward/pkg/alarm"
	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/config"
	"github.com/gardener/etcd-steward/pkg/defrag"
	"github.com/gardener/etcd-steward/pkg/etcdclient"
	"github.com/gardener/etcd-steward/pkg/gc"
	"github.com/gardener/etcd-steward/pkg/initializer"
	"github.com/gardener/etcd-steward/pkg/leaderwatch"
	"github.com/gardener/etcd-steward/pkg/lease"
	"github.com/gardener/etcd-steward/pkg/lock"
	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/server"
	"github.com/gardener/etcd-steward/pkg/snapshotter"
	"github.com/gardener/etcd-steward/pkg/snapshotlease"
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
	fs.StringVar(&cfg.ServiceEndpoint, "service-endpoints", "", "etcd ClusterIP service endpoint URL for scale-out cluster detection (optional)")
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
	fs.Bool("enable-snapshot-lease-updates", true, "update K8s snapshot leases after each snapshot (disable when etcd-druid uses UseEtcdSteward mode)")

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

	// Configure snapshot lease updates (default true — backward compatible).
	if v, err := fs.GetBool("enable-snapshot-lease-updates"); err == nil {
		cfg.EnableSnapshotLeaseUpdates = v
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

	// Build service cluster client if a ClusterIP service endpoint is configured.
	// This client connects to the cluster-level service (e.g. test-client:2379) and is used
	// during scale-out to detect the existing cluster without requiring the local etcd to be up.
	var serviceClusterClient etcdclient.ClusterClient
	if cfg.ServiceEndpoint != "" {
		serviceClientCfg := clientv3.Config{
			Endpoints:   []string{cfg.ServiceEndpoint},
			DialTimeout: 5 * time.Second,
		}
		// Reuse TLS config from local client.
		serviceClientCfg.TLS = etcdClientCfg.TLS
		serviceEtcdClient, err := clientv3.New(serviceClientCfg)
		if err != nil {
			logger.Warn("failed to create service cluster client, scale-out detection may not work",
				zap.String("serviceEndpoint", cfg.ServiceEndpoint),
				zap.Error(err),
			)
		} else {
			defer serviceEtcdClient.Close() //nolint:errcheck
			serviceClusterClient = etcdclient.NewClusterClient(serviceEtcdClient)
		}
	}

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
	// Inject the etcd status client so GetMemberAndClusterID can return real IDs.
	// etcdClient implements the maintenance.Status() call used by EtcdStatusAPI.
	init.SetEtcdStatusAPI(&etcdStatusAdapter{client: etcdClient})
	// Inject service cluster client for scale-out detection (if configured).
	if serviceClusterClient != nil {
		init.SetServiceClusterClient(serviceClusterClient, cfg.ServiceEndpoint)
	}

	// Build snapshotter (if backup configured).
	var snap *snapshotter.Snapshotter
	if store != nil {
		var snapshotLock *lock.Lock
		if cfg.EnableDistributedLock {
			snapshotLock = lock.New(etcdClient, cfg.PodNamespace, cfg.EtcdName+"-snapshot")
		}
		var leaseUpdater snapshotter.SnapshotLeaseUpdater
		if cfg.EnableSnapshotLeaseUpdates {
			leaseUpdater = snapshotlease.New(cfg.EtcdName, cfg.PodNamespace, k8sClientset.CoordinationV1(), logger.Named("snapshotlease"))
		}
		snap = snapshotter.New(
			store, comp,
			cfg.EtcdName, cfg.PodNamespace,
			etcdClient, etcdClient, etcdClient,
			func() bool {
				return !init.IsLearner()
			},
			snapshotLock,
			leaseUpdater,
			logger.Named("snapshotter"),
		)
	}

	// Build HTTP server.
	// configFn returns the etcd YAML config that etcd-wrapper passes to embed.ConfigFromFile.
	// When etcd-druid mounts the config at /var/etcd/config/etcd.conf.yaml, we process it:
	// per-member fields (advertise-client-urls, initial-advertise-peer-urls) are keyed by member
	// name; we extract the value for our pod name and flatten to a string.
	// When the member is joining an existing cluster (scale-out or data-loss recovery),
	// initial-cluster-state is overridden to "existing" so etcd joins the cluster as a
	// learner instead of bootstrapping a new cluster.
	const etcdConfigFilePath = "/var/etcd/config/etcd.conf.yaml"
	configFn := func() ([]byte, error) {
		clusterState := "new"
		var initialClusterOverride string
		if init.NeedsExistingClusterState() {
			clusterState = "existing"
			// When joining an existing cluster, initial-cluster must contain only the
			// current cluster members (not all planned replicas). etcd validates that the
			// count of initial-cluster entries equals the actual cluster member count.
			//
			// Build the name→peerURL map from the ConfigMap's initial-cluster field.
			// These are the authoritative final URLs (e.g. https:// after TLS migration).
			configMapEntries := buildPeerURLMap(cfg.InitialCluster)
			configCtx, configCancel := context.WithTimeout(ctx, 15*time.Second)
			initialClusterOverride = init.GetCurrentMembersInitialCluster(configCtx, configMapEntries)
			configCancel()
		}
		raw, err := os.ReadFile(etcdConfigFilePath)
		if err == nil {
			return processEtcdConfig(raw, cfg.PodName, clusterState, initialClusterOverride)
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
		initialCluster := cfg.InitialCluster
		if initialClusterOverride != "" {
			initialCluster = initialClusterOverride
		}
		yamlContent := fmt.Sprintf(
			"name: %s\ndata-dir: %s\ninitial-cluster: %s\ninitial-cluster-state: %s\ninitial-advertise-peer-urls: %s\nadvertise-client-urls: %s\nlisten-peer-urls: %s\nlisten-client-urls: %s\n",
			cfg.PodName, cfg.DataDir, initialCluster, clusterState,
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

	// Peer URL reconciler: after etcd starts, update the cluster membership record if our
	// registered peer URL differs from the configured one. This can happen when TLS is enabled
	// on an existing member — etcd does NOT auto-update the advertised peer URL in the cluster
	// membership when restarting with a new initial-advertise-peer-urls. Without this, new members
	// joining after TLS migration will see the stale http:// URL and fail to connect.
	if cfg.EtcdPeerURL != "" && !cfg.IsSingleNode {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reconcilePeerURL(ctx, cfg.EtcdPeerURL, clusterClient, init, logger)
		}()
	}

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

	// Defragmentation (Option-3: leader-orchestrated via etcd keys).
	if cfg.EnableDefrag {
		defragOrchestrator := defrag.NewOrchestrator(
			etcdClient,
			etcdClient,
			cfg.PodNamespace, cfg.EtcdName,
			cfg.EtcdEndpoint,
			logger.Named("defrag-orchestrator"),
		)
		statusReconciler.RegisterProvider("defrag-orchestrator", defragOrchestrator)

		defragParticipant := defrag.NewParticipant(
			etcdClient,
			etcdClient,
			cfg.PodNamespace, cfg.EtcdName,
			cfg.PodName,
			cfg.EtcdEndpoint,
			logger.Named("defrag-participant"),
		)
		statusReconciler.RegisterProvider("defrag-participant", defragParticipant)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defragParticipant.Run(ctx)
		}()

		// Orchestrator runs only on leader; it discovers member names from clusterClient.
		wg.Add(1)
		go func() {
			defer wg.Done()
			getMemberNames := func(ctx context.Context) ([]string, error) {
				members, err := clusterClient.ListMembers(ctx)
				if err != nil {
					return nil, err
				}
				names := make([]string, 0, len(members))
				for _, m := range members {
					names = append(names, m.Name)
				}
				return names, nil
			}
			defragOrchestrator.Run(ctx, cfg.PodName, getMemberNames, cfg.DefragPeriod)
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

// buildPeerURLMap parses an initial-cluster string ("name=url,...") into a
// name → peerURL map. Used to supply authoritative ConfigMap URLs to
// GetCurrentMembersInitialCluster when the live cluster may show stale URLs
// (e.g. during a TLS migration that changes http:// to https://).
func buildPeerURLMap(initialCluster string) map[string]string {
	m := make(map[string]string)
	for _, part := range splitComma(initialCluster) {
		idx := indexByte(part, '=')
		if idx < 0 {
			continue
		}
		m[part[:idx]] = part[idx+1:]
	}
	return m
}

// reconcilePeerURL waits for initialization to complete, then checks whether this member's
// registered peer URL in the cluster matches cfg.EtcdPeerURL. If they differ (e.g. after
// enabling peer TLS: registered=http:// but configured=https://), it calls MemberUpdate to fix
// the cluster membership record. Without this, new members joining after TLS migration would
// use the stale http:// URL from initial-cluster and fail to connect.
func reconcilePeerURL(ctx context.Context, configuredPeerURL string, cc etcdclient.ClusterClient, init *initializer.Initializer, logger *zap.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Wait until initialization is complete and etcd is running.
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if init.GetStatus() == initializer.InitializationStatusSuccessful {
				goto ready
			}
		}
	}
ready:
	// Strip scheme for comparison (http vs https).
	normURL := func(u string) string {
		if idx := strings.Index(u, "://"); idx >= 0 {
			return u[idx+3:]
		}
		return u
	}
	normConfigured := normURL(configuredPeerURL)

	// Retry a few times in case etcd is still starting.
	for attempt := 0; attempt < 10; attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		members, err := cc.ListMembers(listCtx)
		cancel()
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}
		for _, m := range members {
			for _, u := range m.PeerURLs {
				if normURL(u) == normConfigured && u != configuredPeerURL {
					// Found our member with a stale URL (different scheme). Update it.
					logger.Info("updating stale peer URL in cluster membership",
						zap.String("from", u),
						zap.String("to", configuredPeerURL),
						zap.Uint64("memberID", m.ID),
					)
					updateCtx, updateCancel := context.WithTimeout(ctx, 10*time.Second)
					updateErr := cc.UpdateMemberPeerURL(updateCtx, m.ID, configuredPeerURL)
					updateCancel()
					if updateErr != nil {
						logger.Warn("failed to update peer URL", zap.Error(updateErr))
					}
					return
				}
				if u == configuredPeerURL {
					// Already correct.
					return
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}

// countClusterMembers counts the number of members in initial-cluster.
func countClusterMembers(initialCluster string) int {	if initialCluster == "" {
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
	// Parse compact-specific args (skip "compact" token at index 0).
	args := os.Args[2:]
	opts, err := compact.ParseArgs(args)
	if err != nil {
		logger.Fatal("compact: invalid arguments", zap.Error(err))
	}

	// Start a minimal HTTP server exposing /metrics so Prometheus can scrape job metrics.
	metricsSrv := startMetricsOnlyServer(logger, 8080)
	defer metricsSrv.Shutdown(context.Background()) //nolint:errcheck

	if err := compact.Run(logger, opts); err != nil {
		logger.Fatal("compact failed", zap.Error(err))
	}
}

func runCopyBackups(logger *zap.Logger) {
	// Parse copy-backups-specific args (skip "copy-backups" token at index 0).
	args := os.Args[2:]
	opts, err := copybackups.ParseArgs(args)
	if err != nil {
		logger.Fatal("copy-backups: invalid arguments", zap.Error(err))
	}

	// Start a minimal HTTP server exposing /metrics so Prometheus can scrape job metrics.
	metricsSrv := startMetricsOnlyServer(logger, 8080)
	defer metricsSrv.Shutdown(context.Background()) //nolint:errcheck

	if err := copybackups.Run(logger, opts); err != nil {
		logger.Fatal("copy-backups failed", zap.Error(err))
	}
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

// startMetricsOnlyServer starts a minimal HTTP server that serves only /metrics on the
// given port. It is used by the compact and copy-backups subcommands so that Prometheus
// can scrape job-level metrics even from short-lived one-shot processes.
// The returned *http.Server must be shut down by the caller.
func startMetricsOnlyServer(logger *zap.Logger, port int) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		logger.Info("starting metrics server", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	return srv
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
// clusterState overrides the initial-cluster-state field ("new" or "existing").
// initialClusterOverride, when non-empty, replaces the initial-cluster field. This is used
// when joining an existing cluster (learner mode) so the field contains only the current
// cluster members instead of all planned replicas.
func processEtcdConfig(raw []byte, memberName, clusterState, initialClusterOverride string) ([]byte, error) {
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

	// Override initial-cluster when joining an existing cluster (learner mode).
	// The ConfigMap lists all planned replicas, but etcd validates that initial-cluster
	// count matches actual cluster member count — so we must provide only current members.
	if initialClusterOverride != "" {
		cfg["initial-cluster"] = initialClusterOverride
	}

	// Override initial-cluster-state: etcd-druid always writes "new" in the ConfigMap,
	// but when a member is joining an existing cluster (scale-out or data-loss recovery)
	// we must use "existing" so etcd joins rather than bootstrapping a fresh cluster.
	cfg["initial-cluster-state"] = clusterState

	// Re-marshal to YAML.
	return marshalYAML(cfg)
}

// etcdStatusAdapter adapts *clientv3.Client to initializer.EtcdStatusAPI.
// clientv3.Maintenance.Status returns *clientv3.StatusResponse; the initializer
// interface expects *initializer.EtcdStatusResponse with just MemberID/ClusterID.
type etcdStatusAdapter struct {
	client *clientv3.Client
}

func (a *etcdStatusAdapter) Status(ctx context.Context, endpoint string) (*initializer.EtcdStatusResponse, error) {
	resp, err := a.client.Status(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return &initializer.EtcdStatusResponse{
		MemberID:  resp.Header.MemberId,
		ClusterID: resp.Header.ClusterId,
	}, nil
}
