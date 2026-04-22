// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gardener/etcd-steward/cmd/etcdsteward/compact"
	"github.com/gardener/etcd-steward/cmd/etcdsteward/copybackups"
	"github.com/gardener/etcd-steward/internal/alarm"
	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/config"
	"github.com/gardener/etcd-steward/internal/defrag"
	"github.com/gardener/etcd-steward/internal/etcdclient"
	"github.com/gardener/etcd-steward/internal/gc"
	"github.com/gardener/etcd-steward/internal/leaderwatch"
	"github.com/gardener/etcd-steward/internal/lease"
	"github.com/gardener/etcd-steward/internal/member"
	"github.com/gardener/etcd-steward/internal/restorer"
	"github.com/gardener/etcd-steward/internal/server"
	"github.com/gardener/etcd-steward/internal/snapshotter"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"github.com/gardener/etcd-steward/internal/statemachine"
	"github.com/gardener/etcd-steward/internal/validator"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	k8srest "k8s.io/client-go/rest"
)

// Version is set via ldflags at build time.
var Version = "dev"

// etcdConfigPath is the default path where druid mounts the etcd ConfigMap.
const etcdConfigPath = "/var/etcd/config/etcd.conf.yaml"

// etcdReadyPollInterval is how often we check if etcd is ready.
const etcdReadyPollInterval = 2 * time.Second

// etcdReadyTimeout is the maximum time to wait for etcd to become ready.
const etcdReadyTimeout = 5 * time.Minute

// initHandler manages the asynchronous, stateful initialization lifecycle
// that the etcd-wrapper polls via /initialization/status and triggers via
// /initialization/start.
type initHandler struct {
	mu        sync.Mutex
	status    server.InitStatus
	validator *validator.Validator
	restorer  *restorer.Restorer // nil if no backup store configured
	cfg       *config.Config
	logger    *zap.Logger
}

// getStatus returns the current initialization status. If the status is
// Successful or Failed, it resets to New on the next read (matching
// backup-restore behavior so that re-initialization is possible on pod
// restart).
func (h *initHandler) getStatus() server.InitStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.status
	if s == server.InitStatusSuccessful || s == server.InitStatusFailed {
		h.status = server.InitStatusNew
	}
	return s
}

// startInit triggers an asynchronous initialization. If initialization is
// already in progress or completed, the call is a no-op and returns nil.
func (h *initHandler) startInit(ctx context.Context, mode string) error {
	h.mu.Lock()
	if h.status != server.InitStatusNew {
		h.mu.Unlock()
		return nil
	}
	h.status = server.InitStatusProgress
	h.mu.Unlock()

	go func() {
		err := h.initialize(ctx, mode)
		h.mu.Lock()
		if err != nil {
			h.logger.Error("initialization failed", zap.Error(err))
			h.status = server.InitStatusFailed
		} else {
			h.logger.Info("initialization completed successfully")
			h.status = server.InitStatusSuccessful
		}
		h.mu.Unlock()
	}()

	return nil
}

// initialize performs the actual initialization work:
//  1. Validate the data directory (sanity or full check based on mode).
//  2. If the data directory is empty or corrupt and a restorer is available,
//     attempt to restore from the latest full snapshot.
//  3. If no restorer or no snapshots, etcd will start fresh.
func (h *initHandler) initialize(_ context.Context, mode string) error {
	h.logger.Info("starting initialization", zap.String("mode", mode))

	// Check if data dir is empty first.
	dataDirEmpty := isDataDirEmpty(h.cfg.DataDir)
	if dataDirEmpty {
		h.logger.Info("data directory is empty, skipping validation")
	} else {
		// Validate data directory.
		if mode == "Full" {
			isSingleNode := !isMultiNode(h.cfg)
			if err := h.validator.FullCheck(isSingleNode); err != nil {
				h.logger.Warn("full validation failed, data may be corrupt", zap.Error(err))
			}
		} else {
			if err := h.validator.SanityCheck(); err != nil {
				h.logger.Warn("sanity check failed, data may be corrupt", zap.Error(err))
			}
		}
	}

	// Determine if we need to restore.
	corrupt := false
	if !dataDirEmpty {
		corrupt = h.validator.IsCorrupt()
	}

	if (dataDirEmpty || corrupt) && h.restorer != nil {
		h.logger.Info("attempting restore from backup store",
			zap.Bool("dataDirEmpty", dataDirEmpty),
			zap.Bool("corrupt", corrupt),
		)

		// If corrupt, clean the data dir before restoring.
		if corrupt {
			h.logger.Info("cleaning corrupt data directory")
			if err := os.RemoveAll(h.cfg.DataDir); err != nil {
				return fmt.Errorf("failed to remove corrupt data dir: %w", err)
			}
			if err := os.MkdirAll(h.cfg.DataDir, 0755); err != nil {
				return fmt.Errorf("failed to re-create data dir: %w", err)
			}
		}

		ctx := context.Background()
		fullSnap, err := h.restorer.FindLatestFullSnapshot(ctx)
		if err != nil {
			h.logger.Warn("no full snapshot found, etcd will start fresh", zap.Error(err))
		} else {
			h.logger.Info("found full snapshot, restoring",
				zap.String("name", fullSnap.Name),
				zap.Int64("endRevision", fullSnap.EndRevision),
			)
			if err := h.restorer.RestoreFull(ctx); err != nil {
				return fmt.Errorf("full snapshot restore failed: %w", err)
			}

			// Find delta snapshots for later application (after etcd starts).
			deltas, dErr := h.restorer.FindDeltaSnapshots(ctx, fullSnap.EndRevision)
			if dErr != nil {
				h.logger.Warn("failed to find delta snapshots", zap.Error(dErr))
			} else if len(deltas) > 0 {
				h.logger.Info("delta snapshots found for post-start application",
					zap.Int("count", len(deltas)),
				)
				// Delta application requires a running etcd client. The main
				// daemon loop will handle this after etcd is ready.
			}
		}
	} else if dataDirEmpty || corrupt {
		h.logger.Info("no backup store configured, etcd will start fresh")
	} else {
		h.logger.Info("data directory is valid, etcd will use existing data")
	}

	return nil
}

// configHandler serves the etcd configuration YAML from the druid-mounted
// ConfigMap or generates a fallback from steward config flags.
type configHandler struct {
	cfg    *config.Config
	logger *zap.Logger
}

func (h *configHandler) getConfig() ([]byte, error) {
	// Try reading the mounted ConfigMap first.
	data, err := os.ReadFile(etcdConfigPath)
	if err == nil {
		h.logger.Info("serving etcd config from mounted ConfigMap", zap.String("path", etcdConfigPath))
		return data, nil
	}

	// Fallback: generate from steward config flags.
	h.logger.Info("mounted ConfigMap not found, generating etcd config from flags")
	return h.generateEtcdConfig(), nil
}

func (h *configHandler) generateEtcdConfig() []byte {
	cfg := h.cfg

	name := cfg.PodName
	if name == "" {
		name = "default"
	}

	listenClientURLs := cfg.ListenClientURLs
	if listenClientURLs == "" {
		listenClientURLs = "http://0.0.0.0:2379"
	}

	advertiseClientURLs := cfg.AdvertiseClientURLs
	if advertiseClientURLs == "" {
		advertiseClientURLs = "http://0.0.0.0:2379"
	}

	listenPeerURLs := cfg.ListenPeerURLs
	if listenPeerURLs == "" {
		listenPeerURLs = "http://0.0.0.0:2380"
	}

	initialAdvertisePeerURLs := cfg.InitialAdvertisePeerURLs
	if initialAdvertisePeerURLs == "" {
		initialAdvertisePeerURLs = "http://0.0.0.0:2380"
	}

	initialCluster := cfg.InitialCluster
	if initialCluster == "" {
		initialCluster = fmt.Sprintf("%s=%s", name, initialAdvertisePeerURLs)
	}

	initialClusterState := cfg.InitialClusterState
	if initialClusterState == "" {
		initialClusterState = "new"
	}

	initialClusterToken := cfg.InitialClusterToken
	if initialClusterToken == "" {
		initialClusterToken = "etcd-cluster"
	}

	autoCompactionMode := cfg.AutoCompactionMode
	if autoCompactionMode == "" {
		autoCompactionMode = "periodic"
	}

	autoCompactionRetention := cfg.AutoCompactionRetention
	if autoCompactionRetention == "" {
		autoCompactionRetention = "30m"
	}

	quotaBytes := cfg.EmbeddedEtcdQuotaBytes
	if quotaBytes == 0 {
		quotaBytes = 8 * 1024 * 1024 * 1024
	}

	return []byte(fmt.Sprintf(`name: %s
data-dir: %s
listen-client-urls: %s
advertise-client-urls: %s
listen-peer-urls: %s
initial-advertise-peer-urls: %s
initial-cluster: %s
initial-cluster-state: %s
initial-cluster-token: %s
auto-compaction-mode: %s
auto-compaction-retention: %s
quota-backend-bytes: %d
`,
		name,
		cfg.DataDir,
		listenClientURLs,
		advertiseClientURLs,
		listenPeerURLs,
		initialAdvertisePeerURLs,
		initialCluster,
		initialClusterState,
		initialClusterToken,
		autoCompactionMode,
		autoCompactionRetention,
		quotaBytes,
	))
}

// snapshotEndpointHandler handles /snapshot/* endpoints by delegating to the
// snapshotter and snapstore.
type snapshotEndpointHandler struct {
	snapshotter *snapshotter.Snapshotter
	store       snapstore.SnapStore
	logger      *zap.Logger
}

func (h *snapshotEndpointHandler) handleFull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.snapshotter == nil {
		http.Error(w, "snapshotter not available", http.StatusServiceUnavailable)
		return
	}
	if err := h.snapshotter.TakeFullSnapshot(r.Context()); err != nil {
		h.logger.Error("on-demand full snapshot failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "full snapshot triggered")
}

func (h *snapshotEndpointHandler) handleDelta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.snapshotter == nil {
		http.Error(w, "snapshotter not available", http.StatusServiceUnavailable)
		return
	}
	if err := h.snapshotter.TakeDeltaSnapshot(r.Context()); err != nil {
		h.logger.Error("on-demand delta snapshot failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "delta snapshot triggered")
}

func (h *snapshotEndpointHandler) handleLatest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.store == nil {
		http.Error(w, "snapstore not available", http.StatusServiceUnavailable)
		return
	}
	snaps, err := h.store.List(r.Context())
	if err != nil {
		h.logger.Error("failed to list snapshots", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(snaps) == 0 {
		http.Error(w, "no snapshots found", http.StatusNotFound)
		return
	}

	// Return the latest snapshot (list is sorted by CreatedAt ascending).
	latest := snaps[len(snaps)-1]
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(latest)
}

// defragMaintenanceAdapter adapts etcdclient.Client to defrag.MaintenanceClient.
type defragMaintenanceAdapter struct {
	client *etcdclient.Client
}

func (a *defragMaintenanceAdapter) Defragment(ctx context.Context, endpoint string) error {
	_, err := a.client.Defragment(ctx, endpoint)
	return err
}

func (a *defragMaintenanceAdapter) Status(ctx context.Context, endpoint string) (int64, error) {
	resp, err := a.client.Status(ctx, endpoint)
	if err != nil {
		return 0, err
	}
	return resp.DbSize, nil
}

// defragKVAdapter adapts etcdclient.Client to defrag.KVClient.
type defragKVAdapter struct {
	client *etcdclient.Client
}

func (a *defragKVAdapter) Put(ctx context.Context, key, val string) error {
	_, err := a.client.Put(ctx, key, val)
	return err
}

func (a *defragKVAdapter) Get(ctx context.Context, key string) (string, error) {
	resp, err := a.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (a *defragKVAdapter) Delete(ctx context.Context, key string) error {
	_, err := a.client.Delete(ctx, key)
	return err
}

// defragClusterAdapter adapts etcdclient.Client to defrag.ClusterClient.
type defragClusterAdapter struct {
	client *etcdclient.Client
}

func (a *defragClusterAdapter) MemberEndpoints(ctx context.Context) ([]string, error) {
	resp, err := a.client.MemberList(ctx)
	if err != nil {
		return nil, err
	}
	var endpoints []string
	for _, m := range resp.Members {
		endpoints = append(endpoints, m.ClientURLs...)
	}
	return endpoints, nil
}

// memberStatusAdapter adapts etcdclient.Client to member.StatusClient.
type memberStatusAdapter struct {
	client *etcdclient.Client
}

func (a *memberStatusAdapter) Status(ctx context.Context, endpoint string) (member.StatusResponse, error) {
	resp, err := a.client.Status(ctx, endpoint)
	if err != nil {
		return member.StatusResponse{}, err
	}
	return member.StatusResponse{
		MemberID:    resp.Header.MemberId,
		Leader:      resp.Leader,
		DBSize:      resp.DbSize,
		DBSizeInUse: resp.DbSizeInUse,
		IsLearner:   resp.IsLearner,
	}, nil
}

func newRootCommand() *cobra.Command {
	cfg := config.DefaultConfig()

	root := &cobra.Command{
		Use:   "etcd-steward",
		Short: "etcd-steward manages etcd operational tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDaemon(cmd, cfg)
		},
	}

	config.BindFlags(cfg, root.PersistentFlags())
	// Ensure pflags are also visible via root.Flags() for direct sub-command use.
	root.Flags().AddFlagSet(root.PersistentFlags())

	root.AddCommand(newVersionCommand())
	root.AddCommand(compact.NewCommand())
	root.AddCommand(copybackups.NewCommand())

	return root
}

func runDaemon(cmd *cobra.Command, cfg *config.Config) error {
	// ----------------------------------------------------------------
	// Phase 1: Parse config and create logger
	// ----------------------------------------------------------------
	if cfg.ConfigFile != "" {
		if err := config.LoadFromFile(cfg, cfg.ConfigFile, cmd.Flags()); err != nil {
			return fmt.Errorf("loading config file: %w", err)
		}
	}

	if vErr := config.Validate(cfg); vErr != nil {
		return fmt.Errorf("config validation failed: %w", vErr)
	}

	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	logger.Info("etcd-steward daemon starting",
		zap.String("version", Version),
		zap.String("pod", cfg.PodName),
		zap.String("namespace", cfg.PodNamespace),
		zap.Strings("endpoints", cfg.EtcdEndpoints),
	)

	// ----------------------------------------------------------------
	// Phase 2: Create snapstore (L1: nil interface gotcha)
	// ----------------------------------------------------------------
	var store snapstore.SnapStore
	localStore, snapErr := snapstore.NewSnapStore(cfg.StoreProvider, cfg.StorePrefix, cfg.StoreContainer, nil)
	if snapErr != nil {
		logger.Warn("failed to create snapstore, snapshot operations will be disabled", zap.Error(snapErr))
	} else {
		store = localStore
	}

	// ----------------------------------------------------------------
	// Phase 3: Create validator and restorer
	// ----------------------------------------------------------------
	val := validator.New(cfg.DataDir, logger.Named("validator"))

	var restorerInst *restorer.Restorer
	if store != nil {
		restorerInst = restorer.New(
			store,
			compression.AlgorithmGzip,
			cfg.DataDir,
			cfg.RestorationTempDir,
			logger.Named("restorer"),
		)
	}

	// ----------------------------------------------------------------
	// Phase 4: Set up signal handling
	// ----------------------------------------------------------------
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, initiating graceful shutdown", zap.String("signal", sig.String()))
		cancel()
	}()

	// ----------------------------------------------------------------
	// Phase 5: Create and start HTTP server FIRST (wrapper polls immediately)
	// ----------------------------------------------------------------
	srv := server.New(cfg.ServerPort, logger.Named("server"))

	// Initialization handler (async, stateful).
	initH := &initHandler{
		status:    server.InitStatusNew,
		validator: val,
		restorer:  restorerInst,
		cfg:       cfg,
		logger:    logger.Named("init"),
	}

	// Config handler.
	cfgH := &configHandler{
		cfg:    cfg,
		logger: logger.Named("config"),
	}

	srv.RegisterInitializationEndpoints(
		initH.getStatus,
		initH.startInit,
		cfgH.getConfig,
	)

	// Snapshot endpoints (initially with nil snapshotter; will be set after
	// etcd is ready and snapshotter is created).
	snapH := &snapshotEndpointHandler{
		store:  store,
		logger: logger.Named("snapshot-endpoint"),
	}
	srv.RegisterHandler("/snapshot/full", http.HandlerFunc(snapH.handleFull))
	srv.RegisterHandler("/snapshot/delta", http.HandlerFunc(snapH.handleDelta))
	srv.RegisterHandler("/snapshot/latest", http.HandlerFunc(snapH.handleLatest))

	// Start HTTP server in background.
	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("HTTP server failed", zap.Error(err))
			cancel()
		}
	}()

	logger.Info("HTTP server started, waiting for wrapper to trigger initialization")

	// ----------------------------------------------------------------
	// Phase 6: Wait for initialization to complete
	// ----------------------------------------------------------------
	if err := waitForInitialization(ctx, initH, logger); err != nil {
		return fmt.Errorf("waiting for initialization: %w", err)
	}

	// ----------------------------------------------------------------
	// Phase 7: Wait for etcd to become ready
	// ----------------------------------------------------------------
	logger.Info("initialization complete, waiting for etcd to become ready",
		zap.Strings("endpoints", cfg.EtcdEndpoints),
	)

	if err := waitForEtcdReady(ctx, cfg.EtcdEndpoints, cfg.EtcdConnectionTimeout, etcdReadyTimeout, logger); err != nil {
		return fmt.Errorf("waiting for etcd to become ready: %w", err)
	}

	logger.Info("etcd is ready, creating client and starting components")

	// ----------------------------------------------------------------
	// Phase 8: Create etcd client
	// ----------------------------------------------------------------
	tlsConfig, tlsErr := buildTLSConfig(cfg)
	if tlsErr != nil {
		logger.Warn("TLS config not available, connecting without TLS", zap.Error(tlsErr))
	}

	etcdClientCfg := clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: cfg.EtcdConnectionTimeout,
	}
	if tlsConfig != nil {
		etcdClientCfg.TLS = tlsConfig
	}

	rawClient, err := clientv3.New(etcdClientCfg)
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer func() { _ = rawClient.Close() }()

	etcdClient := etcdclient.NewClient(rawClient)

	// ----------------------------------------------------------------
	// Phase 9: Create K8s clients (best-effort, not fatal if out-of-cluster)
	// ----------------------------------------------------------------
	var k8sClient kubernetes.Interface
	var dynClient dynamic.Interface

	k8sCfg, k8sErr := k8srest.InClusterConfig()
	if k8sErr != nil {
		logger.Warn("not running in cluster, K8s-dependent features disabled", zap.Error(k8sErr))
	} else {
		k8sClient, err = kubernetes.NewForConfig(k8sCfg)
		if err != nil {
			logger.Warn("failed to create Kubernetes clientset", zap.Error(err))
		}
		dynClient, err = dynamic.NewForConfig(k8sCfg)
		if err != nil {
			logger.Warn("failed to create Kubernetes dynamic client", zap.Error(err))
		}
	}

	// ----------------------------------------------------------------
	// Phase 10: Start all gated components
	// ----------------------------------------------------------------
	var wg sync.WaitGroup

	// 10a: Leader watcher (always runs after etcd is ready).
	leaderWatcher := leaderwatch.New(
		cfg.PodName,
		cfg.LeaderElectionReelectionPeriod,
		rawClient,
		logger.Named("leaderwatch"),
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		leaderWatcher.Run(ctx)
	}()
	logger.Info("leader watcher started")

	// 10b: Snapshotter (requires: store, leader).
	if cfg.EnableSnapshotter && store != nil {
		snapLock := etcdclient.NewLock(rawClient, "/steward/snapshot-lock", 30)
		snap := snapshotter.New(
			etcdClient,
			etcdClient,
			rawClient,
			store,
			snapLock,
			compression.AlgorithmGzip,
			cfg.FullSnapshotInterval,
			cfg.DeltaSnapshotInterval,
			logger.Named("snapshotter"),
		)
		// Set the snapshotter on the endpoint handler so on-demand triggers work.
		snapH.snapshotter = snap

		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("snapshotter waiting for leadership before starting")
			snap.Run(ctx)
		}()
		logger.Info("snapshotter started (will acquire lock)")
	} else {
		logger.Info("snapshotter disabled",
			zap.Bool("enabled", cfg.EnableSnapshotter),
			zap.Bool("storeAvailable", store != nil),
		)
	}

	// 10c: Garbage collector (requires: store).
	if cfg.EnableGC && store != nil {
		collector := gc.New(
			store,
			cfg.MaxFullSnapshots,
			cfg.GCPeriod,
			logger.Named("gc"),
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.Run(ctx)
		}()
		logger.Info("garbage collector started")
	} else {
		logger.Info("garbage collector disabled",
			zap.Bool("enabled", cfg.EnableGC),
			zap.Bool("storeAvailable", store != nil),
		)
	}

	// 10d: Defragmenter (requires: leader role).
	if cfg.EnableDefrag {
		defragLock := etcdclient.NewLock(rawClient, "/steward/defrag-lock", 30)
		defragmenter := defrag.New(
			cfg.PodName,
			cfg.DefragInterval,
			defragLock,
			&leaderRoleProvider{watcher: leaderWatcher},
			&defragMaintenanceAdapter{client: etcdClient},
			&defragKVAdapter{client: etcdClient},
			&defragClusterAdapter{client: etcdClient},
			logger.Named("defrag"),
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defragmenter.Run(ctx)
		}()
		logger.Info("defragmenter started")
	} else {
		logger.Info("defragmenter disabled")
	}

	// 10e: Alarm handler.
	if cfg.EnableAlarmHandler {
		alarmHandler := alarm.New(
			cfg.PodName,
			cfg.AlarmPollInterval,
			cfg.CompactRevisionLag,
			cfg.EtcdEndpoints,
			etcdClient,
			etcdClient,
			logger.Named("alarm"),
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			alarmHandler.Run(ctx)
		}()
		logger.Info("alarm handler started")
	} else {
		logger.Info("alarm handler disabled")
	}

	// 10f: Member lease renewer (requires: K8s client).
	if cfg.EnableMemberLeaseRenewal && k8sClient != nil {
		leaseName := cfg.PodName
		leaseRenewer := lease.NewRenewer(
			cfg.PodName,
			cfg.PodNamespace,
			leaseName,
			cfg.K8sHeartbeatDuration,
			k8sClient,
			logger.Named("member-lease"),
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			leaseRenewer.Run(ctx)
		}()
		logger.Info("member lease renewer started", zap.String("lease", leaseName))
	} else {
		logger.Info("member lease renewer disabled",
			zap.Bool("enabled", cfg.EnableMemberLeaseRenewal),
			zap.Bool("k8sAvailable", k8sClient != nil),
		)
	}

	// 10g: Snapshot lease renewers (requires: K8s client, snapshot lease names).
	if cfg.EnableSnapshotLeaseRenewal && k8sClient != nil {
		if cfg.FullSnapshotLeaseName != "" {
			fullLeaseRenewer := lease.NewRenewer(
				cfg.PodName,
				cfg.PodNamespace,
				cfg.FullSnapshotLeaseName,
				cfg.K8sHeartbeatDuration,
				k8sClient,
				logger.Named("full-snapshot-lease"),
			)
			wg.Add(1)
			go func() {
				defer wg.Done()
				fullLeaseRenewer.Run(ctx)
			}()
			logger.Info("full snapshot lease renewer started", zap.String("lease", cfg.FullSnapshotLeaseName))
		}
		if cfg.DeltaSnapshotLeaseName != "" {
			deltaLeaseRenewer := lease.NewRenewer(
				cfg.PodName,
				cfg.PodNamespace,
				cfg.DeltaSnapshotLeaseName,
				cfg.K8sHeartbeatDuration,
				k8sClient,
				logger.Named("delta-snapshot-lease"),
			)
			wg.Add(1)
			go func() {
				defer wg.Done()
				deltaLeaseRenewer.Run(ctx)
			}()
			logger.Info("delta snapshot lease renewer started", zap.String("lease", cfg.DeltaSnapshotLeaseName))
		}
	} else {
		logger.Info("snapshot lease renewers disabled",
			zap.Bool("enabled", cfg.EnableSnapshotLeaseRenewal),
			zap.Bool("k8sAvailable", k8sClient != nil),
		)
	}

	// 10h: EtcdMember updater (requires: K8s dynamic client).
	if dynClient != nil {
		sm := statemachine.New()
		smRecorder := statemachine.NewK8sRecorder(dynClient)

		// Create a member.StateRecorder that delegates to statemachine.K8sRecorder.
		updaterRecorder := &memberStateRecorderAdapter{
			smRecorder: smRecorder,
			sm:         sm,
		}

		updater := member.NewUpdater(
			cfg.PodName,
			cfg.PodNamespace,
			cfg.K8sHeartbeatDuration,
			updaterRecorder,
			logger.Named("member-updater"),
		)

		// Register the maintenance status provider.
		endpoint := cfg.EtcdEndpoints[0]
		statusProvider := member.NewMaintenanceStatusProvider(
			cfg.PodName,
			endpoint,
			&memberStatusAdapter{client: etcdClient},
		)
		updater.RegisterInfoProvider(statusProvider)

		// Register snapshot info as a supplementary provider.
		if store != nil {
			snapInfoProvider := member.NewSnapshotInfoProvider(store, logger.Named("snapshot-info"))
			updater.RegisterSupplementaryProvider(snapInfoProvider)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			updater.Run(ctx)
		}()
		logger.Info("EtcdMember updater started")
	} else {
		logger.Info("EtcdMember updater disabled (no K8s client)")
	}

	logger.Info("all components started, daemon is running")

	// ----------------------------------------------------------------
	// Phase 11: Block until context is cancelled (signal handler)
	// ----------------------------------------------------------------
	<-ctx.Done()
	logger.Info("context cancelled, waiting for components to stop")

	// Wait for all goroutines to complete graceful shutdown.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("all components stopped gracefully")
	case <-time.After(30 * time.Second):
		logger.Warn("graceful shutdown timed out after 30s")
	}

	return nil
}

// waitForInitialization polls the init handler until initialization reaches
// Successful status, or the context is cancelled.
func waitForInitialization(ctx context.Context, h *initHandler, logger *zap.Logger) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			h.mu.Lock()
			status := h.status
			h.mu.Unlock()

			switch status {
			case server.InitStatusSuccessful:
				logger.Info("initialization reached Successful state")
				return nil
			case server.InitStatusFailed:
				return fmt.Errorf("initialization failed")
			case server.InitStatusNew, server.InitStatusProgress:
				// Still waiting. Note: we do NOT call getStatus() here to avoid
				// the reset-on-read behavior. We peek at the raw status.
				continue
			}
		}
	}
}

// waitForEtcdReady polls etcd until a simple Get succeeds or timeout.
func waitForEtcdReady(ctx context.Context, endpoints []string, dialTimeout, totalTimeout time.Duration, logger *zap.Logger) error {
	deadline := time.After(totalTimeout)
	ticker := time.NewTicker(etcdReadyPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("etcd did not become ready within %s", totalTimeout)
		case <-ticker.C:
			err := probeEtcd(ctx, endpoints, dialTimeout)
			if err == nil {
				return nil
			}
			logger.Info("etcd not ready yet, retrying", zap.Error(err))
		}
	}
}

// probeEtcd attempts a lightweight Get to check if etcd is responsive.
func probeEtcd(ctx context.Context, endpoints []string, dialTimeout time.Duration) error {
	probeCfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	}
	c, err := clientv3.New(probeCfg)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err = c.Get(probeCtx, "health")
	return err
}

// buildTLSConfig creates a TLS config from the certificate paths in cfg.
// Returns nil if no TLS paths are configured.
func buildTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if cfg.CACert == "" && cfg.Cert == "" && cfg.Key == "" {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cfg.CACert != "" {
		caCert, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("reading CA cert %q: %w", cfg.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert %q", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	}

	if cfg.Cert != "" && cfg.Key != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("loading client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// isDataDirEmpty returns true if the data directory does not exist or contains
// no entries.
func isDataDirEmpty(dataDir string) bool {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return true
	}
	return len(entries) == 0
}

// isMultiNode returns true if the config suggests a multi-node cluster.
func isMultiNode(cfg *config.Config) bool {
	return len(cfg.EtcdEndpoints) > 1
}

// leaderRoleProvider adapts leaderwatch.Watcher to defrag.RoleProvider.
type leaderRoleProvider struct {
	watcher *leaderwatch.Watcher
}

func (p *leaderRoleProvider) IsLeader() bool {
	return p.watcher.GetCurrentRole() == leaderwatch.Leader
}

// memberStateRecorderAdapter adapts statemachine.K8sRecorder to member.StateRecorder.
type memberStateRecorderAdapter struct {
	smRecorder *statemachine.K8sRecorder
	sm         *statemachine.StateMachine
}

func (a *memberStateRecorderAdapter) RecordMemberState(ctx context.Context, memberName, namespace string, info member.MemberInfo) error {
	// Map member role to a statemachine action and trigger it.
	var action statemachine.Action
	switch info.Role {
	case "Leader":
		action = statemachine.ActionWonElection
	case "Follower":
		action = statemachine.ActionStartAsFollower
	default:
		// For other roles, just record without state machine transition.
		return nil
	}

	t, err := a.sm.Trigger(action)
	if err != nil {
		// Transition not valid from current state — this is expected for
		// repeated follower/leader states. Log and continue.
		return nil
	}

	return a.smRecorder.Record(ctx, memberName, namespace, t)
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version of etcd-steward",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(Version)
		},
	}
}

func main() {
	pflag.CommandLine.AddGoFlagSet(nil) // ensure pflag is initialized
	if err := newRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
