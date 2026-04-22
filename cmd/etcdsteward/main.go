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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gardener/etcd-steward/cmd/etcdsteward/compact"
	"github.com/gardener/etcd-steward/cmd/etcdsteward/copybackups"
	"github.com/gardener/etcd-steward/internal/alarm"
	"github.com/gardener/etcd-steward/internal/bootstrapper"
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

// configHandler builds etcd configuration YAML from the druid-mounted
// ConfigMap or generates a fallback from steward config flags.
type configHandler struct {
	cfg    *config.Config
	logger *zap.Logger
}

func (h *configHandler) getConfig() ([]byte, error) {
	// Always generate config from flags. The druid-mounted ConfigMap uses
	// a druid-specific format (per-member URL maps) that the wrapper's
	// embed.ConfigFromFile cannot parse directly. The generated config
	// uses flat etcd-native YAML that the wrapper understands.
	h.logger.Info("generating etcd config from flags")
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
	// Map member role to a statemachine reason and trigger it.
	var reason statemachine.Reason
	switch info.Role {
	case "Leader":
		reason = statemachine.ReasonGainedClusterLeadership
	case "Follower":
		reason = statemachine.ReasonLostClusterLeadership
	default:
		// For other roles, just record without state machine transition.
		return nil
	}

	t, err := a.sm.Trigger(reason, "")
	if err != nil {
		// Transition not valid from current state -- this is expected for
		// repeated follower/leader states. Log and continue.
		return nil
	}

	return a.smRecorder.Record(ctx, memberName, namespace, t)
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

// runDaemon implements the Notes Option-1 architecture:
// STEWARD ORCHESTRATES, WRAPPER EXECUTES.
//
// The steward drives the entire flow — it does NOT expose /initialization/*
// endpoints for the wrapper to poll. Instead:
//
//  1. Start HTTP server (/metrics, /healthz, /snapshot/*)
//  2. Validate data directory
//  3. If corrupt: removeMember (multi-node) -> deleteDataDir
//  4. If empty and backup store configured: download full snapshot -> restore via etcdutl
//  5. Build etcd config YAML (from mounted ConfigMap or generate from flags)
//  6. POST /embedded-etcd to wrapper with the config
//  7. Wait for etcd to become reachable (poll etcd Get every 2s)
//  8. If restoration happened: apply delta snapshots via KV client
//  9. POST /readyz/set "ready" to wrapper -> K8s readiness probe now passes
//  10. Start runtime components (snapshotter, GC, defrag, alarm, lease, member updater)
//  11. Block until SIGTERM
func runDaemon(cmd *cobra.Command, cfg *config.Config) error {
	// ----------------------------------------------------------------
	// Parse config and create logger
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

	logger.Info("etcd-steward daemon starting (option-1: steward orchestrates)",
		zap.String("version", Version),
		zap.String("pod", cfg.PodName),
		zap.String("namespace", cfg.PodNamespace),
		zap.Strings("endpoints", cfg.EtcdEndpoints),
		zap.String("wrapperURL", cfg.WrapperURL),
	)

	// ----------------------------------------------------------------
	// Create snapstore (L1: nil interface gotcha)
	// ----------------------------------------------------------------
	var store snapstore.SnapStore
	localStore, snapErr := snapstore.NewSnapStore(cfg.StoreProvider, cfg.StorePrefix, cfg.StoreContainer, nil)
	if snapErr != nil {
		logger.Warn("failed to create snapstore, snapshot operations will be disabled", zap.Error(snapErr))
	} else {
		store = localStore
	}

	// ----------------------------------------------------------------
	// Create validator and restorer
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
	// Set up signal handling
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
	// Step 1: Start HTTP server (/metrics, /healthz, /snapshot/*, AND
	// backward-compatible /initialization/*, /config endpoints for old wrapper)
	// DUAL-MODE: supports both old wrapper (polls /initialization/status)
	// and new wrapper (receives POST /embedded-etcd).
	// ----------------------------------------------------------------
	srv := server.New(cfg.ServerPort, logger.Named("server"))

	// Backward-compatible initialization endpoints for old wrapper (v0.6.2).
	// The old wrapper polls GET /initialization/status, triggers GET /initialization/start,
	// then gets GET /config to start etcd.
	var initStatus atomic.Value
	initStatus.Store("New")

	srv.RegisterHandler("/initialization/status", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := initStatus.Load().(string)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, status)
	}))

	srv.RegisterHandler("/initialization/start", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The actual initialization runs synchronously in the main flow below.
		// This endpoint just signals that the wrapper has asked for init.
		// The status transitions happen in the main flow.
		logger.Info("initialization/start called by wrapper",
			zap.String("mode", r.URL.Query().Get("mode")))
		w.WriteHeader(http.StatusOK)
	}))

	cfgH := &configHandler{
		cfg:    cfg,
		logger: logger.Named("config"),
	}
	srv.RegisterHandler("/config", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := cfgH.getConfig()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-yaml")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))

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

	logger.Info("HTTP server started")

	// Mark init as in progress for the old wrapper.
	initStatus.Store("Progress")

	// ----------------------------------------------------------------
	// Step 2: Validate data directory
	// ----------------------------------------------------------------
	needsRestore := false
	dataDirEmpty := isDataDirEmpty(cfg.DataDir)

	if dataDirEmpty {
		logger.Info("data directory is empty, skipping validation")
	} else {
		// Perform a full validation check.
		isSingleNode := !isMultiNode(cfg)
		if err := val.FullCheck(isSingleNode); err != nil {
			logger.Warn("full validation failed, data may be corrupt", zap.Error(err))
		}
	}

	// ----------------------------------------------------------------
	// Step 3: If data corrupt -> removeMember (multi-node) -> deleteDataDir
	// ----------------------------------------------------------------
	corrupt := false
	if !dataDirEmpty {
		corrupt = val.IsCorrupt()
	}

	if corrupt {
		logger.Warn("data directory is corrupt, cleaning up")
		// TODO: For multi-node: remove self from cluster via etcd client
		// before deleting data directory. This requires a cluster client
		// which is not available yet (etcd is not running).
		if err := os.RemoveAll(cfg.DataDir); err != nil {
			return fmt.Errorf("failed to remove corrupt data directory: %w", err)
		}
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			return fmt.Errorf("failed to re-create data directory: %w", err)
		}
		logger.Info("deleted corrupt data directory")
		dataDirEmpty = true
	}

	// ----------------------------------------------------------------
	// Step 4: If empty and backup store configured -> restore from backup
	// ----------------------------------------------------------------
	if dataDirEmpty && restorerInst != nil {
		logger.Info("data directory is empty, attempting restore from backup store")
		fullSnap, err := restorerInst.FindLatestFullSnapshot(ctx)
		if err != nil {
			logger.Warn("no full snapshot found, etcd will start fresh", zap.Error(err))
		} else {
			logger.Info("found full snapshot, restoring",
				zap.String("name", fullSnap.Name),
				zap.Int64("endRevision", fullSnap.EndRevision),
			)
			if err := restorerInst.RestoreFull(ctx); err != nil {
				logger.Error("restore failed, etcd will start fresh", zap.Error(err))
			} else {
				needsRestore = true // deltas need to be applied after etcd starts
				logger.Info("full snapshot restored successfully")
			}
		}
	} else if dataDirEmpty {
		logger.Info("no backup store configured, etcd will start fresh")
	} else {
		logger.Info("data directory is valid, etcd will use existing data")
	}

	// ----------------------------------------------------------------
	// Step 5: Build etcd config YAML
	// Mark initialization as Successful for old wrapper.
	// ----------------------------------------------------------------
	initStatus.Store("Successful")
	logger.Info("initialization complete, status set to Successful")

	// configHandler already created above for /config endpoint.
	etcdConfigYAML, err := cfgH.getConfig()
	if err != nil {
		return fmt.Errorf("failed to build etcd config: %w", err)
	}
	logger.Info("etcd config built", zap.Int("configBytes", len(etcdConfigYAML)))

	// ----------------------------------------------------------------
	// Step 6: POST /embedded-etcd to wrapper with the config
	// ----------------------------------------------------------------
	wrapperClient := bootstrapper.NewHTTPWrapperClient(cfg.WrapperURL)

	logger.Info("sending etcd config to wrapper", zap.String("wrapperURL", cfg.WrapperURL))
	if err := wrapperClient.StartEmbeddedEtcd(ctx, etcdConfigYAML); err != nil {
		logger.Error("failed to start embedded etcd via wrapper", zap.Error(err))
		// This is not necessarily fatal: the wrapper may use the old init
		// flow or may already have started etcd. Continue and let the
		// readiness poll determine whether etcd comes up.
	}

	// ----------------------------------------------------------------
	// Step 7: Wait for etcd to become reachable (poll Get every 2s)
	// ----------------------------------------------------------------
	logger.Info("waiting for etcd to become reachable",
		zap.Strings("endpoints", cfg.EtcdEndpoints),
	)

	if err := waitForEtcdReady(ctx, cfg.EtcdEndpoints, cfg.EtcdConnectionTimeout, etcdReadyTimeout, logger); err != nil {
		return fmt.Errorf("waiting for etcd to become ready: %w", err)
	}

	logger.Info("etcd is reachable, creating client")

	// ----------------------------------------------------------------
	// Create etcd client for runtime use
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
	// Step 8: If restoration happened, apply delta snapshots via KV client
	// ----------------------------------------------------------------
	if needsRestore && restorerInst != nil {
		logger.Info("applying delta snapshots after restore")
		fullSnap, findErr := restorerInst.FindLatestFullSnapshot(ctx)
		if findErr != nil {
			logger.Warn("could not re-find full snapshot for delta application", zap.Error(findErr))
		} else {
			deltas, dErr := restorerInst.FindDeltaSnapshots(ctx, fullSnap.EndRevision)
			if dErr != nil {
				logger.Warn("failed to find delta snapshots", zap.Error(dErr))
			} else if len(deltas) > 0 {
				logger.Info("applying delta snapshots",
					zap.Int("count", len(deltas)),
					zap.Int64("afterRevision", fullSnap.EndRevision),
				)
				if err := restorerInst.ApplyDeltas(ctx, etcdClient, fullSnap.EndRevision, deltas); err != nil {
					logger.Error("failed to apply delta snapshots", zap.Error(err))
				} else {
					logger.Info("delta snapshots applied successfully", zap.Int("count", len(deltas)))
				}
			} else {
				logger.Info("no delta snapshots to apply")
			}
		}
	}

	// ----------------------------------------------------------------
	// Step 9: POST /readyz/set "ready" to wrapper -> K8s readiness probe passes
	// ----------------------------------------------------------------
	if err := wrapperClient.SetReady(ctx); err != nil {
		logger.Warn("failed to set wrapper ready (wrapper may not support /readyz/set)", zap.Error(err))
	} else {
		logger.Info("wrapper readiness set, pod is now ready")
	}

	// ----------------------------------------------------------------
	// Create K8s clients (best-effort, not fatal if out-of-cluster)
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
	// Step 10: Start all runtime components
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
		sm := statemachine.New(!isMultiNode(cfg))
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
	// Step 11: Block until context is cancelled (signal handler)
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
