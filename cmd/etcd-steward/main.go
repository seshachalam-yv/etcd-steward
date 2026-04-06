// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package main is the entry point for etcd-steward.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
		}
	}

	// Load configuration.
	var cfg config.Config
	configPath := os.Getenv("ETCD_STEWARD_CONFIG")
	if configPath != "" {
		cfg, err = config.LoadFromFile(configPath)
		if err != nil {
			logger.Fatal("failed to load config", zap.Error(err))
		}
	} else {
		cfg = config.DefaultConfig()
		cfg.PodName = os.Getenv("POD_NAME")
		cfg.PodNamespace = os.Getenv("POD_NAMESPACE")
		cfg.EtcdEndpoint = os.Getenv("ETCD_ENDPOINT")
		cfg.EtcdPeerURL = os.Getenv("ETCD_PEER_URL")
		cfg.InitialCluster = os.Getenv("INITIAL_CLUSTER")
	}

	cfg.DeriveEtcdName()

	if errs := cfg.Validate(); len(errs) > 0 {
		for _, e := range errs {
			logger.Error("config validation error", zap.Error(e))
		}
		os.Exit(1)
	}

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
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.EtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
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
	isSingleNode := cfg.IsSingleNode

	// Build initializer.
	init := initializer.New(
		cfg.PodName, cfg.PodNamespace, cfg.EtcdPeerURL,
		cfg.DataDir, cfg.RestorationTempDir, cfg.InitialCluster,
		isSingleNode,
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
	configFn := func() ([]byte, error) {
		return []byte(fmt.Sprintf("name: %s\ninitial-cluster: %s\n", cfg.PodName, cfg.InitialCluster)), nil
	}

	var snapIface server.Snapshotter
	if snap != nil {
		snapIface = snap
	}

	srv := server.NewServer(
		cfg.ServerPort,
		func() initializer.InitializationStatus { return init.GetStatus() },
		func(srvCtx context.Context, mode string) error { return init.Start(srvCtx, mode) },
		configFn,
		snapIface,
		store,
		logger.Named("server"),
	)

	// Start all goroutines.
	var wg sync.WaitGroup

	// HTTP server.
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

	// Alarm handler.
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			alarmHandler.Run(ctx)
		}()
	}

	// Auto-start initialization with the determined mode.
	if err := init.Start(ctx, string(valMode)); err != nil {
		logger.Error("failed to start initialization", zap.Error(err))
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

func runCompact(logger *zap.Logger) {
	logger.Info("compact subcommand not yet fully implemented")
	os.Exit(0)
}

func runCopyBackups(logger *zap.Logger) {
	logger.Info("copy-backups subcommand not yet fully implemented")
	os.Exit(0)
}
