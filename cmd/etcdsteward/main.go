// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/gardener/etcd-steward/cmd/etcdsteward/compact"
	"github.com/gardener/etcd-steward/cmd/etcdsteward/copybackups"
	"github.com/gardener/etcd-steward/internal/config"
	"github.com/gardener/etcd-steward/internal/member"
	"github.com/gardener/etcd-steward/internal/server"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// Version is set via ldflags at build time.
var Version = "dev"

func newRootCommand() *cobra.Command {
	cfg := config.DefaultConfig()

	root := &cobra.Command{
		Use:   "etcd-steward",
		Short: "etcd-steward manages etcd operational tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.ConfigFile != "" {
				if err := config.LoadFromFile(cfg, cfg.ConfigFile, cmd.Flags()); err != nil {
					return fmt.Errorf("loading config file: %w", err)
				}
			}

			logger, _ := zap.NewProduction()
			defer func() { _ = logger.Sync() }()

			logger.Info("etcd-steward daemon starting",
				zap.String("pod", cfg.PodName),
				zap.String("namespace", cfg.PodNamespace),
			)

			// Create snapstore (L1: nil interface gotcha)
			var store snapstore.SnapStore
			localStore, snapErr := snapstore.NewSnapStore(cfg.StoreProvider, cfg.StorePrefix, cfg.StoreContainer, nil)
			if snapErr != nil {
				logger.Warn("failed to create snapstore", zap.Error(snapErr))
			} else {
				store = localStore
			}

			// Create HTTP server with wrapper compat endpoints (L2)
			srv := server.New(cfg.ServerPort, logger.Named("server"))

			var initDone bool
			srv.RegisterInitializationEndpoints(
				func() server.InitStatus {
					if initDone {
						return server.InitStatusSuccessful
					}
					return server.InitStatusNew
				},
				func(ctx context.Context, mode string) error {
					logger.Info("initialization triggered", zap.String("mode", mode))
					return nil
				},
				func() ([]byte, error) {
					// Return a minimal etcd config YAML that the wrapper can parse.
					// The wrapper writes this to a file and passes it to embed.ConfigFromFile.
					etcdCfg := fmt.Sprintf(`name: %s
data-dir: /var/etcd/data/new.etcd
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls: http://0.0.0.0:2379
listen-peer-urls: http://0.0.0.0:2380
initial-advertise-peer-urls: http://0.0.0.0:2380
initial-cluster: %s=http://0.0.0.0:2380
initial-cluster-state: new
initial-cluster-token: etcd-cluster
auto-compaction-mode: periodic
auto-compaction-retention: 30m
quota-backend-bytes: 8589934592
`, cfg.PodName, cfg.PodName)
					return []byte(etcdCfg), nil
				},
			)

			// Start HTTP server in background
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			go func() {
				if err := srv.Run(ctx); err != nil {
					logger.Error("HTTP server failed", zap.Error(err))
				}
			}()

			// Mark init as done (steward bootstraps instantly for now)
			initDone = true
			logger.Info("bootstrap initialization succeeded")

			// Wire snapshot info provider
			if store != nil {
				snapProvider := member.NewSnapshotInfoProvider(store, logger)
				_ = snapProvider
			}

			// Block until context is cancelled (signal handler)
			<-ctx.Done()
			return nil
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
