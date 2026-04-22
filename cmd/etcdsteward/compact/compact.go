// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compact

import (
	"fmt"

	"github.com/gardener/etcd-steward/internal/compactor"
	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewCommand creates a new cobra command for the compact subcommand.
// It performs an offline compaction of etcd snapshots by restoring the latest
// full snapshot, applying deltas, defragmenting, and uploading a new compacted
// full snapshot.
func NewCommand() *cobra.Command {
	var (
		dataDir         string
		storeProvider   string
		storePrefix     string
		storeContainer  string
		compressionAlgo string
	)

	cmd := &cobra.Command{
		Use:   "compact",
		Short: "Compact the etcd database",
		Long: `Compact performs offline compaction of etcd snapshots.

It finds the latest full snapshot and all subsequent delta snapshots, restores
them into a temporary embedded etcd instance, defragments the data, and uploads
a new compacted full snapshot to the store.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return fmt.Errorf("creating logger: %w", err)
			}
			defer func() { _ = logger.Sync() }()

			store, err := snapstore.NewSnapStore(storeProvider, storePrefix, storeContainer, nil)
			if err != nil {
				return fmt.Errorf("creating snapstore: %w", err)
			}

			algo := compression.Algorithm(compressionAlgo)
			c := compactor.New(store, algo, dataDir, dataDir+"/tmp", logger)

			ctx := cmd.Context()
			if err := c.Compact(ctx); err != nil {
				return fmt.Errorf("compaction failed: %w", err)
			}

			logger.Info("compaction completed successfully")
			return nil
		},
	}

	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/etcd/data/new.etcd", "etcd data directory")
	cmd.Flags().StringVar(&storeProvider, "store-provider", "Local", "snapshot store provider (Local, S3, GCS, ABS)")
	cmd.Flags().StringVar(&storePrefix, "store-prefix", "", "snapshot store prefix")
	cmd.Flags().StringVar(&storeContainer, "store-container", "", "snapshot store container path")
	cmd.Flags().StringVar(&compressionAlgo, "compression-algo", "none", "compression algorithm (none, gzip, zstd)")

	return cmd
}
