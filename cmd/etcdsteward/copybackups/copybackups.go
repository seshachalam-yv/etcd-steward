// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package copybackups

import (
	"context"
	"fmt"

	"github.com/gardener/etcd-steward/internal/snapstore"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewCommand creates a new cobra command for the copy-backups subcommand.
// It copies snapshots between storage backends, supporting cross-provider copy.
func NewCommand() *cobra.Command {
	var (
		sourcePrefix    string
		sourceContainer string
		destPrefix      string
		destContainer   string
		sourceProvider  string
		destProvider    string
	)

	cmd := &cobra.Command{
		Use:   "copy-backups",
		Short: "Copy etcd backups between storage backends",
		Long: `Copy etcd backup snapshots from a source storage backend to a destination
storage backend. Supports cross-provider copy (e.g. Local to S3, GCS to ABS).
Snapshots that already exist in the destination (by name) are skipped.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return fmt.Errorf("creating logger: %w", err)
			}
			defer func() { _ = logger.Sync() }()

			srcStore, err := snapstore.NewSnapStore(sourceProvider, sourcePrefix, sourceContainer, nil)
			if err != nil {
				return fmt.Errorf("creating source snapstore: %w", err)
			}

			dstStore, err := snapstore.NewSnapStore(destProvider, destPrefix, destContainer, nil)
			if err != nil {
				return fmt.Errorf("creating destination snapstore: %w", err)
			}

			ctx := cmd.Context()
			copied, skipped, err := CopySnapshots(ctx, srcStore, dstStore, logger)
			if err != nil {
				return fmt.Errorf("copying backups: %w", err)
			}

			logger.Info("copy-backups completed",
				zap.Int("copied", copied),
				zap.Int("skipped", skipped),
			)
			return nil
		},
	}

	cmd.Flags().StringVar(&sourcePrefix, "source-prefix", "", "source snapshot store prefix")
	cmd.Flags().StringVar(&sourceContainer, "source-container", "", "source snapshot store container path")
	cmd.Flags().StringVar(&destPrefix, "dest-prefix", "", "destination snapshot store prefix")
	cmd.Flags().StringVar(&destContainer, "dest-container", "", "destination snapshot store container path")
	cmd.Flags().StringVar(&sourceProvider, "source-provider", "Local", "source storage provider")
	cmd.Flags().StringVar(&destProvider, "dest-provider", "Local", "destination storage provider")

	return cmd
}

// CopySnapshots copies all snapshots from the source store to the destination
// store. Snapshots that already exist in the destination (by name) are skipped.
// Returns the count of copied and skipped snapshots.
func CopySnapshots(ctx context.Context, src, dst snapstore.SnapStore, logger *zap.Logger) (copied, skipped int, err error) {
	srcSnaps, err := src.List(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("listing source snapshots: %w", err)
	}

	// Build a set of existing snapshot names in the destination.
	dstSnaps, err := dst.List(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("listing destination snapshots: %w", err)
	}

	existingNames := make(map[string]struct{}, len(dstSnaps))
	for _, s := range dstSnaps {
		existingNames[s.Name] = struct{}{}
	}

	for _, snap := range srcSnaps {
		if _, exists := existingNames[snap.Name]; exists {
			logger.Info("skipping existing snapshot", zap.String("name", snap.Name))
			skipped++
			continue
		}

		rc, err := src.Download(ctx, snap.Name)
		if err != nil {
			return copied, skipped, fmt.Errorf("downloading snapshot %q from source: %w", snap.Name, err)
		}

		_, err = dst.Upload(ctx, snap, rc)
		_ = rc.Close()
		if err != nil {
			return copied, skipped, fmt.Errorf("uploading snapshot %q to destination: %w", snap.Name, err)
		}

		logger.Info("copied snapshot",
			zap.String("name", snap.Name),
			zap.String("kind", string(snap.Kind)),
			zap.Int64("endRevision", snap.EndRevision),
		)
		copied++
	}

	return copied, skipped, nil
}
