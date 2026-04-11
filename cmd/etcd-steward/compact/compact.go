// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package compact provides the etcd-steward compact subcommand.
package compact

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// Options holds the parsed arguments for the compact subcommand.
type Options struct {
	StorageProvider       string
	StoreContainer        string
	StorePrefix           string
	StoreEndpointOverride string
}

// Run executes the compact subcommand.
// It finds the highest revision across all snapshots in the store and logs it.
// Advancing snapshot leases is no longer done here — etcd-druid reads
// EtcdMember.Status.Snapshots (populated by the snapshotter) to drive compaction.
func Run(logger *zap.Logger, opts Options) error {
	// Resolve the store base directory. For local provider the store container
	// may be a relative path (e.g. "default.bkp"); resolve it relative to the
	// current working directory so it matches the volume mount path.
	container := opts.StoreContainer
	if opts.StorageProvider == "Local" || opts.StorageProvider == "local" || opts.StorageProvider == "" {
		if !filepath.IsAbs(container) {
			cwd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("failed to get working directory: %w", err)
			}
			container = filepath.Join(cwd, container)
		}
	}

	cfg := snapstore.SnapstoreConfig{
		Provider:         opts.StorageProvider,
		Container:        container,
		Prefix:           opts.StorePrefix,
		EndpointOverride: opts.StoreEndpointOverride,
	}
	store, err := snapstore.NewSnapstore(cfg)
	if err != nil {
		return fmt.Errorf("failed to create snapstore: %w", err)
	}

	snaps, err := store.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(snaps) == 0 {
		logger.Info("no snapshots found, nothing to compact")
		return nil
	}

	// Find the highest LastRevision across all snapshots.
	var maxRev int64
	for _, s := range snaps {
		if s.LastRevision > maxRev {
			maxRev = s.LastRevision
		}
	}

	logger.Info("compact: found snapshots",
		zap.Int("count", len(snaps)),
		zap.Int64("maxRevision", maxRev),
	)

	return nil
}

// ParseArgs parses compact subcommand arguments from a slice of args.
func ParseArgs(args []string) (Options, error) {
	var opts Options
	for i := 0; i < len(args); i++ {
		switch {
		case strings.HasPrefix(args[i], "--storage-provider="):
			opts.StorageProvider = strings.TrimPrefix(args[i], "--storage-provider=")
		case args[i] == "--storage-provider" && i+1 < len(args):
			opts.StorageProvider = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--store-container="):
			opts.StoreContainer = strings.TrimPrefix(args[i], "--store-container=")
		case args[i] == "--store-container" && i+1 < len(args):
			opts.StoreContainer = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--store-prefix="):
			opts.StorePrefix = strings.TrimPrefix(args[i], "--store-prefix=")
		case args[i] == "--store-prefix" && i+1 < len(args):
			opts.StorePrefix = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--store-endpoint-override="):
			opts.StoreEndpointOverride = strings.TrimPrefix(args[i], "--store-endpoint-override=")
		case args[i] == "--store-endpoint-override" && i+1 < len(args):
			opts.StoreEndpointOverride = args[i+1]
			i++
		// Legacy flags forwarded by etcd-druid but no longer used — silently ignored.
		case strings.HasPrefix(args[i], "--full-snapshot-lease-name"):
		case strings.HasPrefix(args[i], "--delta-snapshot-lease-name"):
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				i++ // skip value token for two-arg form
			}
		}
	}
	return opts, nil
}

