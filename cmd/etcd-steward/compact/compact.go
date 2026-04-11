// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package compact provides the etcd-steward compact subcommand.
package compact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/gardener/etcd-steward/pkg/snapstore"
	"github.com/gardener/etcd-steward/pkg/snapshotlease"
)

// Options holds the parsed arguments for the compact subcommand.
type Options struct {
	StorageProvider       string
	StoreContainer        string
	StorePrefix           string
	StoreEndpointOverride string
	FullSnapshotLeaseName  string
	DeltaSnapshotLeaseName string
}

// Run executes the compact subcommand.
// It finds the highest revision across all snapshots in the store and
// advances the full snapshot K8s lease to that revision.
func Run(logger *zap.Logger, opts Options) error {
	// Read Kubernetes namespace from the service account token file.
	namespace, err := readNamespace()
	if err != nil {
		return fmt.Errorf("failed to read namespace: %w", err)
	}

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

	// Update K8s leases via in-cluster config.
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to build in-cluster config: %w", err)
	}
	k8sClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Derive etcd name from lease name: "<etcdname>-full-snap".
	etcdName := strings.TrimSuffix(opts.FullSnapshotLeaseName, "-full-snap")

	updater := snapshotlease.New(etcdName, namespace, k8sClient.CoordinationV1(), logger)
	ctx := context.Background()

	if err := updater.UpdateFullSnapshotLease(ctx, maxRev); err != nil {
		return fmt.Errorf("failed to update full snapshot lease: %w", err)
	}

	logger.Info("compact: updated full snapshot lease",
		zap.Int64("revision", maxRev),
		zap.String("lease", opts.FullSnapshotLeaseName),
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
		case strings.HasPrefix(args[i], "--full-snapshot-lease-name="):
			opts.FullSnapshotLeaseName = strings.TrimPrefix(args[i], "--full-snapshot-lease-name=")
		case args[i] == "--full-snapshot-lease-name" && i+1 < len(args):
			opts.FullSnapshotLeaseName = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--delta-snapshot-lease-name="):
			opts.DeltaSnapshotLeaseName = strings.TrimPrefix(args[i], "--delta-snapshot-lease-name=")
		case args[i] == "--delta-snapshot-lease-name" && i+1 < len(args):
			opts.DeltaSnapshotLeaseName = args[i+1]
			i++
		}
	}
	return opts, nil
}

// readNamespace reads the pod's namespace from the mounted service account file.
func readNamespace() (string, error) {
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", fmt.Errorf("could not read namespace file: %w", err)
	}
	ns := strings.TrimSpace(string(data))
	if ns == "" {
		return "", fmt.Errorf("namespace file is empty")
	}
	return ns, nil
}
