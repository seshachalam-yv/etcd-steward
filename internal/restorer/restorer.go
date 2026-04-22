// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package restorer

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.uber.org/zap"
)

// Restorer restores an etcd data directory from snapshots stored in a SnapStore.
type Restorer struct {
	store        snapstore.SnapStore
	compressAlgo compression.Algorithm
	dataDir      string
	tempDir      string
	logger       *zap.Logger
}

// New creates a new Restorer.
func New(
	store snapstore.SnapStore,
	compressAlgo compression.Algorithm,
	dataDir string,
	tempDir string,
	logger *zap.Logger,
) *Restorer {
	return &Restorer{
		store:        store,
		compressAlgo: compressAlgo,
		dataDir:      dataDir,
		tempDir:      tempDir,
		logger:       logger,
	}
}

// RestoreFull downloads the latest full snapshot, decompresses it if needed,
// writes it to a temporary file, and uses etcdutl/snapshot.Restore() to
// restore the etcd data directory.
func (r *Restorer) RestoreFull(ctx context.Context) error {
	fullSnap, err := r.FindLatestFullSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("finding latest full snapshot: %w", err)
	}

	r.logger.Info("restoring from full snapshot",
		zap.String("name", fullSnap.Name),
		zap.Int64("endRevision", fullSnap.EndRevision),
	)

	rc, err := r.store.Download(ctx, fullSnap.Name)
	if err != nil {
		return fmt.Errorf("downloading snapshot %q: %w", fullSnap.Name, err)
	}
	defer func() { _ = rc.Close() }()

	// Decompress if the snapshot is compressed.
	var reader io.Reader = rc
	if fullSnap.IsCompressed {
		dc, err := compression.Decompress(rc, r.compressAlgo)
		if err != nil {
			return fmt.Errorf("decompressing snapshot %q: %w", fullSnap.Name, err)
		}
		defer func() { _ = dc.Close() }()
		reader = dc
	}

	// Write to a temporary file for etcdutl restore.
	if err := os.MkdirAll(r.tempDir, 0755); err != nil {
		return fmt.Errorf("creating temp dir %q: %w", r.tempDir, err)
	}
	tmpFile := filepath.Join(r.tempDir, "restore.db")
	f, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("creating temp file %q: %w", tmpFile, err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpFile)
	}()

	if _, err := io.Copy(f, reader); err != nil {
		return fmt.Errorf("writing snapshot to temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Use etcdutl/snapshot to perform the actual restore.
	mgr := snapshot.NewV3(r.logger)
	restoreCfg := snapshot.RestoreConfig{
		SnapshotPath:        tmpFile,
		Name:                "default",
		OutputDataDir:       r.dataDir,
		PeerURLs:            []string{"http://localhost:2380"},
		InitialCluster:      "default=http://localhost:2380",
		InitialClusterToken: "etcd-cluster",
		SkipHashCheck:       true,
	}

	if err := mgr.Restore(restoreCfg); err != nil {
		return fmt.Errorf("etcdutl restore: %w", err)
	}

	r.logger.Info("full snapshot restored",
		zap.String("name", fullSnap.Name),
		zap.String("dataDir", r.dataDir),
	)
	return nil
}

// FindLatestFullSnapshot returns the most recent full snapshot from the store.
func (r *Restorer) FindLatestFullSnapshot(ctx context.Context) (snapstore.SnapInfo, error) {
	snaps, err := r.store.List(ctx)
	if err != nil {
		return snapstore.SnapInfo{}, fmt.Errorf("listing snapshots: %w", err)
	}

	var fulls []snapstore.SnapInfo
	for _, s := range snaps {
		if s.Kind == snapstore.SnapKindFull {
			fulls = append(fulls, s)
		}
	}

	if len(fulls) == 0 {
		return snapstore.SnapInfo{}, fmt.Errorf("no full snapshots found")
	}

	// Sort descending by CreatedAt to get the latest.
	sort.Slice(fulls, func(i, j int) bool {
		return fulls[i].CreatedAt.After(fulls[j].CreatedAt)
	})

	return fulls[0], nil
}

// FindDeltaSnapshots returns all delta snapshots with EndRevision greater than
// afterRevision, sorted by StartRevision ascending.
func (r *Restorer) FindDeltaSnapshots(ctx context.Context, afterRevision int64) ([]snapstore.SnapInfo, error) {
	snaps, err := r.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing snapshots: %w", err)
	}

	var deltas []snapstore.SnapInfo
	for _, s := range snaps {
		if s.Kind == snapstore.SnapKindDelta && s.EndRevision > afterRevision {
			deltas = append(deltas, s)
		}
	}

	sort.Slice(deltas, func(i, j int) bool {
		return deltas[i].StartRevision < deltas[j].StartRevision
	})

	return deltas, nil
}

// ApplyDeltas is a stub that will replay delta snapshots on top of a restored
// full snapshot. This is not yet implemented.
func (r *Restorer) ApplyDeltas(_ context.Context, _ []snapstore.SnapInfo) error {
	r.logger.Warn("ApplyDeltas is not yet implemented")
	return nil
}
