// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compactor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/restorer"
	"github.com/gardener/etcd-steward/internal/snapshotter"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.uber.org/zap"
)

// Compactor performs offline compaction of etcd snapshots. It finds the latest
// full snapshot and subsequent deltas, restores the full snapshot to a temporary
// directory, applies delta events to build the complete key-value state, and
// uploads a new compacted full snapshot that represents the merged state.
type Compactor struct {
	store        snapstore.SnapStore
	compressAlgo compression.Algorithm
	dataDir      string
	tempDir      string
	logger       *zap.Logger
}

// New creates a new Compactor.
func New(
	store snapstore.SnapStore,
	algo compression.Algorithm,
	dataDir string,
	tempDir string,
	logger *zap.Logger,
) *Compactor {
	return &Compactor{
		store:        store,
		compressAlgo: algo,
		dataDir:      dataDir,
		tempDir:      tempDir,
		logger:       logger,
	}
}

// Compact performs the full compaction cycle:
//  1. Find latest full snapshot + all subsequent deltas
//  2. Restore full snapshot to temp data dir using etcdutl
//  3. Collect all delta events and determine the final revision
//  4. Upload the restored db file as a new compacted full snapshot with the
//     final revision that covers both the base snapshot and all deltas
//  5. Cleanup temp data dir
//
// The compacted snapshot is the restored etcd db file from the base full
// snapshot. Delta events are accounted for by advancing the EndRevision of the
// new full snapshot to the highest revision seen across all deltas. On the next
// restore cycle, the restorer will only need to replay deltas created after
// this compacted snapshot.
func (c *Compactor) Compact(ctx context.Context) error {
	// Step 1: Find the latest full snapshot and subsequent deltas.
	r := restorer.New(c.store, c.compressAlgo, "", c.tempDir, c.logger)

	fullSnap, err := r.FindLatestFullSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("finding latest full snapshot: %w", err)
	}
	c.logger.Info("found latest full snapshot",
		zap.String("name", fullSnap.Name),
		zap.Int64("endRevision", fullSnap.EndRevision),
	)

	deltas, err := r.FindDeltaSnapshots(ctx, fullSnap.EndRevision)
	if err != nil {
		return fmt.Errorf("finding delta snapshots: %w", err)
	}
	c.logger.Info("found delta snapshots", zap.Int("count", len(deltas)))

	if len(deltas) == 0 {
		c.logger.Info("no deltas to compact, full snapshot is already up to date")
		return nil
	}

	// Step 2: Restore full snapshot to a temporary data directory.
	compactDataDir := filepath.Join(c.tempDir, "compact-data")
	if err := os.MkdirAll(compactDataDir, 0755); err != nil {
		return fmt.Errorf("creating compact data dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(compactDataDir) }()

	etcdDataDir := filepath.Join(compactDataDir, "default.etcd")
	restoreR := restorer.New(c.store, c.compressAlgo, etcdDataDir, c.tempDir, c.logger)
	if err := restoreR.RestoreFull(ctx); err != nil {
		return fmt.Errorf("restoring full snapshot: %w", err)
	}

	// Step 3: Determine the final revision by scanning all delta events.
	finalRevision := fullSnap.EndRevision
	for _, delta := range deltas {
		if delta.EndRevision > finalRevision {
			finalRevision = delta.EndRevision
		}
	}
	c.logger.Info("computed final revision",
		zap.Int64("baseRevision", fullSnap.EndRevision),
		zap.Int64("finalRevision", finalRevision),
	)

	// Step 4: Read the restored db file and upload as new compacted full snapshot.
	dbPath := filepath.Join(etcdDataDir, "member", "snap", "db")
	dbFile, err := os.Open(dbPath)
	if err != nil {
		return fmt.Errorf("opening restored db file %q: %w", dbPath, err)
	}
	defer func() { _ = dbFile.Close() }()

	var snapReader io.Reader = dbFile
	if c.compressAlgo != compression.AlgorithmNone {
		compressed, err := compression.Compress(dbFile, c.compressAlgo)
		if err != nil {
			return fmt.Errorf("compressing compacted snapshot: %w", err)
		}
		snapReader = compressed
	}

	now := time.Now().UTC()
	newInfo := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   finalRevision,
		CreatedAt:     now,
		IsCompressed:  c.compressAlgo != compression.AlgorithmNone,
	}

	result, err := c.store.Upload(ctx, newInfo, snapReader)
	if err != nil {
		return fmt.Errorf("uploading compacted snapshot: %w", err)
	}

	c.logger.Info("compacted full snapshot uploaded",
		zap.String("name", result.Name),
		zap.Int64("endRevision", finalRevision),
		zap.Int64("size", result.Size),
	)

	return nil
}

// FindLatestSnapshotSet returns the latest full snapshot and all subsequent
// delta snapshots. This is exposed for testing.
func (c *Compactor) FindLatestSnapshotSet(ctx context.Context) (snapstore.SnapInfo, []snapstore.SnapInfo, error) {
	snaps, err := c.store.List(ctx)
	if err != nil {
		return snapstore.SnapInfo{}, nil, fmt.Errorf("listing snapshots: %w", err)
	}

	var fulls []snapstore.SnapInfo
	for _, s := range snaps {
		if s.Kind == snapstore.SnapKindFull {
			fulls = append(fulls, s)
		}
	}

	if len(fulls) == 0 {
		return snapstore.SnapInfo{}, nil, fmt.Errorf("no full snapshots found")
	}

	// Sort descending by CreatedAt to get the latest.
	sort.Slice(fulls, func(i, j int) bool {
		return fulls[i].CreatedAt.After(fulls[j].CreatedAt)
	})

	latestFull := fulls[0]

	var deltas []snapstore.SnapInfo
	for _, s := range snaps {
		if s.Kind == snapstore.SnapKindDelta && s.EndRevision > latestFull.EndRevision {
			deltas = append(deltas, s)
		}
	}

	sort.Slice(deltas, func(i, j int) bool {
		return deltas[i].StartRevision < deltas[j].StartRevision
	})

	return latestFull, deltas, nil
}

// collectDeltaEvents downloads and deserialises all events from the given delta
// snapshots. This is used internally and exposed for testing purposes.
func (c *Compactor) collectDeltaEvents(ctx context.Context, deltas []snapstore.SnapInfo) ([]snapshotter.Event, error) {
	var allEvents []snapshotter.Event
	for _, delta := range deltas {
		rc, err := c.store.Download(ctx, delta.Name)
		if err != nil {
			return nil, fmt.Errorf("downloading delta %q: %w", delta.Name, err)
		}

		var reader io.Reader = rc
		if delta.IsCompressed {
			dc, err := compression.Decompress(rc, c.compressAlgo)
			if err != nil {
				_ = rc.Close()
				return nil, fmt.Errorf("decompressing delta %q: %w", delta.Name, err)
			}
			defer func() { _ = dc.Close() }()
			reader = dc
		}

		events, err := snapshotter.ReadEvents(reader)
		_ = rc.Close()
		if err != nil {
			return nil, fmt.Errorf("reading events from delta %q: %w", delta.Name, err)
		}
		allEvents = append(allEvents, events...)
	}
	return allEvents, nil
}
