// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package restoration provides native snapshot restoration for etcd without any
// dependency on etcd-backup-restore.
package restoration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// ErrNoSnapshotFound is returned when no full snapshot exists in the store.
var ErrNoSnapshotFound = errors.New("no full snapshot found in snapstore")

// Restore restores the etcd data directory from snapshots in the given store.
//
// Steps:
//  1. List snapshots and find the latest Full snapshot
//  2. Fetch and decompress the full snapshot to tempDir/db
//  3. Find all Incremental snapshots after the full snapshot (delta replay is stubbed for Phase 2)
//  4. Move the restored DB to dataDir/member/snap/db
func Restore(
	_ context.Context,
	store snapstore.Snapstore,
	compressor compression.Compressor,
	dataDir string,
	tempDir string,
	_ string,
	_ string,
	_ string,
	logger *zap.Logger,
) error {
	snaps, err := store.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	// Find the latest Full snapshot (list is sorted by LastRevision ascending).
	var latestFull *snapstore.Snapshot
	for i := len(snaps) - 1; i >= 0; i-- {
		if snaps[i].Kind == "Full" {
			s := snaps[i]
			latestFull = &s
			break
		}
	}

	if latestFull == nil {
		return ErrNoSnapshotFound
	}

	logger.Info("restoring from full snapshot",
		zap.String("snapshot", latestFull.Path()),
		zap.Int64("revision", latestFull.LastRevision),
	)

	// Ensure temp directory exists.
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir %s: %w", tempDir, err)
	}

	// Fetch the full snapshot.
	reader, err := store.Fetch(*latestFull)
	if err != nil {
		return fmt.Errorf("failed to fetch full snapshot %s: %w", latestFull.Path(), err)
	}
	defer reader.Close() //nolint:errcheck

	// Decompress.
	decompressed, err := compressor.Decompress(reader)
	if err != nil {
		return fmt.Errorf("failed to decompress full snapshot: %w", err)
	}
	defer decompressed.Close() //nolint:errcheck

	// Write decompressed data to tempDir/db.
	tmpDBPath := filepath.Join(tempDir, "db")
	tmpFile, err := os.OpenFile(tmpDBPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp DB file %s: %w", tmpDBPath, err)
	}

	if _, err := io.Copy(tmpFile, decompressed); err != nil {
		tmpFile.Close() //nolint:errcheck
		return fmt.Errorf("failed to write snapshot to temp DB: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp DB file: %w", err)
	}

	// Find incremental snapshots after the full snapshot.
	var deltas []snapstore.Snapshot
	for _, s := range snaps {
		if s.Kind == "Incremental" && s.StartRevision >= latestFull.LastRevision {
			deltas = append(deltas, s)
		}
	}

	if len(deltas) > 0 {
		// Delta replay (MVCC event replay onto bbolt DB) is not yet implemented.
		// v0.1.0 uses full-snapshot-only restoration. Incremental delta application
		// will be added in a follow-up (etcd-steward#11).
		logger.Info("skipping delta application — not implemented in v0.1.0 (see issue #11)",
			zap.Int("deltaCount", len(deltas)),
		)
	}

	// Create the target directory.
	targetDir := filepath.Join(dataDir, "member", "snap")
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory %s: %w", targetDir, err)
	}

	// Move the restored DB to the target location.
	targetPath := filepath.Join(targetDir, "db")
	if err := os.Rename(tmpDBPath, targetPath); err != nil {
		// Rename may fail across filesystems — fall back to copy.
		if copyErr := copyFile(tmpDBPath, targetPath); copyErr != nil {
			return fmt.Errorf("failed to move restored DB to %s: rename=%w, copy=%v", targetPath, err, copyErr)
		}
		os.Remove(tmpDBPath) //nolint:errcheck
	}

	logger.Info("restoration completed",
		zap.String("dbPath", targetPath),
		zap.Int64("revision", latestFull.LastRevision),
	)

	return nil
}

// copyFile copies src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close() //nolint:errcheck

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close() //nolint:errcheck
		return err
	}
	return out.Close()
}
