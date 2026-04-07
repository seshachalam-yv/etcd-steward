// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package snapstore provides snapshot storage implementations.
package snapstore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LocalSnapstore stores snapshots as files in a local directory.
type LocalSnapstore struct {
	baseDir string
}

// NewLocal creates a new LocalSnapstore rooted at baseDir.
func NewLocal(baseDir string) *LocalSnapstore {
	return &LocalSnapstore{baseDir: baseDir}
}

// Save writes the snapshot data from r into the store atomically.
// It writes to a temporary file first and then renames it to the final path.
func (l *LocalSnapstore) Save(snap Snapshot, r io.ReadCloser) error {
	defer r.Close() //nolint:errcheck

	dir := filepath.Join(l.baseDir, snap.SnapDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory %s: %w", dir, err)
	}

	finalPath := filepath.Join(dir, snap.SnapName)
	tmpPath := finalPath + ".tmp"

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp file %s: %w", tmpPath, err)
	}

	if _, err := io.Copy(tmpFile, r); err != nil {
		tmpFile.Close()    //nolint:errcheck
		os.Remove(tmpPath) //nolint:errcheck
		return fmt.Errorf("failed to write snapshot data to %s: %w", tmpPath, err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath) //nolint:errcheck
		return fmt.Errorf("failed to close temp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath) //nolint:errcheck
		return fmt.Errorf("failed to rename temp file to %s: %w", finalPath, err)
	}

	return nil
}

// Fetch returns a reader for the given snapshot.
func (l *LocalSnapstore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	path := filepath.Join(l.baseDir, snap.SnapDir, snap.SnapName)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot %s: %w", path, err)
	}
	return f, nil
}

// List returns all snapshots in the store, sorted by LastRevision ascending.
func (l *LocalSnapstore) List() ([]Snapshot, error) {
	var snapshots []Snapshot

	err := filepath.Walk(l.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		name := info.Name()
		// Skip temp files
		if strings.HasSuffix(name, ".tmp") {
			return nil
		}

		snap, parseErr := ParseSnapshotName(name)
		if parseErr != nil {
			return nil
		}

		relDir, relErr := filepath.Rel(l.baseDir, filepath.Dir(path))
		if relErr != nil {
			return relErr
		}
		snap.SnapDir = relDir
		snap.SnapName = name

		snapshots = append(snapshots, snap)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk snapstore directory %s: %w", l.baseDir, err)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].LastRevision < snapshots[j].LastRevision
	})

	return snapshots, nil
}

// Delete removes the given snapshot from the store.
func (l *LocalSnapstore) Delete(snap Snapshot) error {
	path := filepath.Join(l.baseDir, snap.SnapDir, snap.SnapName)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to delete snapshot %s: %w", path, err)
	}
	return nil
}
