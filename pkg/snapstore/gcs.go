// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSSnapstore stores snapshots in a Google Cloud Storage bucket.
type GCSSnapstore struct {
	client  *storage.Client
	bucket  string
	prefix  string
	tempDir string
}

// NewGCS creates a new GCSSnapstore from the given SnapstoreConfig.
// Credentials are loaded from the file pointed to by ETCD_STEWARD_GOOGLE_APPLICATION_CREDENTIALS,
// or the standard Google SDK credential chain (GOOGLE_APPLICATION_CREDENTIALS / workload identity).
func NewGCS(cfg SnapstoreConfig) (*GCSSnapstore, error) {
	ctx := context.Background()

	opts := []option.ClientOption{}

	// Support an explicit credentials file override via env.
	if credPath := os.Getenv("ETCD_STEWARD_GOOGLE_APPLICATION_CREDENTIALS"); credPath != "" {
		opts = append(opts, option.WithCredentialsFile(credPath))
	}

	// Support emulator for testing.
	if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
		endpoint := "http://" + emulatorHost + "/storage/v1/"
		opts = append(opts, option.WithEndpoint(endpoint), option.WithoutAuthentication())
	} else if cfg.EndpointOverride != "" {
		opts = append(opts, option.WithEndpoint(cfg.EndpointOverride))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	if cfg.Container == "" {
		return nil, fmt.Errorf("GCS bucket name is required (set via Container config field)")
	}

	tempDir := cfg.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	return &GCSSnapstore{
		client:  client,
		bucket:  cfg.Container,
		prefix:  cfg.Prefix,
		tempDir: tempDir,
	}, nil
}

// Save uploads the snapshot to GCS.
// For objects larger than the minimum chunk size, data is written directly via a streaming writer.
func (g *GCSSnapstore) Save(snap Snapshot, r io.ReadCloser) error {
	defer r.Close() //nolint:errcheck

	ctx := context.Background()
	key := path.Join(g.prefix, snap.SnapDir, snap.SnapName)

	wc := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)
	if _, err := io.Copy(wc, r); err != nil {
		_ = wc.Close()
		return fmt.Errorf("failed to write snapshot to GCS object %s: %w", key, err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer for %s: %w", key, err)
	}
	return nil
}

// Fetch returns a reader for the given snapshot from GCS.
func (g *GCSSnapstore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	ctx := context.Background()
	key := path.Join(g.prefix, snap.SnapDir, snap.SnapName)
	rc, err := g.client.Bucket(g.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object %s: %w", key, err)
	}
	return rc, nil
}

// List returns all snapshots in the GCS bucket under the configured prefix,
// sorted by LastRevision ascending.
func (g *GCSSnapstore) List() ([]Snapshot, error) {
	ctx := context.Background()

	prefix := g.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	query := &storage.Query{Prefix: prefix}
	it := g.client.Bucket(g.bucket).Objects(ctx, query)

	var snapshots []Snapshot
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list GCS objects under prefix %q: %w", prefix, err)
		}

		// Strip bucket prefix to get relative path.
		rel := strings.TrimPrefix(attrs.Name, prefix)
		parts := strings.SplitN(rel, "/", 2)
		if len(parts) != 2 {
			continue
		}
		snapDir := parts[0]
		snapName := parts[1]
		if snapName == "" || strings.HasSuffix(snapName, ".tmp") {
			continue
		}

		snap, err := ParseSnapshotName(snapName)
		if err != nil {
			continue
		}
		snap.SnapDir = snapDir
		snap.SnapName = snapName
		snapshots = append(snapshots, snap)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].LastRevision < snapshots[j].LastRevision
	})

	return snapshots, nil
}

// Delete removes the given snapshot from GCS.
func (g *GCSSnapstore) Delete(snap Snapshot) error {
	ctx := context.Background()
	key := path.Join(g.prefix, snap.SnapDir, snap.SnapName)
	if err := g.client.Bucket(g.bucket).Object(key).Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete GCS object %s: %w", key, err)
	}
	return nil
}
