// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	gcsstorage "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/gardener/etcd-steward/internal/errors"
)

// Compile-time assertion that GCSSnapStore implements SnapStore.
var _ SnapStore = (*GCSSnapStore)(nil)

// gcsObjectIterator abstracts the GCS object listing iterator.
type gcsObjectIterator interface {
	Next() (*gcsstorage.ObjectAttrs, error)
}

// gcsBucketAPI abstracts the GCS bucket/object operations for testability.
type gcsBucketAPI interface {
	NewWriter(ctx context.Context, object string) io.WriteCloser
	NewReader(ctx context.Context, object string) (io.ReadCloser, error)
	Attrs(ctx context.Context, object string) (*gcsstorage.ObjectAttrs, error)
	Objects(ctx context.Context, query *gcsstorage.Query) gcsObjectIterator
	Delete(ctx context.Context, object string) error
}

// realGCSBucket adapts *gcsstorage.Client to gcsBucketAPI.
type realGCSBucket struct {
	client *gcsstorage.Client
	bucket string
}

func (r *realGCSBucket) NewWriter(ctx context.Context, object string) io.WriteCloser {
	return r.client.Bucket(r.bucket).Object(object).NewWriter(ctx)
}

func (r *realGCSBucket) NewReader(ctx context.Context, object string) (io.ReadCloser, error) {
	return r.client.Bucket(r.bucket).Object(object).NewReader(ctx)
}

func (r *realGCSBucket) Attrs(ctx context.Context, object string) (*gcsstorage.ObjectAttrs, error) {
	return r.client.Bucket(r.bucket).Object(object).Attrs(ctx)
}

func (r *realGCSBucket) Objects(ctx context.Context, query *gcsstorage.Query) gcsObjectIterator {
	return r.client.Bucket(r.bucket).Objects(ctx, query)
}

func (r *realGCSBucket) Delete(ctx context.Context, object string) error {
	return r.client.Bucket(r.bucket).Object(object).Delete(ctx)
}

// GCSSnapStore stores snapshots in a Google Cloud Storage bucket.
type GCSSnapStore struct {
	api    gcsBucketAPI
	bucket string
	prefix string
}

// NewGCS creates a new GCSSnapStore.
// bucket is the GCS bucket name.
// prefix is the key prefix under which snapshots are stored.
// config is reserved for future provider-specific options.
func NewGCS(bucket, prefix string, config map[string]string) (*GCSSnapStore, error) {
	client, err := gcsstorage.NewClient(context.Background())
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, "failed to create GCS client", err)
	}
	return &GCSSnapStore{
		api:    &realGCSBucket{client: client, bucket: bucket},
		bucket: bucket,
		prefix: prefix,
	}, nil
}

// newGCSWithAPI creates a GCSSnapStore with an injected gcsBucketAPI for testing.
func newGCSWithAPI(api gcsBucketAPI, bucket, prefix string) *GCSSnapStore {
	return &GCSSnapStore{
		api:    api,
		bucket: bucket,
		prefix: prefix,
	}
}

// objectName returns the full GCS object name for a snapshot name.
func (g *GCSSnapStore) objectName(name string) string {
	if g.prefix == "" {
		return name
	}
	return path.Join(g.prefix, name)
}

// nameFromObject extracts the snapshot name from a full object name.
func (g *GCSSnapStore) nameFromObject(objName string) string {
	if g.prefix == "" {
		return objName
	}
	return strings.TrimPrefix(objName, g.prefix+"/")
}

// Upload stores snapshot data in GCS and returns the resulting metadata.
func (g *GCSSnapStore) Upload(ctx context.Context, info SnapInfo, data io.Reader) (SnapInfo, error) {
	name := snapFileName(info)
	objName := g.objectName(name)

	w := g.api.NewWriter(ctx, objName)
	n, err := io.Copy(w, data)
	if err != nil {
		_ = w.Close()
		return SnapInfo{}, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to write snapshot %q to GCS", name), err)
	}
	if err := w.Close(); err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to close GCS writer for snapshot %q", name), err)
	}

	result := info
	result.Name = name
	result.Size = n
	result.Prefix = g.prefix
	return result, nil
}

// Download returns a reader for the snapshot identified by name from GCS.
func (g *GCSSnapStore) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	objName := g.objectName(name)
	r, err := g.api.NewReader(ctx, objName)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to download snapshot %q from GCS", name), err)
	}
	return r, nil
}

// GetInfo returns metadata for the snapshot identified by name from GCS.
func (g *GCSSnapStore) GetInfo(ctx context.Context, name string) (SnapInfo, error) {
	objName := g.objectName(name)
	attrs, err := g.api.Attrs(ctx, objName)
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to get info for snapshot %q from GCS", name), err)
	}

	info, parseErr := parseSnapFileName(name)
	if parseErr != nil {
		return SnapInfo{}, parseErr
	}
	info.Size = attrs.Size
	info.Prefix = g.prefix
	return info, nil
}

// List returns all snapshots under the prefix, sorted by CreatedAt ascending.
func (g *GCSSnapStore) List(ctx context.Context) ([]SnapInfo, error) {
	listPrefix := g.prefix
	if listPrefix != "" && !strings.HasSuffix(listPrefix, "/") {
		listPrefix += "/"
	}

	query := &gcsstorage.Query{Prefix: listPrefix}
	it := g.api.Objects(ctx, query)

	var snaps []SnapInfo
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.Wrap(errors.ErrCodeStorage, "failed to list snapshots from GCS", err)
		}

		name := g.nameFromObject(attrs.Name)
		info, parseErr := parseSnapFileName(name)
		if parseErr != nil {
			// Skip files that don't match the expected naming convention.
			continue
		}
		info.Size = attrs.Size
		info.Prefix = g.prefix
		snaps = append(snaps, info)
	}

	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].CreatedAt.Before(snaps[j].CreatedAt)
	})

	return snaps, nil
}

// Delete removes the snapshot identified by name from GCS.
func (g *GCSSnapStore) Delete(ctx context.Context, name string) error {
	objName := g.objectName(name)
	if err := g.api.Delete(ctx, objName); err != nil {
		return errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to delete snapshot %q from GCS", name), err)
	}
	return nil
}
