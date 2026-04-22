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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/gardener/etcd-steward/internal/errors"
)

// Compile-time assertion that ABSSnapStore implements SnapStore.
var _ SnapStore = (*ABSSnapStore)(nil)

// absBlobItem represents a single blob in a listing result.
type absBlobItem struct {
	Name          string
	ContentLength int64
}

// absListPage represents a page of blob listing results.
type absListPage struct {
	Items   []absBlobItem
	HasMore bool
}

// absAPI abstracts Azure Blob Storage operations for testability.
type absAPI interface {
	UploadStream(ctx context.Context, container, blob string, body io.Reader) error
	DownloadStream(ctx context.Context, container, blob string) (io.ReadCloser, error)
	ListBlobs(ctx context.Context, container string, prefix string) ([]absBlobItem, error)
	DeleteBlob(ctx context.Context, container, blob string) error
}

// realABSClient adapts *azblob.Client to absAPI.
type realABSClient struct {
	client *azblob.Client
}

func (r *realABSClient) UploadStream(ctx context.Context, container, blob string, body io.Reader) error {
	_, err := r.client.UploadStream(ctx, container, blob, body, nil)
	return err
}

func (r *realABSClient) DownloadStream(ctx context.Context, container, blob string) (io.ReadCloser, error) {
	resp, err := r.client.DownloadStream(ctx, container, blob, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (r *realABSClient) ListBlobs(ctx context.Context, container string, prefix string) ([]absBlobItem, error) {
	pager := r.client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	var items []absBlobItem
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			var size int64
			if item.Properties != nil && item.Properties.ContentLength != nil {
				size = *item.Properties.ContentLength
			}
			items = append(items, absBlobItem{
				Name:          *item.Name,
				ContentLength: size,
			})
		}
	}
	return items, nil
}

func (r *realABSClient) DeleteBlob(ctx context.Context, container, blob string) error {
	_, err := r.client.DeleteBlob(ctx, container, blob, nil)
	return err
}

// ABSSnapStore stores snapshots in Azure Blob Storage.
type ABSSnapStore struct {
	api       absAPI
	container string
	prefix    string
}

// NewABS creates a new ABSSnapStore.
// container is the Azure Blob Storage container name.
// prefix is the key prefix under which snapshots are stored.
// config must contain "storageAccount" and "storageKey" keys.
func NewABS(container, prefix string, config map[string]string) (*ABSSnapStore, error) {
	storageAccount := config["storageAccount"]
	storageKey := config["storageKey"]

	if storageAccount == "" || storageKey == "" {
		return nil, errors.New(errors.ErrCodeConfig, "storageAccount and storageKey are required for ABS provider")
	}

	cred, err := azblob.NewSharedKeyCredential(storageAccount, storageKey)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, "failed to create Azure shared key credential", err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, "failed to create Azure Blob client", err)
	}

	return &ABSSnapStore{
		api:       &realABSClient{client: client},
		container: container,
		prefix:    prefix,
	}, nil
}

// newABSWithAPI creates an ABSSnapStore with an injected absAPI for testing.
func newABSWithAPI(api absAPI, container, prefix string) *ABSSnapStore {
	return &ABSSnapStore{
		api:       api,
		container: container,
		prefix:    prefix,
	}
}

// blobName returns the full blob name for a snapshot name.
func (a *ABSSnapStore) blobName(name string) string {
	if a.prefix == "" {
		return name
	}
	return path.Join(a.prefix, name)
}

// nameFromBlob extracts the snapshot name from a full blob name.
func (a *ABSSnapStore) nameFromBlob(blob string) string {
	if a.prefix == "" {
		return blob
	}
	return strings.TrimPrefix(blob, a.prefix+"/")
}

// Upload stores snapshot data in Azure Blob Storage and returns the resulting metadata.
func (a *ABSSnapStore) Upload(ctx context.Context, info SnapInfo, data io.Reader) (SnapInfo, error) {
	name := snapFileName(info)
	blob := a.blobName(name)

	if err := a.api.UploadStream(ctx, a.container, blob, data); err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to upload snapshot %q to ABS", name), err)
	}

	result := info
	result.Name = name
	result.Prefix = a.prefix
	return result, nil
}

// Download returns a reader for the snapshot identified by name from Azure Blob Storage.
func (a *ABSSnapStore) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	blob := a.blobName(name)
	body, err := a.api.DownloadStream(ctx, a.container, blob)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to download snapshot %q from ABS", name), err)
	}
	return body, nil
}

// GetInfo returns metadata for the snapshot identified by name from Azure Blob Storage.
func (a *ABSSnapStore) GetInfo(ctx context.Context, name string) (SnapInfo, error) {
	blob := a.blobName(name)
	items, err := a.api.ListBlobs(ctx, a.container, blob)
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to get info for snapshot %q from ABS", name), err)
	}

	for _, item := range items {
		if item.Name == blob {
			info, parseErr := parseSnapFileName(name)
			if parseErr != nil {
				return SnapInfo{}, parseErr
			}
			info.Size = item.ContentLength
			info.Prefix = a.prefix
			return info, nil
		}
	}

	return SnapInfo{}, errors.New(errors.ErrCodeNotFound, fmt.Sprintf("snapshot %q not found in ABS", name))
}

// List returns all snapshots under the prefix, sorted by CreatedAt ascending.
func (a *ABSSnapStore) List(ctx context.Context) ([]SnapInfo, error) {
	listPrefix := a.prefix
	if listPrefix != "" && !strings.HasSuffix(listPrefix, "/") {
		listPrefix += "/"
	}

	items, err := a.api.ListBlobs(ctx, a.container, listPrefix)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, "failed to list snapshots from ABS", err)
	}

	var snaps []SnapInfo
	for _, item := range items {
		name := a.nameFromBlob(item.Name)
		info, parseErr := parseSnapFileName(name)
		if parseErr != nil {
			// Skip files that don't match the expected naming convention.
			continue
		}
		info.Size = item.ContentLength
		info.Prefix = a.prefix
		snaps = append(snaps, info)
	}

	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].CreatedAt.Before(snaps[j].CreatedAt)
	})

	return snaps, nil
}

// Delete removes the snapshot identified by name from Azure Blob Storage.
func (a *ABSSnapStore) Delete(ctx context.Context, name string) error {
	blob := a.blobName(name)
	if err := a.api.DeleteBlob(ctx, a.container, blob); err != nil {
		return errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to delete snapshot %q from ABS", name), err)
	}
	return nil
}
