// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

const (
	// absMinChunkSize is the minimum block size for Azure staged-block uploads (4 MiB).
	absMinChunkSize = 4 * 1024 * 1024
	// absDefaultDomain is the default Azure Blob Storage domain.
	absDefaultDomain = "blob.core.windows.net"
)

// absCredentials holds the parsed Azure Blob Storage authentication configuration.
type absCredentials struct {
	StorageAccount string  `json:"storageAccount"`
	StorageKey     string  `json:"storageKey"`
	BucketName     string  `json:"bucketName"`
	Domain         *string `json:"domain,omitempty"`
}

// sectionReadSeekCloser wraps an io.SectionReader to implement io.ReadSeekCloser.
// The underlying file is NOT closed by Close — the caller owns the file lifetime.
type sectionReadSeekCloser struct {
	*io.SectionReader
}

func (s *sectionReadSeekCloser) Close() error { return nil }

// newSectionRSC returns an io.ReadSeekCloser over the given range of r.
func newSectionRSC(r io.ReaderAt, off, n int64) io.ReadSeekCloser {
	return &sectionReadSeekCloser{io.NewSectionReader(r, off, n)}
}

// ABSSnapstore stores snapshots in an Azure Blob Storage container.
type ABSSnapstore struct {
	client  *container.Client
	prefix  string
	tempDir string
}

// NewABS creates a new ABSSnapstore from the given SnapstoreConfig.
// Credentials are loaded from the JSON file pointed to by the
// ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS_JSON env var, or from
// ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS (a directory with credential files).
func NewABS(cfg SnapstoreConfig) (*ABSSnapstore, error) {
	creds, err := loadABSCredentials(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load Azure credentials: %w", err)
	}

	if creds.StorageAccount == "" {
		return nil, fmt.Errorf("azure storage account name is required")
	}
	if creds.StorageKey == "" {
		return nil, fmt.Errorf("azure storage key is required")
	}

	bucketName := cfg.Container
	if bucketName == "" {
		bucketName = creds.BucketName
	}
	if bucketName == "" {
		return nil, fmt.Errorf("azure container name is required (set via Container config field or bucketName in credentials)")
	}

	sharedKey, err := azblob.NewSharedKeyCredential(creds.StorageAccount, creds.StorageKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure shared key credential: %w", err)
	}

	domain := absDefaultDomain
	if creds.Domain != nil && *creds.Domain != "" {
		domain = *creds.Domain
	}

	var containerURL string
	if cfg.EndpointOverride != "" {
		containerURL = cfg.EndpointOverride + "/" + bucketName
	} else {
		containerURL = fmt.Sprintf("https://%s.%s/%s", creds.StorageAccount, domain, bucketName)
	}

	client, err := container.NewClientWithSharedKeyCredential(containerURL, sharedKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure container client for %s: %w", containerURL, err)
	}

	tempDir := cfg.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	return &ABSSnapstore{
		client:  client,
		prefix:  cfg.Prefix,
		tempDir: tempDir,
	}, nil
}

// Save uploads the snapshot to ABS using staged block blobs.
func (a *ABSSnapstore) Save(snap Snapshot, r io.ReadCloser) error {
	defer r.Close() //nolint:errcheck

	// Buffer to temp file to know the total size for block chunking.
	tmp, err := os.CreateTemp(a.tempDir, "etcd-steward-abs-")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) //nolint:errcheck
	defer tmp.Close()           //nolint:errcheck

	size, err := io.Copy(tmp, r)
	if err != nil {
		return fmt.Errorf("failed to buffer snapshot to temp file: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek temp file: %w", err)
	}

	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blobClient := a.client.NewBlockBlobClient(blobName)
	ctx := context.Background()

	chunkSize := int64(absMinChunkSize)
	if n := size / 49999; n > chunkSize {
		chunkSize = n
	}

	var blockIDs []string
	partNumber := int64(0)
	offset := int64(0)

	for offset < size {
		end := offset + chunkSize
		if end > size {
			end = size
		}

		blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%010d", partNumber)))
		partReader := newSectionRSC(tmp, offset, end-offset)

		_, err := blobClient.StageBlock(ctx, blockID, partReader, &blockblob.StageBlockOptions{})
		if err != nil {
			return fmt.Errorf("failed to stage block %d for %s: %w", partNumber, blobName, err)
		}

		blockIDs = append(blockIDs, blockID)
		offset = end
		partNumber++
	}

	if _, err := blobClient.CommitBlockList(ctx, blockIDs, &blockblob.CommitBlockListOptions{}); err != nil {
		return fmt.Errorf("failed to commit block list for %s: %w", blobName, err)
	}

	return nil
}

// Fetch returns a reader for the given snapshot from ABS.
func (a *ABSSnapstore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blobClient := a.client.NewBlockBlobClient(blobName)

	resp, err := blobClient.DownloadStream(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download ABS blob %s: %w", blobName, err)
	}
	return resp.Body, nil
}

// List returns all snapshots in the ABS container under the configured prefix,
// sorted by LastRevision ascending.
func (a *ABSSnapstore) List() ([]Snapshot, error) {
	prefix := a.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var snapshots []Snapshot

	pager := a.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	ctx := context.Background()
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list ABS blobs under prefix %q: %w", prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			// Strip the container prefix to get the relative path.
			rel := strings.TrimPrefix(*item.Name, prefix)
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
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].LastRevision < snapshots[j].LastRevision
	})

	return snapshots, nil
}

// Delete removes the given snapshot from ABS.
func (a *ABSSnapstore) Delete(snap Snapshot) error {
	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blobClient := a.client.NewBlockBlobClient(blobName)
	_, err := blobClient.Delete(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to delete ABS blob %s: %w", blobName, err)
	}
	return nil
}

// loadABSCredentials reads Azure credentials from environment or config.
func loadABSCredentials(_ SnapstoreConfig) (absCredentials, error) {
	var creds absCredentials

	// Try JSON credentials file from env.
	if jsonPath := os.Getenv("ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS_JSON"); jsonPath != "" {
		data, err := os.ReadFile(jsonPath)
		if err != nil {
			return creds, fmt.Errorf("failed to read Azure credentials file %s: %w", jsonPath, err)
		}
		if err := json.Unmarshal(data, &creds); err != nil {
			return creds, fmt.Errorf("failed to parse Azure credentials file %s: %w", jsonPath, err)
		}
		return creds, nil
	}

	// Try credentials directory.
	if dir := os.Getenv("ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS"); dir != "" {
		if data, err := os.ReadFile(dir + "/storageAccount"); err == nil {
			creds.StorageAccount = strings.TrimSpace(string(data))
		}
		if data, err := os.ReadFile(dir + "/storageKey"); err == nil {
			creds.StorageKey = strings.TrimSpace(string(data))
		}
		if data, err := os.ReadFile(dir + "/bucketName"); err == nil {
			creds.BucketName = strings.TrimSpace(string(data))
		}
	}

	return creds, nil
}
