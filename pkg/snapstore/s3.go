// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// s3MinChunkSize is the minimum part size for S3 multipart uploads (5 MiB).
	s3MinChunkSize = 5 * 1024 * 1024
	// s3ChunkUploadTimeout is the per-part upload timeout.
	s3ChunkUploadTimeout = 3 * time.Minute
)

// s3Credentials holds the parsed S3 authentication configuration.
type s3Credentials struct {
	Region          string  `json:"region"`
	Endpoint        *string `json:"endpoint,omitempty"`
	BucketName      string  `json:"bucketName"`
	AccessKeyID     string  `json:"accessKeyID"`
	SecretAccessKey string  `json:"secretAccessKey"`
	ForcePathStyle  bool    `json:"s3ForcePathStyle"`
}

// S3Snapstore stores snapshots in an AWS S3 bucket.
type S3Snapstore struct {
	client     *s3.Client
	bucket     string
	prefix     string
	tempDir    string
}

// NewS3 creates a new S3Snapstore from the given SnapstoreConfig.
// Credentials are loaded from the JSON file pointed to by the
// ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS_JSON env var, or from
// ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS (a directory containing a .json file),
// or from the standard AWS SDK credential chain.
func NewS3(cfg SnapstoreConfig) (*S3Snapstore, error) {
	creds, err := loadS3Credentials(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load S3 credentials: %w", err)
	}

	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(creds.Region),
	}
	if creds.AccessKeyID != "" && creds.SecretAccessKey != "" {
		awsOpts = append(awsOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if creds.Endpoint != nil && *creds.Endpoint != "" {
		endpoint := *creds.Endpoint
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &endpoint
		})
	}
	if creds.ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	bucket := cfg.Container
	if bucket == "" {
		bucket = creds.BucketName
	}
	if bucket == "" {
		return nil, fmt.Errorf("S3 bucket name is required (set via Container config field or bucketName in credentials)")
	}

	tempDir := cfg.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	return &S3Snapstore{
		client:  s3.NewFromConfig(awsCfg, s3Opts...),
		bucket:  bucket,
		prefix:  cfg.Prefix,
		tempDir: tempDir,
	}, nil
}

// Save uploads the snapshot to S3 using multipart upload.
func (s *S3Snapstore) Save(snap Snapshot, r io.ReadCloser) error {
	defer r.Close() //nolint:errcheck

	// Buffer to temp file so we know the size for multipart chunking.
	tmp, err := os.CreateTemp(s.tempDir, "etcd-steward-s3-")
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

	key := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
	ctx := context.Background()

	// Use multipart upload for large objects; PutObject for small ones.
	if size <= s3MinChunkSize {
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   tmp,
		})
		if err != nil {
			return fmt.Errorf("failed to PutObject %s: %w", key, err)
		}
		return nil
	}

	// Initiate multipart upload.
	mpu, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload for %s: %w", key, err)
	}
	uploadID := mpu.UploadId

	chunkSize := int64(s3MinChunkSize)
	if n := size / 9999; n > chunkSize {
		chunkSize = n
	}

	var completedParts []s3types.CompletedPart
	partNumber := int32(1)
	offset := int64(0)

	for offset < size {
		end := offset + chunkSize
		if end > size {
			end = size
		}
		partSize := end - offset

		partReader := io.NewSectionReader(tmp, offset, partSize)
		partCtx, partCancel := context.WithTimeout(ctx, s3ChunkUploadTimeout)
		uploadResp, uploadErr := s.client.UploadPart(partCtx, &s3.UploadPartInput{
			Bucket:     aws.String(s.bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(partNumber),
			Body:       partReader,
		})
		partCancel()
		if uploadErr != nil {
			// Abort on failure.
			_, _ = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(s.bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			})
			return fmt.Errorf("failed to upload part %d of %s: %w", partNumber, key, uploadErr)
		}

		completedParts = append(completedParts, s3types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})

		offset = end
		partNumber++
	}

	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		_, _ = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
		})
		return fmt.Errorf("failed to complete multipart upload for %s: %w", key, err)
	}

	return nil
}

// Fetch returns a reader for the given snapshot from S3.
func (s *S3Snapstore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	key := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to GetObject %s: %w", key, err)
	}
	return resp.Body, nil
}

// List returns all snapshots in the S3 bucket under the configured prefix,
// sorted by LastRevision ascending.
func (s *S3Snapstore) List() ([]Snapshot, error) {
	prefix := s.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var snapshots []Snapshot

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects under prefix %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			// Strip the bucket prefix to get the relative path.
			rel := strings.TrimPrefix(key, prefix)
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

// Delete removes the given snapshot from S3.
func (s *S3Snapstore) Delete(snap Snapshot) error {
	key := path.Join(s.prefix, snap.SnapDir, snap.SnapName)
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete S3 object %s: %w", key, err)
	}
	return nil
}

// loadS3Credentials reads S3 credentials from environment or config.
func loadS3Credentials(cfg SnapstoreConfig) (s3Credentials, error) {
	var creds s3Credentials

	// Try JSON credentials file from env.
	if jsonPath := os.Getenv("ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS_JSON"); jsonPath != "" {
		data, err := os.ReadFile(jsonPath)
		if err != nil {
			return creds, fmt.Errorf("failed to read AWS credentials file %s: %w", jsonPath, err)
		}
		if err := json.Unmarshal(data, &creds); err != nil {
			return creds, fmt.Errorf("failed to parse AWS credentials file %s: %w", jsonPath, err)
		}
		if cfg.EndpointOverride != "" {
			creds.Endpoint = &cfg.EndpointOverride
		}
		return creds, nil
	}

	// Try credentials directory.
	if dir := os.Getenv("ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS"); dir != "" {
		if data, err := os.ReadFile(dir + "/region"); err == nil {
			creds.Region = strings.TrimSpace(string(data))
		}
		if data, err := os.ReadFile(dir + "/accessKeyID"); err == nil {
			creds.AccessKeyID = strings.TrimSpace(string(data))
		}
		if data, err := os.ReadFile(dir + "/secretAccessKey"); err == nil {
			creds.SecretAccessKey = strings.TrimSpace(string(data))
		}
		if data, err := os.ReadFile(dir + "/bucketName"); err == nil {
			creds.BucketName = strings.TrimSpace(string(data))
		}
	}

	if cfg.EndpointOverride != "" {
		creds.Endpoint = &cfg.EndpointOverride
	}

	return creds, nil
}
