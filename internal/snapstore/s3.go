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

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gardener/etcd-steward/internal/errors"
)

// Compile-time assertion that S3SnapStore implements SnapStore.
var _ SnapStore = (*S3SnapStore)(nil)

// s3API is the subset of the S3 client API used by S3SnapStore.
// This enables mocking in tests.
type s3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// S3SnapStore stores snapshots in an AWS S3 bucket.
type S3SnapStore struct {
	client s3API
	bucket string
	prefix string
}

// NewS3 creates a new S3SnapStore.
// bucket is the S3 bucket name.
// prefix is the key prefix under which snapshots are stored.
// config may contain "region" and "endpoint" keys.
func NewS3(bucket, prefix string, config map[string]string) (*S3SnapStore, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region, ok := config["region"]; ok && region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, "failed to load AWS config", err)
	}

	var s3Opts []func(*s3.Options)
	if endpoint, ok := config["endpoint"]; ok && endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, s3Opts...)
	return newS3WithClient(client, bucket, prefix), nil
}

// newS3WithClient creates an S3SnapStore with an injected s3API implementation.
func newS3WithClient(client s3API, bucket, prefix string) *S3SnapStore {
	return &S3SnapStore{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}
}

// objectKey returns the full S3 object key for a snapshot name.
func (s *S3SnapStore) objectKey(name string) string {
	if s.prefix == "" {
		return name
	}
	return path.Join(s.prefix, name)
}

// nameFromKey extracts the snapshot name from a full object key.
func (s *S3SnapStore) nameFromKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return strings.TrimPrefix(key, s.prefix+"/")
}

// Upload stores snapshot data in S3 and returns the resulting metadata.
func (s *S3SnapStore) Upload(ctx context.Context, info SnapInfo, data io.Reader) (SnapInfo, error) {
	name := snapFileName(info)
	key := s.objectKey(name)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   data,
	})
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to upload snapshot %q to S3", name), err)
	}

	result := info
	result.Name = name
	result.Prefix = s.prefix
	return result, nil
}

// Download returns a reader for the snapshot identified by name from S3.
func (s *S3SnapStore) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	key := s.objectKey(name)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to download snapshot %q from S3", name), err)
	}
	return out.Body, nil
}

// GetInfo returns metadata for a single snapshot by listing it from S3.
func (s *S3SnapStore) GetInfo(ctx context.Context, name string) (SnapInfo, error) {
	key := s.objectKey(name)
	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(key),
	})
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to get info for snapshot %q from S3", name), err)
	}

	for _, obj := range out.Contents {
		if aws.ToString(obj.Key) == key {
			info, parseErr := parseSnapFileName(name)
			if parseErr != nil {
				return SnapInfo{}, parseErr
			}
			info.Size = aws.ToInt64(obj.Size)
			info.Prefix = s.prefix
			return info, nil
		}
	}

	return SnapInfo{}, errors.New(errors.ErrCodeNotFound, fmt.Sprintf("snapshot %q not found in S3", name))
}

// List returns all snapshots under the prefix, sorted by CreatedAt ascending.
func (s *S3SnapStore) List(ctx context.Context) ([]SnapInfo, error) {
	listPrefix := s.prefix
	if listPrefix != "" && !strings.HasSuffix(listPrefix, "/") {
		listPrefix += "/"
	}

	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(listPrefix),
	})
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeStorage, "failed to list snapshots from S3", err)
	}

	var snaps []SnapInfo
	for _, obj := range out.Contents {
		name := s.nameFromKey(aws.ToString(obj.Key))
		info, parseErr := parseSnapFileName(name)
		if parseErr != nil {
			// Skip files that don't match the expected naming convention.
			continue
		}
		info.Size = aws.ToInt64(obj.Size)
		info.Prefix = s.prefix
		snaps = append(snaps, info)
	}

	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].CreatedAt.Before(snaps[j].CreatedAt)
	})

	return snaps, nil
}

// Delete removes the snapshot identified by name from S3.
func (s *S3SnapStore) Delete(ctx context.Context, name string) error {
	key := s.objectKey(name)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return errors.Wrap(errors.ErrCodeStorage, fmt.Sprintf("failed to delete snapshot %q from S3", name), err)
	}
	return nil
}
