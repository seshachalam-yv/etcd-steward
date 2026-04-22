// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// mockS3Client implements s3API using an in-memory map.
type mockS3Client struct {
	mu      sync.Mutex
	objects map[string][]byte // key → data
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{objects: make(map[string][]byte)}
}

func (m *mockS3Client) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.objects[aws.ToString(params.Key)] = data
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := aws.ToString(params.Key)
	data, ok := m.objects[key]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockS3Client) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := aws.ToString(params.Prefix)
	var contents []s3types.Object
	for key, data := range m.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			size := int64(len(data))
			contents = append(contents, s3types.Object{
				Key:  aws.String(key),
				Size: &size,
			})
		}
	}
	return &s3.ListObjectsV2Output{
		Contents: contents,
	}, nil
}

func (m *mockS3Client) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, aws.ToString(params.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func TestS3Upload(t *testing.T) {
	mock := newMockS3Client()
	store := newS3WithClient(mock, "test-bucket", "backups")

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   100,
		CreatedAt:     now,
	}

	payload := []byte("s3-snapshot-data")
	result, err := store.Upload(ctx, info, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if result.Name == "" {
		t.Fatal("expected non-empty name")
	}
	if result.Kind != SnapKindFull {
		t.Fatalf("expected kind %q, got %q", SnapKindFull, result.Kind)
	}
	if result.Prefix != "backups" {
		t.Fatalf("expected prefix 'backups', got %q", result.Prefix)
	}
}

func TestS3Download(t *testing.T) {
	mock := newMockS3Client()
	store := newS3WithClient(mock, "test-bucket", "backups")

	ctx := context.Background()
	payload := []byte("s3-download-data")
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   50,
		CreatedAt:     time.Now().UTC().Truncate(time.Second),
	}

	result, err := store.Upload(ctx, info, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := store.Download(ctx, result.Name)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("expected %q, got %q", payload, got)
	}
}

func TestS3List(t *testing.T) {
	mock := newMockS3Client()
	store := newS3WithClient(mock, "test-bucket", "backups")

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	_, err := store.Upload(ctx, SnapInfo{
		Kind: SnapKindFull, StartRevision: 1, EndRevision: 100, CreatedAt: now,
	}, bytes.NewReader([]byte("full")))
	if err != nil {
		t.Fatalf("Upload full: %v", err)
	}

	_, err = store.Upload(ctx, SnapInfo{
		Kind: SnapKindDelta, StartRevision: 101, EndRevision: 200, CreatedAt: now.Add(time.Second),
	}, bytes.NewReader([]byte("delta")))
	if err != nil {
		t.Fatalf("Upload delta: %v", err)
	}

	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
	if !snaps[0].CreatedAt.Before(snaps[1].CreatedAt) {
		t.Fatal("expected snapshots sorted by CreatedAt ascending")
	}
}

func TestS3Delete(t *testing.T) {
	mock := newMockS3Client()
	store := newS3WithClient(mock, "test-bucket", "backups")

	ctx := context.Background()
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   10,
		CreatedAt:     time.Now().UTC().Truncate(time.Second),
	}

	result, err := store.Upload(ctx, info, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	if err := store.Delete(ctx, result.Name); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots after delete, got %d", len(snaps))
	}
}
