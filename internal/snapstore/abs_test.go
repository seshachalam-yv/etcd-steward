// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockABSAPI implements absAPI using an in-memory map.
type mockABSAPI struct {
	mu      sync.Mutex
	objects map[string][]byte // "container/blob" → data
}

func newMockABSAPI() *mockABSAPI {
	return &mockABSAPI{objects: make(map[string][]byte)}
}

func (m *mockABSAPI) key(container, blob string) string {
	return container + "/" + blob
}

func (m *mockABSAPI) UploadStream(_ context.Context, container, blob string, body io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	m.objects[m.key(container, blob)] = data
	return nil
}

func (m *mockABSAPI) DownloadStream(_ context.Context, container, blob string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[m.key(container, blob)]
	if !ok {
		return nil, fmt.Errorf("blob %q not found in container %q", blob, container)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockABSAPI) ListBlobs(_ context.Context, container string, prefix string) ([]absBlobItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	containerPrefix := container + "/"
	var items []absBlobItem
	for k, data := range m.objects {
		if !strings.HasPrefix(k, containerPrefix) {
			continue
		}
		blob := strings.TrimPrefix(k, containerPrefix)
		if strings.HasPrefix(blob, prefix) {
			items = append(items, absBlobItem{
				Name:          blob,
				ContentLength: int64(len(data)),
			})
		}
	}
	return items, nil
}

func (m *mockABSAPI) DeleteBlob(_ context.Context, container, blob string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, m.key(container, blob))
	return nil
}

func TestABSUpload(t *testing.T) {
	mock := newMockABSAPI()
	store := newABSWithAPI(mock, "test-container", "backups")

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   100,
		CreatedAt:     now,
	}

	payload := []byte("abs-snapshot-data")
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

func TestABSDownload(t *testing.T) {
	mock := newMockABSAPI()
	store := newABSWithAPI(mock, "test-container", "backups")

	ctx := context.Background()
	payload := []byte("abs-download-data")
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

func TestABSList(t *testing.T) {
	mock := newMockABSAPI()
	store := newABSWithAPI(mock, "test-container", "backups")

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

func TestABSDelete(t *testing.T) {
	mock := newMockABSAPI()
	store := newABSWithAPI(mock, "test-container", "backups")

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
