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

	gcsstorage "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// mockGCSBucket implements gcsBucketAPI using an in-memory map.
type mockGCSBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMockGCSBucket() *mockGCSBucket {
	return &mockGCSBucket{objects: make(map[string][]byte)}
}

// mockGCSWriter collects written bytes and stores them in the mock on Close.
type mockGCSWriter struct {
	buf    bytes.Buffer
	mock   *mockGCSBucket
	object string
}

func (w *mockGCSWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *mockGCSWriter) Close() error {
	w.mock.mu.Lock()
	defer w.mock.mu.Unlock()
	w.mock.objects[w.object] = w.buf.Bytes()
	return nil
}

func (m *mockGCSBucket) NewWriter(_ context.Context, object string) io.WriteCloser {
	return &mockGCSWriter{mock: m, object: object}
}

func (m *mockGCSBucket) NewReader(_ context.Context, object string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[object]
	if !ok {
		return nil, fmt.Errorf("object %q not found", object)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockGCSBucket) Attrs(_ context.Context, object string) (*gcsstorage.ObjectAttrs, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[object]
	if !ok {
		return nil, fmt.Errorf("object %q not found", object)
	}
	return &gcsstorage.ObjectAttrs{
		Name: object,
		Size: int64(len(data)),
	}, nil
}

// mockGCSIterator iterates over a slice of ObjectAttrs.
type mockGCSIterator struct {
	attrs []*gcsstorage.ObjectAttrs
	idx   int
}

func (it *mockGCSIterator) Next() (*gcsstorage.ObjectAttrs, error) {
	if it.idx >= len(it.attrs) {
		return nil, iterator.Done
	}
	a := it.attrs[it.idx]
	it.idx++
	return a, nil
}

func (m *mockGCSBucket) Objects(_ context.Context, query *gcsstorage.Query) gcsObjectIterator {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := ""
	if query != nil {
		prefix = query.Prefix
	}
	var attrs []*gcsstorage.ObjectAttrs
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			attrs = append(attrs, &gcsstorage.ObjectAttrs{
				Name: key,
				Size: int64(len(data)),
			})
		}
	}
	return &mockGCSIterator{attrs: attrs}
}

func (m *mockGCSBucket) Delete(_ context.Context, object string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, object)
	return nil
}

func TestGCSUpload(t *testing.T) {
	mock := newMockGCSBucket()
	store := newGCSWithAPI(mock, "test-bucket", "backups")

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   100,
		CreatedAt:     now,
	}

	payload := []byte("gcs-snapshot-data")
	result, err := store.Upload(ctx, info, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if result.Name == "" {
		t.Fatal("expected non-empty name")
	}
	if result.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), result.Size)
	}
	if result.Prefix != "backups" {
		t.Fatalf("expected prefix 'backups', got %q", result.Prefix)
	}
}

func TestGCSDownload(t *testing.T) {
	mock := newMockGCSBucket()
	store := newGCSWithAPI(mock, "test-bucket", "backups")

	ctx := context.Background()
	payload := []byte("gcs-download-data")
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

func TestGCSList(t *testing.T) {
	mock := newMockGCSBucket()
	store := newGCSWithAPI(mock, "test-bucket", "backups")

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

func TestGCSDelete(t *testing.T) {
	mock := newMockGCSBucket()
	store := newGCSWithAPI(mock, "test-bucket", "backups")

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
