// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/snapstore"

	"go.uber.org/zap"
)

// mockSnapStore implements snapstore.SnapStore for testing.
type mockSnapStore struct {
	snaps []snapstore.SnapInfo
	err   error
}

func (m *mockSnapStore) Upload(_ context.Context, info snapstore.SnapInfo, _ io.Reader) (snapstore.SnapInfo, error) {
	return info, nil
}

func (m *mockSnapStore) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockSnapStore) GetInfo(_ context.Context, _ string) (snapstore.SnapInfo, error) {
	return snapstore.SnapInfo{}, fmt.Errorf("not implemented")
}

func (m *mockSnapStore) List(_ context.Context) ([]snapstore.SnapInfo, error) {
	return m.snaps, m.err
}

func (m *mockSnapStore) Delete(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}

func TestSnapshotInfoProvider_WithSnapshots(t *testing.T) {
	store := &mockSnapStore{
		snaps: []snapstore.SnapInfo{
			{
				Kind:          snapstore.SnapKindFull,
				StartRevision: 1,
				EndRevision:   100,
				CreatedAt:     time.Now().Add(-2 * time.Hour),
				Size:          5000,
			},
			{
				Kind:          snapstore.SnapKindDelta,
				StartRevision: 101,
				EndRevision:   200,
				CreatedAt:     time.Now().Add(-1 * time.Hour),
				Size:          1200,
			},
			{
				Kind:          snapstore.SnapKindDelta,
				StartRevision: 201,
				EndRevision:   300,
				CreatedAt:     time.Now(),
				Size:          800,
			},
		},
	}
	logger := zap.NewNop()
	p := NewSnapshotInfoProvider(store, logger)

	info, err := p.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v, ok := info["lastFullSnapshotRevision"].(int64); !ok || v != 100 {
		t.Fatalf("expected lastFullSnapshotRevision=100, got %v", info["lastFullSnapshotRevision"])
	}
	if v, ok := info["lastDeltaSnapshotRevision"].(int64); !ok || v != 300 {
		t.Fatalf("expected lastDeltaSnapshotRevision=300, got %v", info["lastDeltaSnapshotRevision"])
	}
	if v, ok := info["totalSnapshotCount"].(int); !ok || v != 3 {
		t.Fatalf("expected totalSnapshotCount=3, got %v", info["totalSnapshotCount"])
	}
	if v, ok := info["accumulatedDeltaSize"].(int64); !ok || v != 2000 {
		t.Fatalf("expected accumulatedDeltaSize=2000, got %v", info["accumulatedDeltaSize"])
	}
}

func TestSnapshotInfoProvider_EmptyStore(t *testing.T) {
	store := &mockSnapStore{snaps: nil}
	logger := zap.NewNop()
	p := NewSnapshotInfoProvider(store, logger)

	info, err := p.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v, ok := info["lastFullSnapshotRevision"].(int64); !ok || v != 0 {
		t.Fatalf("expected lastFullSnapshotRevision=0, got %v", info["lastFullSnapshotRevision"])
	}
	if v, ok := info["lastDeltaSnapshotRevision"].(int64); !ok || v != 0 {
		t.Fatalf("expected lastDeltaSnapshotRevision=0, got %v", info["lastDeltaSnapshotRevision"])
	}
	if v, ok := info["totalSnapshotCount"].(int); !ok || v != 0 {
		t.Fatalf("expected totalSnapshotCount=0, got %v", info["totalSnapshotCount"])
	}
	if v, ok := info["accumulatedDeltaSize"].(int64); !ok || v != 0 {
		t.Fatalf("expected accumulatedDeltaSize=0, got %v", info["accumulatedDeltaSize"])
	}
}

func TestSnapshotInfoProvider_ID(t *testing.T) {
	store := &mockSnapStore{}
	logger := zap.NewNop()
	p := NewSnapshotInfoProvider(store, logger)

	if got := p.ID(); got != "snapshot-info" {
		t.Fatalf("expected ID %q, got %q", "snapshot-info", got)
	}
}
