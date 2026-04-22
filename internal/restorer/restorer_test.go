// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package restorer

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.uber.org/zap"
)

func setupStore(t *testing.T, infos []snapstore.SnapInfo) *snapstore.LocalSnapStore {
	t.Helper()
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	ctx := context.Background()
	for _, info := range infos {
		_, err := store.Upload(ctx, info, bytes.NewReader([]byte("snapshot-data")))
		if err != nil {
			t.Fatalf("Upload: %v", err)
		}
	}
	return store
}

func TestFindLatestFullSnapshot(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(time.Second)},
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 200, CreatedAt: now.Add(2 * time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	snap, err := r.FindLatestFullSnapshot(context.Background())
	if err != nil {
		t.Fatalf("FindLatestFullSnapshot: %v", err)
	}
	if snap.EndRevision != 200 {
		t.Fatalf("expected EndRevision 200, got %d", snap.EndRevision)
	}
}

func TestFindLatestFullSnapshotNoFulls(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindDelta, StartRevision: 1, EndRevision: 50, CreatedAt: now},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	_, err := r.FindLatestFullSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected error when no full snapshots exist")
	}
	if !strings.Contains(err.Error(), "no full snapshots found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindDeltaSnapshots(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(time.Second)},
		{Kind: snapstore.SnapKindDelta, StartRevision: 151, EndRevision: 200, CreatedAt: now.Add(2 * time.Second)},
		{Kind: snapstore.SnapKindDelta, StartRevision: 201, EndRevision: 250, CreatedAt: now.Add(3 * time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	deltas, err := r.FindDeltaSnapshots(context.Background(), 150)
	if err != nil {
		t.Fatalf("FindDeltaSnapshots: %v", err)
	}
	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas after revision 150, got %d", len(deltas))
	}
	if deltas[0].StartRevision != 151 {
		t.Fatalf("expected first delta StartRevision 151, got %d", deltas[0].StartRevision)
	}
	if deltas[1].StartRevision != 201 {
		t.Fatalf("expected second delta StartRevision 201, got %d", deltas[1].StartRevision)
	}
}

func TestFindDeltaSnapshotsNoneMatch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	deltas, err := r.FindDeltaSnapshots(context.Background(), 200)
	if err != nil {
		t.Fatalf("FindDeltaSnapshots: %v", err)
	}
	if len(deltas) != 0 {
		t.Fatalf("expected 0 deltas after revision 200, got %d", len(deltas))
	}
}

func TestApplyDeltasStub(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	// ApplyDeltas is a stub that should return nil.
	if err := r.ApplyDeltas(context.Background(), nil); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}
}

func TestRestoreFullFailsNoSnapshots(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	err = r.RestoreFull(context.Background())
	if err == nil {
		t.Fatal("expected error when no snapshots exist")
	}
	if !strings.Contains(err.Error(), "no full snapshots found") {
		t.Fatalf("unexpected error: %v", err)
	}
}
