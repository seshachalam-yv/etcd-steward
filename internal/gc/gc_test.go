// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package gc

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.uber.org/zap"
)

// helper creates a LocalSnapStore in a temp dir and uploads the given snap infos.
func setupStore(t *testing.T, infos []snapstore.SnapInfo) *snapstore.LocalSnapStore {
	t.Helper()
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	ctx := context.Background()
	for _, info := range infos {
		_, err := store.Upload(ctx, info, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("Upload: %v", err)
		}
	}
	return store
}

func TestGroupSnapshotsBasic(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	snaps := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(1 * time.Second)},
		{Kind: snapstore.SnapKindDelta, StartRevision: 151, EndRevision: 200, CreatedAt: now.Add(2 * time.Second)},
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 200, CreatedAt: now.Add(3 * time.Second)},
		{Kind: snapstore.SnapKindDelta, StartRevision: 201, EndRevision: 250, CreatedAt: now.Add(4 * time.Second)},
	}

	sets := GroupSnapshots(snaps)
	if len(sets) != 2 {
		t.Fatalf("expected 2 sets, got %d", len(sets))
	}

	// First set: full at rev 100 + 2 deltas.
	if sets[0].Full.EndRevision != 100 {
		t.Fatalf("expected first set full EndRevision 100, got %d", sets[0].Full.EndRevision)
	}
	if len(sets[0].Deltas) != 2 {
		t.Fatalf("expected 2 deltas in first set, got %d", len(sets[0].Deltas))
	}

	// Second set: full at rev 200 + 1 delta.
	if sets[1].Full.EndRevision != 200 {
		t.Fatalf("expected second set full EndRevision 200, got %d", sets[1].Full.EndRevision)
	}
	if len(sets[1].Deltas) != 1 {
		t.Fatalf("expected 1 delta in second set, got %d", len(sets[1].Deltas))
	}
}

func TestGroupSnapshotsOrphanedDeltasIgnored(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	snaps := []snapstore.SnapInfo{
		// Orphaned delta (no preceding full).
		{Kind: snapstore.SnapKindDelta, StartRevision: 1, EndRevision: 50, CreatedAt: now},
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now.Add(time.Second)},
	}

	sets := GroupSnapshots(snaps)
	if len(sets) != 1 {
		t.Fatalf("expected 1 set, got %d", len(sets))
	}
	if len(sets[0].Deltas) != 0 {
		t.Fatalf("expected 0 deltas, got %d", len(sets[0].Deltas))
	}
}

func TestCollectDeletesOldSets(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(1 * time.Second)},
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 200, CreatedAt: now.Add(2 * time.Second)},
		{Kind: snapstore.SnapKindDelta, StartRevision: 201, EndRevision: 250, CreatedAt: now.Add(3 * time.Second)},
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 300, CreatedAt: now.Add(4 * time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()

	gc := New(store, 2, time.Hour, logger)
	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// Should have deleted the first set (full + 1 delta = 2 snaps).
	// Remaining: 2 sets (set2: full+delta, set3: full) = 3 snaps.
	if len(snaps) != 3 {
		t.Fatalf("expected 3 snapshots after GC, got %d", len(snaps))
	}

	// Verify oldest remaining is the second full.
	if snaps[0].EndRevision != 200 {
		t.Fatalf("expected oldest remaining EndRevision 200, got %d", snaps[0].EndRevision)
	}
}

func TestCollectNeverDeletesLastSet(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()

	// maxSets=0 would try to delete everything, but we always keep at least 1.
	gc := New(store, 0, time.Hour, logger)
	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// The single set (full + delta) must be retained.
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots (1 set retained), got %d", len(snaps))
	}
}

func TestCollectNoOpWhenUnderLimit(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 50, CreatedAt: now},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()

	gc := New(store, 5, time.Hour, logger)
	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
}

func TestCollectEmptyStore(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	logger := zap.NewNop()
	gc := New(store, 2, time.Hour, logger)

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect on empty store: %v", err)
	}
}
