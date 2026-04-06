// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package gc

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// mockSnapstore records deletes for testing.
type mockSnapstore struct {
	snaps   []snapstore.Snapshot
	deleted []snapstore.Snapshot
}

func (m *mockSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (m *mockSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockSnapstore) List() ([]snapstore.Snapshot, error) {
	return m.snaps, nil
}
func (m *mockSnapstore) Delete(snap snapstore.Snapshot) error {
	m.deleted = append(m.deleted, snap)
	return nil
}

func snap(kind string, startRev, lastRev int64) snapstore.Snapshot {
	return snapstore.Snapshot{
		Kind:          kind,
		StartRevision: startRev,
		LastRevision:  lastRev,
		CreatedOn:     time.Unix(0, lastRev*1e9).UTC(),
		SnapDir:       "backups",
		SnapName:      fmt.Sprintf("%s-%016d-%016d-%d", kind, startRev, lastRev, lastRev*1e9),
	}
}

func TestCollect_NoSnapshots(t *testing.T) {
	store := &mockSnapstore{}
	gc := New(store, 3, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(store.deleted) != 0 {
		t.Errorf("expected 0 deletes, got %d", len(store.deleted))
	}
}

func TestCollect_WithinRetention(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),
			snap("Incremental", 100, 150),
			snap("Full", 0, 200),
		},
	}
	gc := New(store, 3, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(store.deleted) != 0 {
		t.Errorf("expected 0 deletes (within retention), got %d", len(store.deleted))
	}
}

func TestCollect_DeletesOldestSets(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),       // set 0
			snap("Incremental", 100, 150), // set 0
			snap("Full", 0, 200),       // set 1
			snap("Incremental", 200, 250), // set 1
			snap("Full", 0, 300),       // set 2
			snap("Incremental", 300, 350), // set 2
			snap("Full", 0, 400),       // set 3
		},
	}
	gc := New(store, 2, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}

	// 4 sets, keep 2 → delete 2 oldest sets.
	// Set 0: Full@100, Incr@150 → 2 deletes
	// Set 1: Full@200, Incr@250 → 2 deletes
	// Total: 4 deletes
	if len(store.deleted) != 4 {
		t.Fatalf("expected 4 deletes, got %d", len(store.deleted))
	}

	// Verify deleted revisions.
	deletedRevisions := make(map[int64]bool)
	for _, d := range store.deleted {
		deletedRevisions[d.LastRevision] = true
	}
	for _, rev := range []int64{100, 150, 200, 250} {
		if !deletedRevisions[rev] {
			t.Errorf("expected revision %d to be deleted", rev)
		}
	}
	// Verify latest sets are preserved.
	for _, rev := range []int64{300, 350, 400} {
		if deletedRevisions[rev] {
			t.Errorf("revision %d should be retained, but was deleted", rev)
		}
	}
}

func TestCollect_LatestSetNeverDeleted(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),
			snap("Full", 0, 200),
			snap("Full", 0, 300),
		},
	}
	gc := New(store, 1, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}

	// 3 sets, keep 1 → delete 2.
	if len(store.deleted) != 2 {
		t.Fatalf("expected 2 deletes, got %d", len(store.deleted))
	}

	for _, d := range store.deleted {
		if d.LastRevision == 300 {
			t.Error("latest set (rev 300) should never be deleted")
		}
	}
}

func TestCollect_IncrementalsWithoutFull(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Incremental", 50, 80), // orphan before first Full
			snap("Full", 0, 100),
			snap("Full", 0, 200),
		},
	}
	gc := New(store, 2, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}

	// 3 sets (orphan, Full@100, Full@200), keep 2 → delete 1 orphan set.
	if len(store.deleted) != 1 {
		t.Fatalf("expected 1 delete, got %d", len(store.deleted))
	}
	if store.deleted[0].LastRevision != 80 {
		t.Errorf("expected orphan incremental (rev 80) deleted, got rev %d", store.deleted[0].LastRevision)
	}
}

func TestGroupIntoSets(t *testing.T) {
	tests := []struct {
		name     string
		snaps    []snapstore.Snapshot
		wantSets int
	}{
		{
			name:     "empty",
			snaps:    nil,
			wantSets: 0,
		},
		{
			name: "single full",
			snaps: []snapstore.Snapshot{
				snap("Full", 0, 100),
			},
			wantSets: 1,
		},
		{
			name: "full with incrementals",
			snaps: []snapstore.Snapshot{
				snap("Full", 0, 100),
				snap("Incremental", 100, 150),
				snap("Incremental", 150, 200),
			},
			wantSets: 1,
		},
		{
			name: "two full sets",
			snaps: []snapstore.Snapshot{
				snap("Full", 0, 100),
				snap("Incremental", 100, 150),
				snap("Full", 0, 200),
				snap("Incremental", 200, 250),
			},
			wantSets: 2,
		},
		{
			name: "orphan incrementals before first full",
			snaps: []snapstore.Snapshot{
				snap("Incremental", 50, 80),
				snap("Full", 0, 100),
			},
			wantSets: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sets := groupIntoSets(tc.snaps)
			if len(sets) != tc.wantSets {
				t.Errorf("expected %d sets, got %d", tc.wantSets, len(sets))
			}
		})
	}
}
