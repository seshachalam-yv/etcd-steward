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
			snap("Full", 0, 100),          // set 0
			snap("Incremental", 100, 150), // set 0
			snap("Full", 0, 200),          // set 1
			snap("Incremental", 200, 250), // set 1
			snap("Full", 0, 300),          // set 2
			snap("Incremental", 300, 350), // set 2
			snap("Full", 0, 400),          // set 3
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

// errSnapstore is a mockSnapstore variant whose List always returns an error.
type errSnapstore struct{}

func (e *errSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (e *errSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (e *errSnapstore) List() ([]snapstore.Snapshot, error) {
	return nil, fmt.Errorf("list error")
}
func (e *errSnapstore) Delete(_ snapstore.Snapshot) error { return nil }

func TestCollect_ListError(t *testing.T) {
	gc := New(&errSnapstore{}, 3, zap.NewNop())
	err := gc.Collect(context.Background())
	if err == nil {
		t.Fatal("expected error from List, got nil")
	}
}

// deleteErrSnapstore records deletes but returns an error for every Delete call.
type deleteErrSnapstore struct {
	snaps   []snapstore.Snapshot
	deleted []snapstore.Snapshot
}

func (d *deleteErrSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (d *deleteErrSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (d *deleteErrSnapstore) List() ([]snapstore.Snapshot, error) { return d.snaps, nil }
func (d *deleteErrSnapstore) Delete(snap snapstore.Snapshot) error {
	d.deleted = append(d.deleted, snap)
	return fmt.Errorf("delete error for %s", snap.SnapName)
}

func TestCollect_DeleteFullError_ContinuesWithIncrementals(t *testing.T) {
	// 3 sets, keep 1 → delete 2. Errors on delete should be logged and execution continues.
	store := &deleteErrSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),
			snap("Incremental", 100, 150),
			snap("Full", 0, 200),
			snap("Incremental", 200, 250),
			snap("Full", 0, 300),
		},
	}
	gc := New(store, 1, zap.NewNop())
	// Collect should not return error even when individual deletes fail.
	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("expected no error from Collect when Delete fails, got: %v", err)
	}
	// All 4 deletes should have been attempted (Full@100, Incr@150, Full@200, Incr@250).
	if len(store.deleted) != 4 {
		t.Errorf("expected 4 delete attempts, got %d", len(store.deleted))
	}
}

func TestCollect_ExactlyMaxPlusOne_DeletesOneSet(t *testing.T) {
	// maxFullSnapshots=2, 3 sets → delete exactly 1 oldest set.
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),
			snap("Incremental", 100, 150),
			snap("Full", 0, 200),
			snap("Incremental", 200, 250),
			snap("Full", 0, 300),
		},
	}
	gc := New(store, 2, zap.NewNop())
	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}
	// Set 0: Full@100 + Incr@150 → 2 deletes
	if len(store.deleted) != 2 {
		t.Fatalf("expected 2 deletes, got %d", len(store.deleted))
	}
	for _, d := range store.deleted {
		if d.LastRevision != 100 && d.LastRevision != 150 {
			t.Errorf("unexpected revision deleted: %d", d.LastRevision)
		}
	}
}

func TestCollect_ContextCancelledAfterCollection(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),
			snap("Full", 0, 200),
			snap("Full", 0, 300),
			snap("Full", 0, 400),
		},
	}
	gc := New(store, 2, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so ctx.Err() is set before Collect checks it

	err := gc.Collect(ctx)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestCollect_OrphanIncrementalsWithinLimit(t *testing.T) {
	// 2 sets (orphan + Full@100), keep 2 → no deletions
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Incremental", 50, 80),
			snap("Full", 0, 100),
		},
	}
	gc := New(store, 2, zap.NewNop())
	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}
	if len(store.deleted) != 0 {
		t.Errorf("expected 0 deletes when within limit, got %d", len(store.deleted))
	}
}

func TestRun_SkipsCollectWhenNotLeader(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),
			snap("Full", 0, 200),
			snap("Full", 0, 300),
			snap("Full", 0, 400),
		},
	}
	gc := New(store, 1, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	gc.Run(ctx, 10*time.Millisecond, func() bool { return false })

	if len(store.deleted) != 0 {
		t.Errorf("expected no deletes when not leader, got %d", len(store.deleted))
	}
}

func TestRun_CallsCollectWhenLeader(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snap("Full", 0, 100),
			snap("Full", 0, 200),
			snap("Full", 0, 300),
			snap("Full", 0, 400),
		},
	}
	gc := New(store, 1, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// isLeader returns true: collection should run and delete old sets.
	gc.Run(ctx, 10*time.Millisecond, func() bool { return true })

	if len(store.deleted) == 0 {
		t.Error("expected deletes when leader, got none")
	}
}

func TestRun_ExitsOnContextCancel(t *testing.T) {
	store := &mockSnapstore{}
	gc := New(store, 3, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		gc.Run(ctx, 10*time.Millisecond, func() bool { return false })
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// ok, Run returned after cancel
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// snapWithAge creates a snapshot with a CreatedOn time offset from now.
func snapWithAge(kind string, startRev, lastRev int64, age time.Duration) snapstore.Snapshot {
	s := snap(kind, startRev, lastRev)
	s.CreatedOn = time.Now().UTC().Add(-age)
	return s
}

func TestCollect_TimeBased_DeletesOldSets(t *testing.T) {
	// Two old sets (>2h) and one recent set (<2h).
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snapWithAge("Full", 0, 100, 3*time.Hour),         // old — should be deleted
			snapWithAge("Incremental", 100, 150, 3*time.Hour), // old
			snapWithAge("Full", 0, 200, 2*time.Hour+1*time.Minute), // old
			snapWithAge("Full", 0, 300, 30*time.Minute),      // recent — keep (also newest, never deleted)
		},
	}
	gc := NewWithConfig(store, "default", "etcd-main", Config{
		Policy:               PolicyTimeBased,
		MaxRetentionDuration: 2 * time.Hour,
	}, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}

	// Oldest two sets deleted (Full@100 + Incr@150, Full@200).
	if len(store.deleted) != 3 {
		t.Fatalf("expected 3 deletes, got %d", len(store.deleted))
	}

	// Newest (Full@300) must not be deleted.
	for _, d := range store.deleted {
		if d.LastRevision == 300 {
			t.Error("newest snapshot (rev 300) should not be deleted by TimeBased GC")
		}
	}
}

func TestCollect_TimeBased_KeepsAllWhenNotExpired(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snapWithAge("Full", 0, 100, 30*time.Minute),
			snapWithAge("Full", 0, 200, 10*time.Minute),
		},
	}
	gc := NewWithConfig(store, "default", "etcd-main", Config{
		Policy:               PolicyTimeBased,
		MaxRetentionDuration: 1 * time.Hour,
	}, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}
	if len(store.deleted) != 0 {
		t.Errorf("expected 0 deletes for unexpired snapshots, got %d", len(store.deleted))
	}
}

func TestCollect_Calendar_DailyRetention(t *testing.T) {
	now := time.Now().UTC()
	// Create 10 sets on 10 different days. Calendar policy with daily=3 should keep 3 days.
	snaps := make([]snapstore.Snapshot, 10)
	for i := 0; i < 10; i++ {
		rev := int64((i + 1) * 100)
		s := snapWithAge("Full", 0, rev, time.Duration(10-i)*24*time.Hour)
		s.CreatedOn = s.CreatedOn.Truncate(24 * time.Hour).Add(time.Duration(i) * time.Hour) // distinct hours within distinct days
		_ = now
		snaps[i] = s
	}
	store := &mockSnapstore{snaps: snaps}

	gc := NewWithConfig(store, "default", "etcd-main", Config{
		Policy:          PolicyCalendar,
		DailyRetention:  3,
	}, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}

	// With 10 sets on 10 different days and daily=3, at least 7 should be deleted.
	if len(store.deleted) < 7 {
		t.Errorf("expected >= 7 deletes with daily=3, got %d", len(store.deleted))
	}

	// Most recent set must never be deleted.
	newestRev := int64(1000)
	for _, d := range store.deleted {
		if d.LastRevision == newestRev {
			t.Errorf("newest snapshot (rev %d) should not be deleted by Calendar GC", newestRev)
		}
	}
}

func TestCollect_Exponential_KeepsRecentAndOld(t *testing.T) {
	// Create sets at various ages: <1h (many), <24h (same hour bucket), <7d, older.
	// The exponential policy scans newest-to-oldest by LastRevision (list is asc-sorted).
	// For the 24h window: rev 110 has higher LastRevision so it wins its hour bucket;
	// rev 100 (same hour, lower LastRevision) is discarded.
	now := time.Now().UTC()
	inSameHour1 := now.Add(-3*time.Hour - 10*time.Minute) // assigned to rev 100
	inSameHour2 := now.Add(-3*time.Hour - 20*time.Minute) // assigned to rev 110 (higher rev wins)

	snap100 := snap("Full", 0, 100)
	snap100.CreatedOn = inSameHour1 // same hour as snap110 but lower LastRevision
	snap110 := snap("Full", 0, 110)
	snap110.CreatedOn = inSameHour2 // same hour; rev 110 > rev 100 → wins bucket

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			snapWithAge("Full", 0, 10, 30*time.Minute),     // <1h — keep
			snapWithAge("Full", 0, 20, 20*time.Minute),     // <1h — keep (newest overall)
			snap100,                                          // 24h window, same hour as snap110
			snap110,                                          // 24h window, wins hour bucket (higher rev)
			snapWithAge("Full", 0, 200, 48*time.Hour),      // day window
			snapWithAge("Full", 0, 300, 6*24*time.Hour),    // day window
			snapWithAge("Full", 0, 1000, 8*7*24*time.Hour), // >4w — too old
		},
	}
	gc := NewWithConfig(store, "default", "etcd-main", Config{
		Policy: PolicyExponential,
	}, zap.NewNop())

	if err := gc.Collect(context.Background()); err != nil {
		t.Fatalf("Collect error: %v", err)
	}

	// Newest set (rev 20) — never deleted.
	for _, d := range store.deleted {
		if d.LastRevision == 20 {
			t.Error("newest snapshot (rev 20) should not be deleted by Exponential GC")
		}
	}

	// Rev 110 wins the hour bucket (higher revision); rev 100 should be discarded.
	found100 := false
	for _, d := range store.deleted {
		if d.LastRevision == 100 {
			found100 = true
		}
	}
	if !found100 {
		t.Error("same-hour lower-revision snapshot (rev 100) should have been deleted; rev 110 wins the bucket")
	}
}

func TestNewWithConfig_DefaultsLimitBased(t *testing.T) {
	store := &mockSnapstore{}
	gc := NewWithConfig(store, "default", "etcd", Config{}, zap.NewNop())
	if gc == nil {
		t.Fatal("expected non-nil GarbageCollector")
	}
	// Default policy should be LimitBased with maxFullSnapshots=7.
	if gc.cfg.Policy != PolicyLimitBased {
		t.Errorf("expected default policy %q, got %q", PolicyLimitBased, gc.cfg.Policy)
	}
	if gc.cfg.MaxFullSnapshots != 7 {
		t.Errorf("expected default MaxFullSnapshots=7, got %d", gc.cfg.MaxFullSnapshots)
	}
}
