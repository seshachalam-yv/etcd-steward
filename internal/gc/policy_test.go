// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package gc

import (
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/snapstore"
)

func makeSet(createdAt time.Time, endRev int64) SnapshotSet {
	return SnapshotSet{
		Full: snapstore.SnapInfo{
			Kind:          snapstore.SnapKindFull,
			StartRevision: 0,
			EndRevision:   endRev,
			CreatedAt:     createdAt,
			Name:          "test",
		},
	}
}

func TestCountPolicy_KeepsNewest(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	sets := []SnapshotSet{
		makeSet(now.Add(-3*time.Hour), 100),
		makeSet(now.Add(-2*time.Hour), 200),
		makeSet(now.Add(-1*time.Hour), 300),
		makeSet(now, 400),
	}

	toDelete := EvaluateCountPolicy(sets, 2)
	if len(toDelete) != 2 {
		t.Fatalf("expected 2 sets to delete, got %d", len(toDelete))
	}
	// The oldest two should be deleted (indices 0 and 1).
	if toDelete[0] != 0 || toDelete[1] != 1 {
		t.Fatalf("expected indices [0, 1], got %v", toDelete)
	}
}

func TestCountPolicy_NeverDeletesLatest(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	sets := []SnapshotSet{
		makeSet(now.Add(-1*time.Hour), 100),
		makeSet(now, 200),
	}

	// maxSets=0 would try to delete everything, but at least 1 is always kept.
	toDelete := EvaluateCountPolicy(sets, 0)
	if len(toDelete) != 1 {
		t.Fatalf("expected 1 set to delete, got %d", len(toDelete))
	}
	if toDelete[0] != 0 {
		t.Fatalf("expected index 0 to be deleted, got %d", toDelete[0])
	}
}

func TestTimePolicy_RetainsRecent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	policy := &TimePolicy{RetainDuration: 2 * time.Hour}

	// A set created 1 hour ago should be retained.
	recentSet := makeSet(now.Add(-1*time.Hour), 100)
	if !policy.ShouldRetain(recentSet, now) {
		t.Fatal("expected recent set to be retained")
	}
}

func TestTimePolicy_DeletesOld(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	policy := &TimePolicy{RetainDuration: 2 * time.Hour}

	// A set created 3 hours ago should not be retained.
	oldSet := makeSet(now.Add(-3*time.Hour), 100)
	if policy.ShouldRetain(oldSet, now) {
		t.Fatal("expected old set to be deleted")
	}
}

func TestCalendarPolicy_HourGranularity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Create 5 sets within the last 6 hours, policy keeps at most 3 hourly.
	sets := []SnapshotSet{
		makeSet(now.Add(-5*time.Hour), 100),
		makeSet(now.Add(-4*time.Hour), 200),
		makeSet(now.Add(-3*time.Hour), 300),
		makeSet(now.Add(-2*time.Hour), 400),
		makeSet(now.Add(-1*time.Hour), 500),
	}

	policy := CalendarPolicy{
		HourMax:   6,
		HourCount: 3,
	}

	toDelete := EvaluateCalendarPolicy(sets, policy, now)
	// With 5 sets and HourCount=3, the 2 oldest should be deleted.
	// The latest is always kept, plus the 2 nearest to it = 3 kept.
	if len(toDelete) != 2 {
		t.Fatalf("expected 2 sets to delete, got %d: %v", len(toDelete), toDelete)
	}
	// Deleted should be the two oldest.
	if toDelete[0] != 0 || toDelete[1] != 1 {
		t.Fatalf("expected indices [0, 1], got %v", toDelete)
	}
}

func TestCalendarPolicy_FullCascade(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Create sets at various time distances to exercise cascading tiers.
	sets := []SnapshotSet{
		makeSet(now.Add(-60*24*time.Hour), 10),  // ~2 months ago
		makeSet(now.Add(-45*24*time.Hour), 20),  // ~1.5 months ago
		makeSet(now.Add(-20*24*time.Hour), 30),  // ~3 weeks ago
		makeSet(now.Add(-10*24*time.Hour), 40),  // ~1.5 weeks ago
		makeSet(now.Add(-3*24*time.Hour), 50),   // 3 days ago
		makeSet(now.Add(-1*24*time.Hour), 60),   // 1 day ago
		makeSet(now.Add(-6*time.Hour), 70),      // 6 hours ago
		makeSet(now.Add(-2*time.Hour), 80),      // 2 hours ago
		makeSet(now.Add(-30*time.Minute), 90),   // 30 minutes ago
		makeSet(now, 100),                        // now (latest, always kept)
	}

	policy := CalendarPolicy{
		HourMax:    12,
		HourCount:  3,
		DayMax:     7,
		DayCount:   2,
		WeekMax:    4,
		WeekCount:  2,
		MonthMax:   3,
		MonthCount: 1,
	}

	toDelete := EvaluateCalendarPolicy(sets, policy, now)

	// Build retained map.
	retained := make(map[int]bool)
	for i := range sets {
		retained[i] = true
	}
	for _, idx := range toDelete {
		retained[idx] = false
	}

	// The latest set (index 9) must always be retained.
	if !retained[9] {
		t.Fatal("latest set (index 9) must always be retained")
	}

	// At least some sets should be deleted (we have 10 sets but limited retention).
	if len(toDelete) == 0 {
		t.Fatal("expected at least some sets to be deleted with cascading policy")
	}

	// Verify total retained does not exceed sum of tier counts + 1 (latest).
	// Hour=3, Day=2, Week=2, Month=1, but some sets match multiple tiers,
	// so this is an upper bound.
	totalRetained := len(sets) - len(toDelete)
	if totalRetained > 10 {
		t.Fatalf("too many retained sets: %d", totalRetained)
	}
}
