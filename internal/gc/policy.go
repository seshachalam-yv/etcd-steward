// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package gc

import (
	"time"
)

// RetentionPolicy determines whether a given SnapshotSet should be retained.
type RetentionPolicy interface {
	// ShouldRetain returns true if the given snapshot set should be kept.
	ShouldRetain(set SnapshotSet, now time.Time) bool
}

// CountPolicy retains the newest MaxSets snapshot sets.
type CountPolicy struct {
	// MaxSets is the maximum number of sets to retain.
	MaxSets int

	// rank is set internally during evaluation to track position from newest.
	rank int
	// total is the total number of sets being evaluated.
	total int
}

// ShouldRetain returns true if the set is among the newest MaxSets sets.
// The caller must set rank/total before calling; use EvaluateCountPolicy
// instead for correct usage.
func (p *CountPolicy) ShouldRetain(set SnapshotSet, _ time.Time) bool {
	// Always retain at least 1 set (the latest).
	maxSets := p.MaxSets
	if maxSets < 1 {
		maxSets = 1
	}
	// Retain if within the last maxSets from the end (newest).
	return p.rank >= p.total-maxSets
}

// EvaluateCountPolicy evaluates a list of sets (oldest-first) and returns
// the indices of sets to delete.
func EvaluateCountPolicy(sets []SnapshotSet, maxSets int) []int {
	if maxSets < 1 {
		maxSets = 1
	}
	if len(sets) <= maxSets {
		return nil
	}

	toDelete := len(sets) - maxSets
	// Always keep at least 1 set (the latest).
	if toDelete >= len(sets) {
		toDelete = len(sets) - 1
	}

	indices := make([]int, 0, toDelete)
	for i := 0; i < toDelete; i++ {
		indices = append(indices, i)
	}
	return indices
}

// TimePolicy retains sets created within RetainDuration from now.
type TimePolicy struct {
	// RetainDuration is the maximum age of snapshot sets to retain.
	RetainDuration time.Duration
}

// ShouldRetain returns true if the set's full snapshot was created within
// RetainDuration of now.
func (p *TimePolicy) ShouldRetain(set SnapshotSet, now time.Time) bool {
	cutoff := now.Add(-p.RetainDuration)
	return !set.Full.CreatedAt.Before(cutoff)
}

// CalendarPolicy implements a cascading retention strategy that keeps a
// specified number of snapshots at each time granularity:
//   - HourCount sets within the last HourMax hours
//   - DayCount sets within the last DayMax days
//   - WeekCount sets within the last WeekMax weeks
//   - MonthCount sets within the last MonthMax months
//
// A set that matches any tier is retained.
type CalendarPolicy struct {
	// HourMax is the number of hours to look back for hourly retention.
	HourMax int
	// HourCount is the maximum sets to keep within HourMax hours.
	HourCount int
	// DayMax is the number of days to look back for daily retention.
	DayMax int
	// DayCount is the maximum sets to keep within DayMax days.
	DayCount int
	// WeekMax is the number of weeks to look back for weekly retention.
	WeekMax int
	// WeekCount is the maximum sets to keep within WeekMax weeks.
	WeekCount int
	// MonthMax is the number of months to look back for monthly retention.
	MonthMax int
	// MonthCount is the maximum sets to keep within MonthMax months.
	MonthCount int
}

// EvaluateCalendarPolicy evaluates sets (oldest-first) and returns the indices
// of sets to delete according to the cascading calendar policy.
// The latest set is never deleted.
func EvaluateCalendarPolicy(sets []SnapshotSet, p CalendarPolicy, now time.Time) []int {
	if len(sets) == 0 {
		return nil
	}

	retained := make([]bool, len(sets))
	// Always retain the latest set.
	retained[len(sets)-1] = true

	// Process tiers from finest to coarsest. For each tier, walk sets from
	// newest to oldest, keeping at most tierCount sets whose full snapshot
	// falls within the tier window.
	tiers := []struct {
		window time.Duration
		count  int
	}{
		{time.Duration(p.HourMax) * time.Hour, p.HourCount},
		{time.Duration(p.DayMax) * 24 * time.Hour, p.DayCount},
		{time.Duration(p.WeekMax) * 7 * 24 * time.Hour, p.WeekCount},
		{time.Duration(p.MonthMax) * 30 * 24 * time.Hour, p.MonthCount},
	}

	for _, tier := range tiers {
		if tier.count <= 0 {
			continue
		}
		cutoff := now.Add(-tier.window)
		kept := 0
		// Walk from newest to oldest.
		for i := len(sets) - 1; i >= 0; i-- {
			if sets[i].Full.CreatedAt.Before(cutoff) {
				continue
			}
			if kept < tier.count {
				retained[i] = true
				kept++
			}
		}
	}

	var toDelete []int
	for i := range sets {
		if !retained[i] {
			toDelete = append(toDelete, i)
		}
	}
	return toDelete
}

// ShouldRetain is a convenience method that always returns true. Calendar
// policy must be evaluated as a batch via EvaluateCalendarPolicy since
// retention depends on the position of a set relative to all other sets.
// This method exists to satisfy the RetentionPolicy interface.
func (p *CalendarPolicy) ShouldRetain(_ SnapshotSet, _ time.Time) bool {
	return true
}
