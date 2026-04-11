// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package gc provides snapshot garbage collection for etcd-steward.
package gc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/metrics"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// Policy names for GC.
const (
	PolicyLimitBased   = "LimitBased"
	PolicyExponential  = "Exponential"
	PolicyTimeBased    = "TimeBased"
	PolicyCalendar     = "Calendar"
)

// Config holds all GC parameters. At least one retention policy must be set.
type Config struct {
	// Policy selects the retention algorithm.
	// One of "LimitBased", "Exponential", "TimeBased", "Calendar".
	Policy string
	// MaxFullSnapshots is the maximum number of full snapshot sets to retain (LimitBased).
	MaxFullSnapshots int
	// MaxRetentionDuration retains only snapshots newer than this age (TimeBased).
	MaxRetentionDuration time.Duration

	// Calendar-based retention: keep at least one snapshot per calendar period.
	// Zero values mean the policy is disabled for that granularity.
	HourlyRetention  int // number of distinct hours to keep
	DailyRetention   int // number of distinct days to keep
	WeeklyRetention  int // number of distinct weeks to keep
	MonthlyRetention int // number of distinct months to keep
}

// GarbageCollector deletes old snapshot sets from the snapstore based on retention policy.
type GarbageCollector struct {
	store     snapstore.Snapstore
	namespace string
	name      string
	cfg       Config
	logger    *zap.Logger
}

// New creates a GarbageCollector that retains at most maxFullSnapshots full snapshot sets.
// Deprecated: prefer NewWithConfig for full policy support.
func New(store snapstore.Snapstore, maxFullSnapshots int, logger *zap.Logger) *GarbageCollector {
	return NewWithConfig(store, "", "", Config{
		Policy:           PolicyLimitBased,
		MaxFullSnapshots: maxFullSnapshots,
	}, logger)
}

// NewWithConfig creates a GarbageCollector with the given Config.
func NewWithConfig(store snapstore.Snapstore, namespace, name string, cfg Config, logger *zap.Logger) *GarbageCollector {
	if cfg.Policy == "" {
		cfg.Policy = PolicyLimitBased
	}
	if cfg.MaxFullSnapshots == 0 && cfg.Policy == PolicyLimitBased {
		cfg.MaxFullSnapshots = 7
	}
	return &GarbageCollector{
		store:     store,
		namespace: namespace,
		name:      name,
		cfg:       cfg,
		logger:    logger,
	}
}

// Run runs garbage collection on the given period, skipping if isLeader returns false.
// Blocks until ctx is cancelled.
func (gc *GarbageCollector) Run(ctx context.Context, period time.Duration, isLeader func() bool) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !isLeader() {
				gc.logger.Debug("skipping GC, not leader")
				continue
			}
			if err := gc.Collect(ctx); err != nil {
				gc.logger.Error("garbage collection failed", zap.Error(err))
			}
		}
	}
}

// snapshotSet is a group of snapshots starting with a Full snapshot followed by its Incrementals.
type snapshotSet struct {
	full         snapstore.Snapshot
	incrementals []snapstore.Snapshot
}

// Collect performs a single garbage collection pass using the configured policy.
func (gc *GarbageCollector) Collect(ctx context.Context) error {
	switch gc.cfg.Policy {
	case PolicyTimeBased:
		return gc.collectTimeBased(ctx)
	case PolicyCalendar:
		return gc.collectCalendar(ctx)
	case PolicyExponential:
		return gc.collectExponential(ctx)
	default:
		return gc.collectLimitBased(ctx)
	}
}

// collectLimitBased retains the last maxFullSnapshots full snapshot sets.
func (gc *GarbageCollector) collectLimitBased(ctx context.Context) error {
	snaps, err := gc.store.List()
	if err != nil {
		return err
	}

	if len(snaps) == 0 {
		return nil
	}

	sets := groupIntoSets(snaps)
	if len(sets) <= gc.cfg.MaxFullSnapshots {
		return nil
	}

	// Delete sets older than the retained ones.
	deleteCount := len(sets) - gc.cfg.MaxFullSnapshots
	for i := 0; i < deleteCount; i++ {
		set := sets[i]
		gc.logger.Info("deleting snapshot set (LimitBased)",
			zap.Int64("fullRevision", set.full.LastRevision),
			zap.Int("incrementals", len(set.incrementals)),
		)
		gc.deleteSet(ctx, set, PolicyLimitBased)
	}

	return ctx.Err()
}

// collectTimeBased removes snapshots older than cfg.MaxRetentionDuration.
// The most recent snapshot is always retained regardless of age.
func (gc *GarbageCollector) collectTimeBased(ctx context.Context) error {
	if gc.cfg.MaxRetentionDuration <= 0 {
		return nil
	}

	snaps, err := gc.store.List()
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		return nil
	}

	sets := groupIntoSets(snaps)
	if len(sets) == 0 {
		return nil
	}

	cutoff := time.Now().UTC().Add(-gc.cfg.MaxRetentionDuration)

	// Always retain the latest set.
	for i := 0; i < len(sets)-1; i++ {
		if sets[i].full.CreatedOn.Before(cutoff) {
			gc.logger.Info("deleting snapshot set (TimeBased)",
				zap.Time("createdOn", sets[i].full.CreatedOn),
				zap.Duration("maxRetention", gc.cfg.MaxRetentionDuration),
			)
			gc.deleteSet(ctx, sets[i], PolicyTimeBased)
		}
	}

	return ctx.Err()
}

// collectCalendar retains a specified number of snapshots per calendar period
// (hourly, daily, weekly, monthly). For each period bucket, the newest snapshot
// in that bucket is kept; all others in the bucket are candidates for deletion.
// The most recent snapshot is always retained.
func (gc *GarbageCollector) collectCalendar(ctx context.Context) error {
	snaps, err := gc.store.List()
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		return nil
	}

	sets := groupIntoSets(snaps)
	if len(sets) <= 1 {
		return nil
	}

	// Build the set of full snapshot CreatedOn times (index → time).
	// Collect which set indices are retained per calendar rule.
	retain := make([]bool, len(sets))
	retain[len(sets)-1] = true // always keep the newest

	type bucketKey struct{ year, bucket int }

	keepNewestPerBucket := func(granularity string, bucketOf func(t time.Time) bucketKey, maxBuckets int) {
		if maxBuckets <= 0 {
			return
		}
		// Scan from newest to oldest, collecting the first (newest) set per bucket.
		seen := make(map[bucketKey]bool)
		keptBuckets := 0
		for i := len(sets) - 1; i >= 0; i-- {
			t := sets[i].full.CreatedOn
			k := bucketOf(t)
			if !seen[k] {
				seen[k] = true
				retain[i] = true
				keptBuckets++
				if keptBuckets >= maxBuckets {
					break
				}
			}
		}
		_ = granularity
	}

	// Hourly retention
	keepNewestPerBucket("hourly", func(t time.Time) bucketKey {
		return bucketKey{year: t.Year()*10000 + int(t.Month())*100 + t.Day(), bucket: t.Hour()}
	}, gc.cfg.HourlyRetention)

	// Daily retention
	keepNewestPerBucket("daily", func(t time.Time) bucketKey {
		return bucketKey{year: t.Year()*100 + int(t.Month()), bucket: t.Day()}
	}, gc.cfg.DailyRetention)

	// Weekly retention — use ISO week number
	keepNewestPerBucket("weekly", func(t time.Time) bucketKey {
		year, week := t.ISOWeek()
		return bucketKey{year: year, bucket: week}
	}, gc.cfg.WeeklyRetention)

	// Monthly retention
	keepNewestPerBucket("monthly", func(t time.Time) bucketKey {
		return bucketKey{year: t.Year(), bucket: int(t.Month())}
	}, gc.cfg.MonthlyRetention)

	// Delete sets not retained by any rule.
	for i, set := range sets {
		if retain[i] {
			continue
		}
		gc.logger.Info("deleting snapshot set (Calendar)",
			zap.Int64("fullRevision", set.full.LastRevision),
			zap.Time("createdOn", set.full.CreatedOn),
		)
		gc.deleteSet(ctx, set, PolicyCalendar)
	}

	return ctx.Err()
}

// collectExponential applies an exponential retention policy:
// keep all snapshots from the last hour, then 1/hour for the past 24h,
// then 1/day for the past 7 days, then 1/week for the past 4 weeks.
func (gc *GarbageCollector) collectExponential(ctx context.Context) error {
	snaps, err := gc.store.List()
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		return nil
	}

	sets := groupIntoSets(snaps)
	if len(sets) <= 1 {
		return nil
	}

	now := time.Now().UTC()
	retain := make([]bool, len(sets))
	retain[len(sets)-1] = true // always keep the newest

	type hourKey struct{ year, day, hour int }
	type dayKey struct{ year, day int }
	type weekKey struct{ year, week int }

	seenHour := make(map[hourKey]bool)
	seenDay := make(map[dayKey]bool)
	seenWeek := make(map[weekKey]bool)

	for i := len(sets) - 1; i >= 0; i-- {
		t := sets[i].full.CreatedOn
		age := now.Sub(t)
		switch {
		case age <= time.Hour:
			retain[i] = true
		case age <= 24*time.Hour:
			k := hourKey{t.Year(), t.YearDay(), t.Hour()}
			if !seenHour[k] {
				seenHour[k] = true
				retain[i] = true
			}
		case age <= 7*24*time.Hour:
			k := dayKey{t.Year(), t.YearDay()}
			if !seenDay[k] {
				seenDay[k] = true
				retain[i] = true
			}
		case age <= 4*7*24*time.Hour:
			year, week := t.ISOWeek()
			k := weekKey{year, week}
			if !seenWeek[k] {
				seenWeek[k] = true
				retain[i] = true
			}
		}
	}

	for i, set := range sets {
		if retain[i] {
			continue
		}
		gc.logger.Info("deleting snapshot set (Exponential)",
			zap.Int64("fullRevision", set.full.LastRevision),
			zap.Time("createdOn", set.full.CreatedOn),
		)
		gc.deleteSet(ctx, set, PolicyExponential)
	}

	return ctx.Err()
}

// deleteSet deletes all snapshots in the set and emits GC metrics.
func (gc *GarbageCollector) deleteSet(_ context.Context, set snapshotSet, policy string) {
	if set.full.SnapName != "" {
		if err := gc.store.Delete(set.full); err != nil {
			gc.logger.Error("failed to delete full snapshot",
				zap.String("path", set.full.Path()),
				zap.Error(err),
			)
		} else {
			metrics.GCSnapshotsDeletedTotal.WithLabelValues(gc.namespace, gc.name, "Full", policy).Inc()
		}
	}

	for _, inc := range set.incrementals {
		if err := gc.store.Delete(inc); err != nil {
			gc.logger.Error("failed to delete incremental snapshot",
				zap.String("path", inc.Path()),
				zap.Error(err),
			)
		} else {
			metrics.GCSnapshotsDeletedTotal.WithLabelValues(gc.namespace, gc.name, "Incremental", policy).Inc()
		}
	}
}

// groupIntoSets groups snapshots (sorted by LastRevision ascending) into sets.
// Each Full snapshot starts a new set. Incremental snapshots are appended to the
// most recent set. If there are Incrementals before the first Full, they form a
// set with a zero-value Full snapshot.
func groupIntoSets(snaps []snapstore.Snapshot) []snapshotSet {
	var sets []snapshotSet
	var current *snapshotSet

	for _, snap := range snaps {
		if snap.Kind == "Full" {
			if current != nil {
				sets = append(sets, *current)
			}
			current = &snapshotSet{full: snap}
		} else {
			if current == nil {
				// Incremental before any Full — create an orphan set.
				current = &snapshotSet{}
			}
			current.incrementals = append(current.incrementals, snap)
		}
	}

	if current != nil {
		sets = append(sets, *current)
	}

	return sets
}
