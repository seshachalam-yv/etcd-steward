// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package gc

import (
	"context"
	"sort"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.uber.org/zap"
)

// SnapshotSet groups a full snapshot together with the delta snapshots that
// follow it (up to but not including the next full snapshot).
type SnapshotSet struct {
	// Full is the base full snapshot for this set.
	Full snapstore.SnapInfo
	// Deltas contains the delta snapshots that belong to this set, ordered by
	// CreatedAt ascending.
	Deltas []snapstore.SnapInfo
}

// GarbageCollector periodically removes old snapshot sets, keeping at most
// maxSets sets. The latest set is never deleted.
// When a RetentionPolicy is configured it is used instead of the simple
// maxSets count.
type GarbageCollector struct {
	store   snapstore.SnapStore
	maxSets int
	period  time.Duration
	policy  RetentionPolicy
	logger  *zap.Logger
}

// New creates a new GarbageCollector.
func New(store snapstore.SnapStore, maxSets int, period time.Duration, logger *zap.Logger) *GarbageCollector {
	return &GarbageCollector{
		store:   store,
		maxSets: maxSets,
		period:  period,
		logger:  logger,
	}
}

// NewWithPolicy creates a GarbageCollector that uses the given RetentionPolicy
// to decide which snapshot sets to delete.
func NewWithPolicy(store snapstore.SnapStore, policy RetentionPolicy, period time.Duration, logger *zap.Logger) *GarbageCollector {
	return &GarbageCollector{
		store:  store,
		policy: policy,
		period: period,
		logger: logger,
	}
}

// Run periodically calls Collect until the context is cancelled.
func (g *GarbageCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(g.period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			g.logger.Info("garbage collector context cancelled, stopping")
			return
		case <-ticker.C:
			if err := g.Collect(ctx); err != nil {
				g.logger.Error("garbage collection failed", zap.Error(err))
			}
		}
	}
}

// Collect lists all snapshots, groups them into sets, and deletes the oldest
// sets that exceed maxSets. The latest set is never deleted (minimum 1 is
// always retained). When a RetentionPolicy is configured, it is used to
// determine which sets to remove.
func (g *GarbageCollector) Collect(ctx context.Context) error {
	snaps, err := g.store.List(ctx)
	if err != nil {
		return errors.Wrap(errors.ErrCodeSnapshot, "listing snapshots for garbage collection", err)
	}

	sets := GroupSnapshots(snaps)

	deleteIndices := g.computeDeleteIndices(sets)
	if len(deleteIndices) == 0 {
		g.logger.Info("no sets to garbage collect",
			zap.Int("totalSets", len(sets)),
		)
		return nil
	}

	deleteSet := make(map[int]bool, len(deleteIndices))
	for _, idx := range deleteIndices {
		deleteSet[idx] = true
	}

	var deleted int
	for i, set := range sets {
		if !deleteSet[i] {
			continue
		}
		// Delete deltas first, then the full snapshot.
		for _, delta := range set.Deltas {
			if err := g.store.Delete(ctx, delta.Name); err != nil {
				g.logger.Error("failed to delete delta snapshot",
					zap.String("name", delta.Name),
					zap.Error(err),
				)
				continue
			}
			deleted++
		}
		if err := g.store.Delete(ctx, set.Full.Name); err != nil {
			g.logger.Error("failed to delete full snapshot",
				zap.String("name", set.Full.Name),
				zap.Error(err),
			)
			continue
		}
		deleted++
	}

	g.logger.Info("garbage collection completed",
		zap.Int("deletedSnapshots", deleted),
		zap.Int("setsRemoved", len(deleteIndices)),
		zap.Int("setsRetained", len(sets)-len(deleteIndices)),
	)
	return nil
}

// computeDeleteIndices returns the indices (into a sorted oldest-first set
// slice) of sets that should be deleted.
func (g *GarbageCollector) computeDeleteIndices(sets []SnapshotSet) []int {
	if g.policy != nil {
		return g.computePolicyDeleteIndices(sets)
	}
	return EvaluateCountPolicy(sets, g.maxSets)
}

// computePolicyDeleteIndices uses the configured RetentionPolicy to determine
// which sets to delete. For CalendarPolicy it delegates to the batch evaluator;
// for other policies it evaluates each set individually.
func (g *GarbageCollector) computePolicyDeleteIndices(sets []SnapshotSet) []int {
	now := time.Now().UTC()

	if cp, ok := g.policy.(*CalendarPolicy); ok {
		return EvaluateCalendarPolicy(sets, *cp, now)
	}

	var toDelete []int
	for i, set := range sets {
		// Never delete the latest set.
		if i == len(sets)-1 {
			continue
		}
		if !g.policy.ShouldRetain(set, now) {
			toDelete = append(toDelete, i)
		}
	}
	return toDelete
}

// GroupSnapshots groups a flat list of snapshots (sorted by CreatedAt ascending)
// into SnapshotSets. Each set starts with a Full snapshot and includes all
// subsequent Delta snapshots up to (but not including) the next Full snapshot.
// The returned sets are sorted oldest-first.
func GroupSnapshots(snaps []snapstore.SnapInfo) []SnapshotSet {
	// Ensure ascending order by CreatedAt.
	sorted := make([]snapstore.SnapInfo, len(snaps))
	copy(sorted, snaps)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})

	var sets []SnapshotSet
	var current *SnapshotSet

	for _, snap := range sorted {
		if snap.Kind == snapstore.SnapKindFull {
			if current != nil {
				sets = append(sets, *current)
			}
			current = &SnapshotSet{Full: snap}
		} else if snap.Kind == snapstore.SnapKindDelta && current != nil {
			current.Deltas = append(current.Deltas, snap)
		}
		// Delta snapshots before any full snapshot are orphaned and ignored.
	}

	if current != nil {
		sets = append(sets, *current)
	}

	return sets
}
