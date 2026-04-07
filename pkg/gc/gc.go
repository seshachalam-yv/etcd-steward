// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package gc provides snapshot garbage collection for etcd-steward.
package gc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// GarbageCollector deletes old snapshot sets from the snapstore based on retention policy.
type GarbageCollector struct {
	store            snapstore.Snapstore
	maxFullSnapshots int
	logger           *zap.Logger
}

// New creates a GarbageCollector that retains at most maxFullSnapshots full snapshot sets.
func New(store snapstore.Snapstore, maxFullSnapshots int, logger *zap.Logger) *GarbageCollector {
	return &GarbageCollector{
		store:            store,
		maxFullSnapshots: maxFullSnapshots,
		logger:           logger,
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

// Collect performs a single garbage collection pass using LimitBased retention.
// It groups snapshots into sets (each Full starts a new set), retains the last
// maxFullSnapshots sets, and deletes all older sets.
func (gc *GarbageCollector) Collect(ctx context.Context) error {
	snaps, err := gc.store.List()
	if err != nil {
		return err
	}

	if len(snaps) == 0 {
		return nil
	}

	sets := groupIntoSets(snaps)
	if len(sets) <= gc.maxFullSnapshots {
		return nil
	}

	// Delete sets older than the retained ones.
	// Never delete the latest set regardless.
	deleteCount := len(sets) - gc.maxFullSnapshots
	for i := 0; i < deleteCount; i++ {
		set := sets[i]
		gc.logger.Info("deleting snapshot set",
			zap.Int64("fullRevision", set.full.LastRevision),
			zap.Int("incrementals", len(set.incrementals)),
		)

		if set.full.SnapName != "" {
			if err := gc.store.Delete(set.full); err != nil {
				gc.logger.Error("failed to delete full snapshot",
					zap.String("path", set.full.Path()),
					zap.Error(err),
				)
			}
		}

		for _, inc := range set.incrementals {
			if err := gc.store.Delete(inc); err != nil {
				gc.logger.Error("failed to delete incremental snapshot",
					zap.String("path", inc.Path()),
					zap.Error(err),
				)
			}
		}
	}

	// Check for context cancellation.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	return nil
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
