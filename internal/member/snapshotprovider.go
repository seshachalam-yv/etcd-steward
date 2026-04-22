// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"

	"github.com/gardener/etcd-steward/internal/errors"
	"github.com/gardener/etcd-steward/internal/snapstore"

	"go.uber.org/zap"
)

// SnapshotInfoProvider reports snapshot status by querying the snap store.
type SnapshotInfoProvider struct {
	store  snapstore.SnapStore
	logger *zap.Logger
}

// NewSnapshotInfoProvider creates a SnapshotInfoProvider backed by the given store.
func NewSnapshotInfoProvider(store snapstore.SnapStore, logger *zap.Logger) *SnapshotInfoProvider {
	return &SnapshotInfoProvider{
		store:  store,
		logger: logger,
	}
}

// ID returns the unique identifier for this info provider.
func (p *SnapshotInfoProvider) ID() string {
	return "snapshot-info"
}

// GetInfo lists snapshots from the store and returns summary metadata:
//   - lastFullSnapshotRevision: int64
//   - lastDeltaSnapshotRevision: int64
//   - totalSnapshotCount: int
//   - accumulatedDeltaSize: int64
func (p *SnapshotInfoProvider) GetInfo(ctx context.Context) (map[string]interface{}, error) {
	snaps, err := p.store.List(ctx)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeSnapshot, "failed to list snapshots", err)
	}

	var (
		lastFullRev  int64
		lastDeltaRev int64
		deltaSize    int64
	)

	for _, s := range snaps {
		switch s.Kind {
		case snapstore.SnapKindFull:
			if s.EndRevision > lastFullRev {
				lastFullRev = s.EndRevision
			}
		case snapstore.SnapKindDelta:
			if s.EndRevision > lastDeltaRev {
				lastDeltaRev = s.EndRevision
			}
			deltaSize += s.Size
		}
	}

	p.logger.Debug("snapshot info collected",
		zap.Int64("lastFullRev", lastFullRev),
		zap.Int64("lastDeltaRev", lastDeltaRev),
		zap.Int("count", len(snaps)),
		zap.Int64("deltaSize", deltaSize),
	)

	return map[string]interface{}{
		"lastFullSnapshotRevision":  lastFullRev,
		"lastDeltaSnapshotRevision": lastDeltaRev,
		"totalSnapshotCount":        len(snaps),
		"accumulatedDeltaSize":      deltaSize,
	}, nil
}
