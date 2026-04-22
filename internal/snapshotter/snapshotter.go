// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/etcdclient"
	"github.com/gardener/etcd-steward/internal/snapstore"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// DistributedLock provides mutual exclusion across replicas so that only one
// etcd-steward instance takes snapshots at a time.
type DistributedLock interface {
	// Acquire blocks until the lock is obtained or the context is cancelled.
	Acquire(ctx context.Context) error
	// Release releases the lock.
	Release(ctx context.Context) error
}

// Snapshotter takes periodic full and delta snapshots of an etcd cluster,
// coordinated via a distributed lock.
type Snapshotter struct {
	etcdClient   etcdclient.KV
	maintenance  etcdclient.Maintenance
	store        snapstore.SnapStore
	lock         DistributedLock
	compressAlgo compression.Algorithm
	fullInterval time.Duration
	deltaInterval time.Duration
	logger       *zap.Logger

	mu           sync.Mutex
	lastFullRev  int64
	lastDeltaRev int64
}

// New creates a new Snapshotter.
func New(
	etcdClient etcdclient.KV,
	maintenance etcdclient.Maintenance,
	store snapstore.SnapStore,
	lock DistributedLock,
	compressAlgo compression.Algorithm,
	fullInterval time.Duration,
	deltaInterval time.Duration,
	logger *zap.Logger,
) *Snapshotter {
	return &Snapshotter{
		etcdClient:    etcdClient,
		maintenance:   maintenance,
		store:         store,
		lock:          lock,
		compressAlgo:  compressAlgo,
		fullInterval:  fullInterval,
		deltaInterval: deltaInterval,
		logger:        logger,
	}
}

// Run acquires the distributed lock, takes an initial full snapshot, and then
// runs ticker loops for full and delta snapshots until the context is cancelled.
// The lock is released on return.
func (s *Snapshotter) Run(ctx context.Context) error {
	s.logger.Info("acquiring distributed lock")
	if err := s.lock.Acquire(ctx); err != nil {
		return fmt.Errorf("failed to acquire distributed lock: %w", err)
	}
	defer func() {
		s.logger.Info("releasing distributed lock")
		if err := s.lock.Release(context.Background()); err != nil {
			s.logger.Error("failed to release distributed lock", zap.Error(err))
		}
	}()

	s.logger.Info("taking initial full snapshot")
	if err := s.TakeFullSnapshot(ctx); err != nil {
		return fmt.Errorf("initial full snapshot failed: %w", err)
	}

	fullTicker := time.NewTicker(s.fullInterval)
	defer fullTicker.Stop()

	deltaTicker := time.NewTicker(s.deltaInterval)
	defer deltaTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("snapshotter context cancelled, stopping")
			return nil
		case <-fullTicker.C:
			if err := s.TakeFullSnapshot(ctx); err != nil {
				s.logger.Error("full snapshot failed", zap.Error(err))
			}
		case <-deltaTicker.C:
			if err := s.TakeDeltaSnapshot(ctx); err != nil {
				s.logger.Error("delta snapshot failed", zap.Error(err))
			}
		}
	}
}

// TakeFullSnapshot streams a point-in-time snapshot from etcd via the
// Maintenance.Snapshot API, optionally compresses it, and uploads it to the
// snap store.
func (s *Snapshotter) TakeFullSnapshot(ctx context.Context) error {
	s.logger.Info("taking full snapshot")

	rc, err := s.maintenance.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("maintenance snapshot: %w", err)
	}
	defer func() { _ = rc.Close() }()

	// Read the snapshot data into memory so we can compress it and measure size.
	raw, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("reading snapshot stream: %w", err)
	}

	compressed, err := compression.Compress(bytes.NewReader(raw), s.compressAlgo)
	if err != nil {
		return fmt.Errorf("compressing snapshot: %w", err)
	}

	// Determine current revision via a lightweight KV read.
	rev, err := s.currentRevision(ctx)
	if err != nil {
		return fmt.Errorf("getting current revision: %w", err)
	}

	now := time.Now().UTC()
	info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   rev,
		CreatedAt:     now,
		IsCompressed:  s.compressAlgo != compression.AlgorithmNone,
	}

	result, err := s.store.Upload(ctx, info, compressed)
	if err != nil {
		return fmt.Errorf("uploading full snapshot: %w", err)
	}

	s.mu.Lock()
	s.lastFullRev = rev
	s.lastDeltaRev = rev
	s.mu.Unlock()

	s.logger.Info("full snapshot uploaded",
		zap.String("name", result.Name),
		zap.Int64("revision", rev),
		zap.Int64("size", result.Size),
	)
	return nil
}

// TakeDeltaSnapshot records the current etcd revision as a delta snapshot.
// If the revision has not advanced since the last snapshot, the delta is
// skipped.
func (s *Snapshotter) TakeDeltaSnapshot(ctx context.Context) error {
	rev, err := s.currentRevision(ctx)
	if err != nil {
		return fmt.Errorf("getting current revision for delta: %w", err)
	}

	s.mu.Lock()
	lastRev := s.lastDeltaRev
	s.mu.Unlock()

	if rev <= lastRev {
		s.logger.Info("no new revisions since last snapshot, skipping delta",
			zap.Int64("current", rev),
			zap.Int64("last", lastRev),
		)
		return nil
	}

	s.logger.Info("taking delta snapshot",
		zap.Int64("startRevision", lastRev+1),
		zap.Int64("endRevision", rev),
	)

	// Delta snapshots are lightweight markers; store a minimal payload with
	// revision range metadata.
	payload := []byte(fmt.Sprintf("delta:%d-%d", lastRev+1, rev))
	compressed, err := compression.Compress(bytes.NewReader(payload), s.compressAlgo)
	if err != nil {
		return fmt.Errorf("compressing delta snapshot: %w", err)
	}

	now := time.Now().UTC()
	info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: lastRev + 1,
		EndRevision:   rev,
		CreatedAt:     now,
		IsCompressed:  s.compressAlgo != compression.AlgorithmNone,
	}

	result, err := s.store.Upload(ctx, info, compressed)
	if err != nil {
		return fmt.Errorf("uploading delta snapshot: %w", err)
	}

	s.mu.Lock()
	s.lastDeltaRev = rev
	s.mu.Unlock()

	s.logger.Info("delta snapshot uploaded",
		zap.String("name", result.Name),
		zap.Int64("startRevision", lastRev+1),
		zap.Int64("endRevision", rev),
		zap.Int64("size", result.Size),
	)
	return nil
}

// currentRevision returns the latest committed revision of the etcd cluster.
func (s *Snapshotter) currentRevision(ctx context.Context) (int64, error) {
	resp, err := s.etcdClient.Get(ctx, "", clientv3.WithLimit(1))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}
