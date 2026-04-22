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

// maxDeltaSnapshotBytes is the upper bound on accumulated event data before a
// delta snapshot is flushed to the snap store.
const maxDeltaSnapshotBytes = 100 * 1024 * 1024

// Watcher abstracts the etcd Watch API so it can be mocked in tests.
type Watcher interface {
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

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
	watcher      Watcher
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
	watcher Watcher,
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
		watcher:       watcher,
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

// TakeDeltaSnapshot watches the etcd event stream from the last known revision,
// collects mutation events, serialises them as compressed NDJSON, and uploads
// the result to the snap store. If no new revisions exist the snapshot is
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

	// Watch events from lastDeltaRev+1 up to the current revision.
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	watchCh := s.watcher.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(lastRev+1))

	var events []Event
	var accumulatedSize int
	endRev := lastRev

	collectLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case wresp, ok := <-watchCh:
			if !ok {
				break collectLoop
			}
			if err := wresp.Err(); err != nil {
				return fmt.Errorf("watch error: %w", err)
			}
			for _, ev := range wresp.Events {
				event := Event{
					Key:      ev.Kv.Key,
					Revision: ev.Kv.ModRevision,
				}
				if ev.Type == clientv3.EventTypePut {
					event.Type = EventTypePut
					event.Value = ev.Kv.Value
				} else {
					event.Type = EventTypeDelete
				}
				events = append(events, event)
				accumulatedSize += len(event.Key) + len(event.Value)

				if event.Revision > endRev {
					endRev = event.Revision
				}

				if accumulatedSize >= maxDeltaSnapshotBytes {
					break collectLoop
				}
			}
			// If we have caught up to the target revision, stop collecting.
			if endRev >= rev {
				break collectLoop
			}
		}
	}
	watchCancel()

	if len(events) == 0 {
		s.logger.Info("watch returned no events, skipping delta")
		return nil
	}

	// Serialise events as NDJSON.
	var buf bytes.Buffer
	if err := WriteEvents(&buf, events); err != nil {
		return fmt.Errorf("serialising delta events: %w", err)
	}

	compressed, err := compression.Compress(&buf, s.compressAlgo)
	if err != nil {
		return fmt.Errorf("compressing delta snapshot: %w", err)
	}

	now := time.Now().UTC()
	info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: lastRev + 1,
		EndRevision:   endRev,
		CreatedAt:     now,
		IsCompressed:  s.compressAlgo != compression.AlgorithmNone,
	}

	result, err := s.store.Upload(ctx, info, compressed)
	if err != nil {
		return fmt.Errorf("uploading delta snapshot: %w", err)
	}

	s.mu.Lock()
	s.lastDeltaRev = endRev
	s.mu.Unlock()

	s.logger.Info("delta snapshot uploaded",
		zap.String("name", result.Name),
		zap.Int64("startRevision", lastRev+1),
		zap.Int64("endRevision", endRev),
		zap.Int64("size", result.Size),
		zap.Int("eventCount", len(events)),
	)
	return nil
}

// currentRevision returns the latest committed revision of the etcd cluster.
func (s *Snapshotter) currentRevision(ctx context.Context) (int64, error) {
	resp, err := s.etcdClient.Get(ctx, "", clientv3.WithLimit(1))
	if err != nil {
		return 0, err
	}
	if resp == nil || resp.Header == nil {
		return 0, fmt.Errorf("etcd returned nil response or header")
	}
	return resp.Header.Revision, nil
}
