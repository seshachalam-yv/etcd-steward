// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package snapshotter provides full and delta snapshot pipelines for etcd.
package snapshotter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/metrics"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// ErrNotLeader is returned when a snapshot is attempted on a non-leader member.
var ErrNotLeader = errors.New("not leader, skipping snapshot")

// EtcdSnapshotAPI is the subset of the etcd maintenance interface for snapshots.
type EtcdSnapshotAPI interface {
	Snapshot(ctx context.Context) (io.ReadCloser, error)
}

// EtcdWatchAPI is the subset of the etcd client for watching key changes.
type EtcdWatchAPI interface {
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

// EtcdStatusAPI is the subset of the etcd KV interface for querying current revision.
type EtcdStatusAPI interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

// CronScheduler parses cron expressions and provides a channel that fires at scheduled times.
// This is a simplified interface — for production, a proper cron library would be used.
type CronScheduler interface {
	Next(from time.Time) time.Time
}

// Snapshotter manages the full and delta snapshot pipeline.
type Snapshotter struct {
	store      snapstore.Snapstore
	compressor compression.Compressor
	etcdName   string
	namespace  string

	snapshotAPI EtcdSnapshotAPI
	watchAPI    EtcdWatchAPI
	kvAPI       EtcdStatusAPI
	isLeader    func() bool
	logger      *zap.Logger

	mu           sync.Mutex
	lastRevision int64
}

// New creates a Snapshotter with the given dependencies.
func New(
	store snapstore.Snapstore,
	compressor compression.Compressor,
	etcdName, namespace string,
	snapshotAPI EtcdSnapshotAPI,
	watchAPI EtcdWatchAPI,
	kvAPI EtcdStatusAPI,
	isLeader func() bool,
	logger *zap.Logger,
) *Snapshotter {
	return &Snapshotter{
		store:       store,
		compressor:  compressor,
		etcdName:    etcdName,
		namespace:   namespace,
		snapshotAPI: snapshotAPI,
		watchAPI:    watchAPI,
		kvAPI:       kvAPI,
		isLeader:    isLeader,
		logger:      logger,
	}
}

// TriggerFullSnapshot takes a full etcd snapshot and saves it to the snapstore.
// Returns ErrNotLeader if this member is not the leader.
func (s *Snapshotter) TriggerFullSnapshot(ctx context.Context, isFinal bool) (*snapstore.Snapshot, error) {
	if !s.isLeader() {
		return nil, ErrNotLeader
	}

	start := time.Now()

	// Get current revision for snapshot metadata.
	currentRev, err := s.getCurrentRevision(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current revision: %w", err)
	}

	// Take etcd snapshot.
	reader, err := s.snapshotAPI.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to take etcd snapshot: %w", err)
	}
	defer reader.Close() //nolint:errcheck

	// Read snapshot data.
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	// Compress.
	var compressed bytes.Buffer
	w, err := s.compressor.Compress(&compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("failed to compress snapshot: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize compression: %w", err)
	}

	now := time.Now().UTC()
	snap := snapstore.Snapshot{
		Kind:          "Full",
		StartRevision: 0,
		LastRevision:  currentRev,
		CreatedOn:     now,
		SnapDir:       fmt.Sprintf("Backup-%d", now.Unix()),
	}
	snap.SnapName = snapstore.FormatSnapshotName(snap) + s.compressor.FileExtension()

	if err := s.store.Save(snap, io.NopCloser(&compressed)); err != nil {
		return nil, fmt.Errorf("failed to save full snapshot: %w", err)
	}

	s.mu.Lock()
	s.lastRevision = currentRev
	s.mu.Unlock()

	duration := time.Since(start).Seconds()
	metrics.SnapshotDurationSeconds.WithLabelValues("full").Observe(duration)

	s.logger.Info("full snapshot saved",
		zap.Int64("revision", currentRev),
		zap.String("path", snap.Path()),
		zap.Float64("durationSeconds", duration),
		zap.Bool("isFinal", isFinal),
	)

	return &snap, nil
}

// TriggerDeltaSnapshot watches etcd events from the last known revision and saves
// the collected events as an incremental snapshot.
func (s *Snapshotter) TriggerDeltaSnapshot(ctx context.Context) (*snapstore.Snapshot, error) {
	if !s.isLeader() {
		return nil, ErrNotLeader
	}

	start := time.Now()

	s.mu.Lock()
	startRev := s.lastRevision
	s.mu.Unlock()

	if startRev == 0 {
		return nil, fmt.Errorf("no full snapshot taken yet, cannot take delta")
	}

	// Watch from startRev+1 to collect events.
	watchCtx, watchCancel := context.WithTimeout(ctx, 30*time.Second)
	defer watchCancel()

	wch := s.watchAPI.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(startRev+1))

	var events bytes.Buffer
	var lastRev int64
	eventCount := int64(0)
	maxDeltaEvents := int64(1_000_000)
	maxDeltaSize := int64(100 * 1024 * 1024) // 100 MiB

	for watchResp := range wch {
		if watchResp.Err() != nil {
			break
		}
		for _, ev := range watchResp.Events {
			// Simple serialization: write the key-value as length-prefixed bytes.
			if ev.Kv != nil {
				events.Write(ev.Kv.Key)
				events.WriteByte('\n')
				events.Write(ev.Kv.Value)
				events.WriteByte('\n')
			}
			if ev.Kv != nil && ev.Kv.ModRevision > lastRev {
				lastRev = ev.Kv.ModRevision
			}
			eventCount++
		}

		if eventCount >= maxDeltaEvents || int64(events.Len()) >= maxDeltaSize {
			break
		}
	}

	if eventCount == 0 {
		return nil, nil // No events to snapshot.
	}

	// Compress the collected events.
	var compressed bytes.Buffer
	w, err := s.compressor.Compress(&compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressor for delta: %w", err)
	}
	if _, err := w.Write(events.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to compress delta: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize delta compression: %w", err)
	}

	now := time.Now().UTC()
	snap := snapstore.Snapshot{
		Kind:          "Incremental",
		StartRevision: startRev,
		LastRevision:  lastRev,
		CreatedOn:     now,
		SnapDir:       fmt.Sprintf("Backup-%d", now.Unix()),
	}
	snap.SnapName = snapstore.FormatSnapshotName(snap) + s.compressor.FileExtension()

	if err := s.store.Save(snap, io.NopCloser(&compressed)); err != nil {
		return nil, fmt.Errorf("failed to save delta snapshot: %w", err)
	}

	s.mu.Lock()
	s.lastRevision = lastRev
	s.mu.Unlock()

	duration := time.Since(start).Seconds()
	metrics.SnapshotDurationSeconds.WithLabelValues("delta").Observe(duration)

	s.logger.Info("delta snapshot saved",
		zap.Int64("startRevision", startRev),
		zap.Int64("lastRevision", lastRev),
		zap.Int64("events", eventCount),
		zap.String("path", snap.Path()),
		zap.Float64("durationSeconds", duration),
	)

	return &snap, nil
}

// RunFullSnapshotSchedule runs TriggerFullSnapshot on a simple periodic schedule.
// For Phase 2, this uses a fixed interval parsed from the schedule string.
// A real cron parser would be used in production.
func (s *Snapshotter) RunFullSnapshotSchedule(ctx context.Context, schedule string) {
	// Parse schedule as a simple interval. Default to 24h.
	interval := 24 * time.Hour

	s.logger.Info("starting full snapshot schedule", zap.String("schedule", schedule), zap.Duration("interval", interval))

	// Take an initial full snapshot immediately.
	if _, err := s.TriggerFullSnapshot(ctx, false); err != nil && !errors.Is(err, ErrNotLeader) {
		s.logger.Error("initial full snapshot failed", zap.Error(err))
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.TriggerFullSnapshot(ctx, false); err != nil && !errors.Is(err, ErrNotLeader) {
				s.logger.Error("scheduled full snapshot failed", zap.Error(err))
			}
		}
	}
}

// RunDeltaSnapshotLoop runs TriggerDeltaSnapshot on the given period until ctx is cancelled.
func (s *Snapshotter) RunDeltaSnapshotLoop(ctx context.Context, period time.Duration) {
	s.logger.Info("starting delta snapshot loop", zap.Duration("period", period))

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.TriggerDeltaSnapshot(ctx); err != nil && !errors.Is(err, ErrNotLeader) {
				s.logger.Error("delta snapshot failed", zap.Error(err))
			}
		}
	}
}

// getCurrentRevision queries etcd for the current revision.
func (s *Snapshotter) getCurrentRevision(ctx context.Context) (int64, error) {
	resp, err := s.kvAPI.Get(ctx, "", clientv3.WithLimit(1))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}
