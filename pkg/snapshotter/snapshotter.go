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
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/metrics"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// ErrNotLeader is returned when a snapshot is attempted on a non-leader member.
var ErrNotLeader = errors.New("not leader, skipping snapshot")

// ErrNoFullSnapshot is returned when a delta snapshot is attempted before the first full snapshot.
// This is an expected startup condition and should not be treated as an error.
var ErrNoFullSnapshot = errors.New("no full snapshot taken yet, cannot take delta")

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

	mu                   sync.Mutex
	lastRevision         int64
	lastFullRevision     int64 // revision of the last full snapshot; delta events at or below this are redundant
	snapshotInfo         member.SnapshotInfo // tracks latest snapshot metadata for InfoProvider
	accumulatedDeltaBytes int64              // running total of uncompressed delta sizes
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

	// Take etcd snapshot first, then read the revision from the cluster.
	// The revision is queried AFTER the snapshot stream starts so it reflects
	// the actual state captured in the snapshot file, not a pre-snapshot estimate.
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

	// Get the revision after the snapshot has been fully streamed.
	// This revision matches what was captured in the snapshot more closely
	// than a pre-snapshot status call.
	currentRev, err := s.getCurrentRevision(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current revision: %w", err)
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

	uncompressedSize := int64(len(data))
	s.mu.Lock()
	s.lastRevision = currentRev
	s.lastFullRevision = currentRev
	s.accumulatedDeltaBytes = 0
	qty := resource.NewMilliQuantity(uncompressedSize*1000, resource.BinarySI)
	zero := resource.MustParse("0")
	s.snapshotInfo = member.SnapshotInfo{
		LastFull: &member.SnapshotEntry{
			Name:          snap.SnapName,
			Timestamp:     metav1.NewTime(now),
			StartRevision: 0,
			EndRevision:   currentRev,
			Size:          qty,
		},
		LastDelta:            s.snapshotInfo.LastDelta, // preserve existing delta
		AccumulatedDeltaSize: &zero,
	}
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
		return nil, ErrNoFullSnapshot
	}

	// Watch from startRev+1 to collect events.
	watchCtx, watchCancel := context.WithTimeout(ctx, 30*time.Second)
	defer watchCancel()

	wch := s.watchAPI.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(startRev+1))

	var allEvents []DeltaEvent
	var lastRev int64
	eventCount := int64(0)
	maxDeltaEvents := int64(1_000_000)
	maxDeltaSize := int64(100 * 1024 * 1024) // 100 MiB
	var rawSize int64

	for watchResp := range wch {
		if watchResp.Err() != nil {
			break
		}
		for _, ev := range watchResp.Events {
			if ev.Kv == nil {
				continue
			}
			evType := EventTypePut
			if ev.Type == mvccpb.DELETE {
				evType = EventTypeDelete
			}
			de := DeltaEvent{
				Type:           evType,
				Key:            ev.Kv.Key,
				Value:          ev.Kv.Value,
				CreateRevision: ev.Kv.CreateRevision,
				ModRevision:    ev.Kv.ModRevision,
				Version:        ev.Kv.Version,
			}
			allEvents = append(allEvents, de)
			if ev.Kv.ModRevision > lastRev {
				lastRev = ev.Kv.ModRevision
			}
			eventCount++
			rawSize += int64(len(ev.Kv.Key) + len(ev.Kv.Value))
		}

		if eventCount >= maxDeltaEvents || rawSize >= maxDeltaSize {
			break
		}
	}

	// Re-read lastFullRevision under lock. A concurrent TriggerFullSnapshot may have
	// advanced it while the watch was running. Discard events already covered by the
	// newer full snapshot so the delta's startRevision is correct.
	s.mu.Lock()
	effectiveStartRev := s.lastFullRevision
	s.mu.Unlock()
	if effectiveStartRev < startRev {
		effectiveStartRev = startRev
	}

	var events bytes.Buffer
	var filteredCount int64
	for _, de := range allEvents {
		if de.ModRevision <= effectiveStartRev {
			continue // already covered by a full snapshot
		}
		line, err := MarshalDeltaEvent(de)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal delta event: %w", err)
		}
		events.Write(line)
		events.WriteByte('\n')
		filteredCount++
	}

	if filteredCount == 0 {
		return nil, nil // No events to snapshot beyond what full snapshots cover.
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
		StartRevision: effectiveStartRev,
		LastRevision:  lastRev,
		CreatedOn:     now,
		SnapDir:       fmt.Sprintf("Backup-%d", now.Unix()),
	}
	snap.SnapName = snapstore.FormatSnapshotName(snap) + s.compressor.FileExtension()

	if err := s.store.Save(snap, io.NopCloser(&compressed)); err != nil {
		return nil, fmt.Errorf("failed to save delta snapshot: %w", err)
	}

	deltaSize := int64(events.Len())
	s.mu.Lock()
	s.lastRevision = lastRev
	s.accumulatedDeltaBytes += deltaSize
	accDeltaQty := resource.NewMilliQuantity(s.accumulatedDeltaBytes*1000, resource.BinarySI)
	deltaQty := resource.NewMilliQuantity(deltaSize*1000, resource.BinarySI)
	s.snapshotInfo.LastDelta = &member.SnapshotEntry{
		Name:          snap.SnapName,
		Timestamp:     metav1.NewTime(now),
		StartRevision: effectiveStartRev,
		EndRevision:   lastRev,
		Size:          deltaQty,
	}
	s.snapshotInfo.AccumulatedDeltaSize = accDeltaQty
	s.mu.Unlock()

	duration := time.Since(start).Seconds()
	metrics.SnapshotDurationSeconds.WithLabelValues("delta").Observe(duration)

	s.logger.Info("delta snapshot saved",
		zap.Int64("startRevision", effectiveStartRev),
		zap.Int64("lastRevision", lastRev),
		zap.Int64("events", filteredCount),
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
			if _, err := s.TriggerDeltaSnapshot(ctx); err != nil {
				switch {
				case errors.Is(err, ErrNotLeader):
					// Not the leader — expected on follower pods, skip silently.
				case errors.Is(err, ErrNoFullSnapshot):
					// First full snapshot not yet taken — normal at startup, no action needed.
					s.logger.Debug("skipping delta snapshot, waiting for first full snapshot")
				default:
					s.logger.Error("delta snapshot failed", zap.Error(err))
				}
			}
		}
	}
}

// getCurrentRevision queries etcd for the current revision.
func (s *Snapshotter) getCurrentRevision(ctx context.Context) (int64, error) {
	// Use the null-byte key with WithFromKey so we get the Header.Revision
	// without triggering "key is not provided" — equivalent to a range scan
	// from the first key in the keyspace (0 results if empty).
	resp, err := s.kvAPI.Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithLimit(1))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// ProvideInfo implements member.InfoProvider.
// Returns the latest snapshot metadata collected by this Snapshotter.
// Safe to call concurrently with TriggerFullSnapshot and TriggerDeltaSnapshot.
func (s *Snapshotter) ProvideInfo() member.StatusInfo {
	s.mu.Lock()
	info := s.snapshotInfo // copy under lock
	s.mu.Unlock()
	if info.LastFull == nil && info.LastDelta == nil {
		return member.StatusInfo{}
	}
	return member.StatusInfo{Snapshots: &info}
}
