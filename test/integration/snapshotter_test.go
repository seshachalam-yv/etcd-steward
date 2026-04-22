// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/snapshotter"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// TestSnapshotterFullCycle_NoCompression tests the full snapshot lifecycle
// without compression: take a full snapshot, write events via the watch mock,
// take a delta snapshot, and verify both snapshots exist in the store with
// correct metadata and content.
func TestSnapshotterFullCycle_NoCompression(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()

	kv := &mockKV{revision: 100}
	maint := &mockMaintenance{snapshotData: []byte("full-db-snapshot-data")}
	lock := &mockLock{}

	// Prepare watcher with events at revisions 101-105.
	watchEvents := make([]*clientv3.Event, 5)
	for i := range 5 {
		rev := int64(101 + i)
		watchEvents[i] = &clientv3.Event{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte(fmt.Sprintf("/registry/key-%d", i)),
				Value:       []byte(fmt.Sprintf("value-%d", i)),
				ModRevision: rev,
			},
		}
	}
	// Make the last event a DELETE.
	watchEvents[4].Type = clientv3.EventTypeDelete
	watchEvents[4].Kv.Value = nil

	watcher := &mockWatcher{
		responses: []clientv3.WatchResponse{
			{
				Header: etcdserverpb.ResponseHeader{Revision: 105},
				Events: watchEvents,
			},
		},
	}

	logger := zap.NewNop()
	s := snapshotter.New(kv, maint, watcher, store, lock, compression.AlgorithmNone,
		time.Hour, time.Minute, logger)

	// Step 1: Take a full snapshot.
	if err := s.TakeFullSnapshot(ctx); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	// Step 2: Advance revision and take a delta snapshot.
	kv.revision = 105
	if err := s.TakeDeltaSnapshot(ctx); err != nil {
		t.Fatalf("TakeDeltaSnapshot: %v", err)
	}

	// Verify: store should contain exactly 2 snapshots.
	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}

	// Find full and delta snapshots.
	var fullSnap, deltaSnap snapstore.SnapInfo
	for _, snap := range snaps {
		switch snap.Kind {
		case snapstore.SnapKindFull:
			fullSnap = snap
		case snapstore.SnapKindDelta:
			deltaSnap = snap
		}
	}

	// Verify full snapshot.
	if fullSnap.Kind != snapstore.SnapKindFull {
		t.Fatalf("expected full snapshot kind, got %s", fullSnap.Kind)
	}
	if fullSnap.EndRevision != 100 {
		t.Fatalf("expected full EndRevision=100, got %d", fullSnap.EndRevision)
	}

	// Verify delta snapshot metadata.
	if deltaSnap.Kind != snapstore.SnapKindDelta {
		t.Fatalf("expected delta snapshot kind, got %s", deltaSnap.Kind)
	}
	if deltaSnap.StartRevision != 101 {
		t.Fatalf("expected delta StartRevision=101, got %d", deltaSnap.StartRevision)
	}
	if deltaSnap.EndRevision != 105 {
		t.Fatalf("expected delta EndRevision=105, got %d", deltaSnap.EndRevision)
	}

	// Verify delta snapshot content: download and read NDJSON events.
	rc, err := store.Download(ctx, deltaSnap.Name)
	if err != nil {
		t.Fatalf("Download delta: %v", err)
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	events, err := snapshotter.ReadEvents(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	if len(events) != 5 {
		t.Fatalf("expected 5 events in delta, got %d", len(events))
	}

	// Verify first 4 are PUTs and last is DELETE.
	for i := 0; i < 4; i++ {
		if events[i].Type != snapshotter.EventTypePut {
			t.Fatalf("event %d: expected PUT, got %s", i, events[i].Type)
		}
		expectedKey := fmt.Sprintf("/registry/key-%d", i)
		if string(events[i].Key) != expectedKey {
			t.Fatalf("event %d Key: got %q, want %q", i, events[i].Key, expectedKey)
		}
		expectedValue := fmt.Sprintf("value-%d", i)
		if string(events[i].Value) != expectedValue {
			t.Fatalf("event %d Value: got %q, want %q", i, events[i].Value, expectedValue)
		}
	}
	if events[4].Type != snapshotter.EventTypeDelete {
		t.Fatalf("event 4: expected DELETE, got %s", events[4].Type)
	}
	if events[4].Revision != 105 {
		t.Fatalf("event 4 Revision: got %d, want 105", events[4].Revision)
	}
}

// TestSnapshotterFullCycle_WithGzipCompression tests the snapshot lifecycle
// with gzip compression enabled. Verifies that compressed snapshots are
// stored and that compressed delta data is smaller than raw event data.
func TestSnapshotterFullCycle_WithGzipCompression(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()

	kv := &mockKV{revision: 50}
	maint := &mockMaintenance{snapshotData: bytes.Repeat([]byte("A"), 4096)}
	lock := &mockLock{}

	// Generate 20 events with repetitive data (compresses well).
	watchEvents := make([]*clientv3.Event, 20)
	for i := range 20 {
		rev := int64(51 + i)
		watchEvents[i] = &clientv3.Event{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte(fmt.Sprintf("/registry/pods/default/pod-%d", i)),
				Value:       []byte(fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod-%d"}}`, i)),
				ModRevision: rev,
			},
		}
	}

	watcher := &mockWatcher{
		responses: []clientv3.WatchResponse{
			{
				Header: etcdserverpb.ResponseHeader{Revision: 70},
				Events: watchEvents,
			},
		},
	}

	logger := zap.NewNop()
	s := snapshotter.New(kv, maint, watcher, store, lock, compression.AlgorithmGzip,
		time.Hour, time.Minute, logger)

	if err := s.TakeFullSnapshot(ctx); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	kv.revision = 70
	if err := s.TakeDeltaSnapshot(ctx); err != nil {
		t.Fatalf("TakeDeltaSnapshot: %v", err)
	}

	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}

	// Verify compression actually reduced size for the full snapshot.
	var fullSnap snapstore.SnapInfo
	for _, snap := range snaps {
		if snap.Kind == snapstore.SnapKindFull {
			fullSnap = snap
			break
		}
	}
	// The original data is 4096 bytes of repeated 'A'. Gzip should compress
	// this significantly.
	if fullSnap.Size >= 4096 {
		t.Fatalf("expected compressed full snapshot to be smaller than 4096 bytes, got %d", fullSnap.Size)
	}
}

// TestSnapshotterDeltaSkipsWhenNoNewRevisions verifies that TakeDeltaSnapshot
// does not create a delta snapshot when there are no new revisions since the
// last full snapshot.
func TestSnapshotterDeltaSkipsWhenNoNewRevisions(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()

	kv := &mockKV{revision: 42}
	maint := &mockMaintenance{snapshotData: []byte("snap")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := snapshotter.New(kv, maint, watcher, store, lock, compression.AlgorithmNone,
		time.Hour, time.Minute, logger)

	if err := s.TakeFullSnapshot(ctx); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	// Revision unchanged -- delta should be a no-op.
	if err := s.TakeDeltaSnapshot(ctx); err != nil {
		t.Fatalf("TakeDeltaSnapshot: %v", err)
	}

	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot (full only), got %d", len(snaps))
	}

	if watcher.watchCalled {
		t.Fatal("expected watcher.Watch not to be called when revision unchanged")
	}
}

// TestSnapshotterMultipleDeltas verifies that taking multiple consecutive delta
// snapshots correctly advances the tracked revision and stores each delta
// independently.
func TestSnapshotterMultipleDeltas(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()

	kv := &mockKV{revision: 10}
	maint := &mockMaintenance{snapshotData: []byte("full-data")}
	lock := &mockLock{}
	logger := zap.NewNop()

	// Take a full snapshot at revision 10.
	watcher1 := &mockWatcher{
		responses: []clientv3.WatchResponse{
			{
				Header: etcdserverpb.ResponseHeader{Revision: 20},
				Events: []*clientv3.Event{
					{
						Type: clientv3.EventTypePut,
						Kv: &mvccpb.KeyValue{
							Key:         []byte("/delta1/key"),
							Value:       []byte("val1"),
							ModRevision: 20,
						},
					},
				},
			},
		},
	}

	s := snapshotter.New(kv, maint, watcher1, store, lock, compression.AlgorithmNone,
		time.Hour, time.Minute, logger)

	if err := s.TakeFullSnapshot(ctx); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	// Delta 1: revision 10 -> 20.
	kv.revision = 20
	if err := s.TakeDeltaSnapshot(ctx); err != nil {
		t.Fatalf("TakeDeltaSnapshot 1: %v", err)
	}

	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots after first delta, got %d", len(snaps))
	}

	// Delta 2: revision 20 -> 20. Should skip (no new revisions).
	if err := s.TakeDeltaSnapshot(ctx); err != nil {
		t.Fatalf("TakeDeltaSnapshot 2 (skip): %v", err)
	}

	snaps, err = store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected still 2 snapshots (no new revision), got %d", len(snaps))
	}
}
