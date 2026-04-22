// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// --- mock types ---

type mockLock struct {
	acquireErr error
	releaseErr error
	acquired   bool
	released   bool
}

func (m *mockLock) Acquire(_ context.Context) error {
	if m.acquireErr != nil {
		return m.acquireErr
	}
	m.acquired = true
	return nil
}

func (m *mockLock) Release(_ context.Context) error {
	if m.releaseErr != nil {
		return m.releaseErr
	}
	m.released = true
	return nil
}

type mockKV struct {
	revision int64
	getErr   error
}

func (m *mockKV) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: m.revision},
	}, nil
}

func (m *mockKV) Put(_ context.Context, _, _ string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return nil, nil
}

func (m *mockKV) Delete(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return nil, nil
}

func (m *mockKV) Compact(_ context.Context, _ int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, nil
}

type mockMaintenance struct {
	snapshotData []byte
	snapshotErr  error
}

func (m *mockMaintenance) Defragment(_ context.Context, _ string) (*clientv3.DefragmentResponse, error) {
	return nil, nil
}

func (m *mockMaintenance) AlarmList(_ context.Context) (*clientv3.AlarmResponse, error) {
	return nil, nil
}

func (m *mockMaintenance) AlarmDisarm(_ context.Context, _ *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	return nil, nil
}

func (m *mockMaintenance) Status(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
	return nil, nil
}

func (m *mockMaintenance) Snapshot(_ context.Context) (io.ReadCloser, error) {
	if m.snapshotErr != nil {
		return nil, m.snapshotErr
	}
	return io.NopCloser(bytes.NewReader(m.snapshotData)), nil
}

// mockWatcher implements the Watcher interface for testing.
type mockWatcher struct {
	// events to return on the watch channel. The channel is closed after
	// all responses are sent.
	responses []clientv3.WatchResponse
	// watchCalled tracks whether Watch was invoked.
	watchCalled bool
}

func (m *mockWatcher) Watch(_ context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	m.watchCalled = true
	ch := make(chan clientv3.WatchResponse, len(m.responses))
	for _, r := range m.responses {
		ch <- r
	}
	close(ch)
	return ch
}

// --- tests ---

func TestTakeFullSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 42}
	maint := &mockMaintenance{snapshotData: []byte("full-db-snapshot")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	if err := s.TakeFullSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].Kind != snapstore.SnapKindFull {
		t.Fatalf("expected kind Full, got %s", snaps[0].Kind)
	}
	if snaps[0].EndRevision != 42 {
		t.Fatalf("expected end revision 42, got %d", snaps[0].EndRevision)
	}

	s.mu.Lock()
	if s.lastFullRev != 42 {
		t.Fatalf("expected lastFullRev 42, got %d", s.lastFullRev)
	}
	if s.lastDeltaRev != 42 {
		t.Fatalf("expected lastDeltaRev 42, got %d", s.lastDeltaRev)
	}
	s.mu.Unlock()
}

func TestTakeFullSnapshotWithCompression(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 10}
	maint := &mockMaintenance{snapshotData: []byte("compressed-full-snapshot")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmGzip, time.Hour, time.Minute, logger)

	if err := s.TakeFullSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].EndRevision != 10 {
		t.Fatalf("expected end revision 10, got %d", snaps[0].EndRevision)
	}
}

func TestTakeDeltaSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 100}
	maint := &mockMaintenance{snapshotData: []byte("full-data")}
	lock := &mockLock{}
	logger := zap.NewNop()

	// Prepare a watcher that returns events covering revisions 101-150.
	watcher := &mockWatcher{
		responses: []clientv3.WatchResponse{
			{
				Header: etcdserverpb.ResponseHeader{Revision: 150},
				Events: []*clientv3.Event{
					{
						Type: clientv3.EventTypePut,
						Kv: &mvccpb.KeyValue{
							Key:         []byte("/key/1"),
							Value:       []byte("val1"),
							ModRevision: 150,
						},
					},
				},
			},
		},
	}

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	// Take a full first to set lastDeltaRev.
	if err := s.TakeFullSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	// Advance revision.
	kv.revision = 150

	if err := s.TakeDeltaSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeDeltaSnapshot: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}

	// Find the delta snapshot by kind (sort order may vary when timestamps
	// fall within the same second).
	var delta snapstore.SnapInfo
	var found bool
	for _, snap := range snaps {
		if snap.Kind == snapstore.SnapKindDelta {
			delta = snap
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected a delta snapshot in the list")
	}
	if delta.StartRevision != 101 {
		t.Fatalf("expected start revision 101, got %d", delta.StartRevision)
	}
	if delta.EndRevision != 150 {
		t.Fatalf("expected end revision 150, got %d", delta.EndRevision)
	}

	s.mu.Lock()
	if s.lastDeltaRev != 150 {
		t.Fatalf("expected lastDeltaRev 150, got %d", s.lastDeltaRev)
	}
	s.mu.Unlock()
}

func TestTakeDeltaSnapshotSkipsWhenNoNewRevisions(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 50}
	maint := &mockMaintenance{snapshotData: []byte("full")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	if err := s.TakeFullSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	// Same revision -- delta should be skipped.
	if err := s.TakeDeltaSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeDeltaSnapshot: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot (no delta), got %d", len(snaps))
	}

	// Watcher should not have been called when revision is unchanged.
	if watcher.watchCalled {
		t.Fatal("expected watcher.Watch not to be called when no new revisions")
	}
}

func TestRunAcquiresAndReleasesLock(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 5}
	maint := &mockMaintenance{snapshotData: []byte("snap")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Hour, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx)
	}()

	// Let the goroutine start and take the initial full snapshot.
	time.Sleep(100 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !lock.acquired {
		t.Fatal("expected lock to be acquired")
	}
	if !lock.released {
		t.Fatal("expected lock to be released")
	}
}

func TestRunFailsWhenLockAcquireFails(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 1}
	maint := &mockMaintenance{snapshotData: []byte("snap")}
	lock := &mockLock{acquireErr: fmt.Errorf("lock unavailable")}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Hour, logger)

	err = s.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when lock acquire fails")
	}
	if !strings.Contains(err.Error(), "lock unavailable") {
		t.Fatalf("expected error to contain 'lock unavailable', got: %v", err)
	}
}

func TestTakeFullSnapshotFailsOnMaintenanceError(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 1}
	maint := &mockMaintenance{snapshotErr: fmt.Errorf("etcd unavailable")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	err = s.TakeFullSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected error on maintenance snapshot failure")
	}
	if !strings.Contains(err.Error(), "etcd unavailable") {
		t.Fatalf("expected error to contain 'etcd unavailable', got: %v", err)
	}
}

func TestTakeFullSnapshotFailsOnKVError(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 0, getErr: fmt.Errorf("kv unavailable")}
	maint := &mockMaintenance{snapshotData: []byte("snap")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	err = s.TakeFullSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected error when KV.Get fails during full snapshot")
	}
	if !strings.Contains(err.Error(), "kv unavailable") {
		t.Fatalf("expected error to contain 'kv unavailable', got: %v", err)
	}
}

func TestTakeDeltaSnapshotFailsOnKVError(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 10, getErr: fmt.Errorf("connection refused")}
	maint := &mockMaintenance{}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	err = s.TakeDeltaSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected error on KV.Get failure")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("expected error to contain 'connection refused', got: %v", err)
	}
}

func TestTakeDeltaSnapshot_CapturesEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 10}
	maint := &mockMaintenance{snapshotData: []byte("full-snap")}
	lock := &mockLock{}
	logger := zap.NewNop()

	// Build 5 watch events at revisions 11-15.
	watchEvents := make([]*clientv3.Event, 5)
	for i := range 5 {
		rev := int64(11 + i)
		watchEvents[i] = &clientv3.Event{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte(fmt.Sprintf("/key/%d", i)),
				Value:       []byte(fmt.Sprintf("value-%d", i)),
				ModRevision: rev,
			},
		}
	}
	// Make the last event a DELETE to test type mapping.
	watchEvents[4].Type = clientv3.EventTypeDelete
	watchEvents[4].Kv.Value = nil

	watcher := &mockWatcher{
		responses: []clientv3.WatchResponse{
			{
				Header: etcdserverpb.ResponseHeader{Revision: 15},
				Events: watchEvents,
			},
		},
	}

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	// Take a full snapshot first to set lastDeltaRev = 10.
	if err := s.TakeFullSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	// Advance revision to 15.
	kv.revision = 15

	if err := s.TakeDeltaSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeDeltaSnapshot: %v", err)
	}

	if !watcher.watchCalled {
		t.Fatal("expected watcher.Watch to be called")
	}

	// Verify the uploaded delta contains exactly 5 NDJSON events.
	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	var deltaSnap snapstore.SnapInfo
	var found bool
	for _, snap := range snaps {
		if snap.Kind == snapstore.SnapKindDelta {
			deltaSnap = snap
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected a delta snapshot in the list")
	}

	rc, err := store.Download(context.Background(), deltaSnap.Name)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	events, err := ReadEvents(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}

	// Verify the first 4 are PUT and the last is DELETE.
	for i := 0; i < 4; i++ {
		if events[i].Type != EventTypePut {
			t.Fatalf("event %d: expected PUT, got %s", i, events[i].Type)
		}
		expectedKey := fmt.Sprintf("/key/%d", i)
		if string(events[i].Key) != expectedKey {
			t.Fatalf("event %d Key: got %q, want %q", i, events[i].Key, expectedKey)
		}
	}
	if events[4].Type != EventTypeDelete {
		t.Fatalf("event 4: expected DELETE, got %s", events[4].Type)
	}
	if events[4].Revision != 15 {
		t.Fatalf("event 4 Revision: got %d, want 15", events[4].Revision)
	}

	// Verify lastDeltaRev was advanced.
	s.mu.Lock()
	if s.lastDeltaRev != 15 {
		t.Fatalf("expected lastDeltaRev 15, got %d", s.lastDeltaRev)
	}
	s.mu.Unlock()
}

func TestTakeDeltaSnapshot_SkipsWhenNoNewRevisions(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	kv := &mockKV{revision: 50}
	maint := &mockMaintenance{snapshotData: []byte("full")}
	lock := &mockLock{}
	watcher := &mockWatcher{}
	logger := zap.NewNop()

	s := New(kv, maint, watcher, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	// Take full to set lastDeltaRev = 50.
	if err := s.TakeFullSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeFullSnapshot: %v", err)
	}

	// Revision unchanged -- delta should be skipped, no watch call.
	if err := s.TakeDeltaSnapshot(context.Background()); err != nil {
		t.Fatalf("TakeDeltaSnapshot: %v", err)
	}

	if watcher.watchCalled {
		t.Fatal("expected watcher.Watch not to be called when revision unchanged")
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Only the full snapshot should exist; no delta uploaded.
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot (full only), got %d", len(snaps))
	}
}
