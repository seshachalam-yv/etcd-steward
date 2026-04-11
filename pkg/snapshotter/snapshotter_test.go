// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapshotter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// fakeSnapshotAPI implements EtcdSnapshotAPI.
type fakeSnapshotAPI struct {
	data []byte
	err  error
}

func (f *fakeSnapshotAPI) Snapshot(_ context.Context) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

// fakeWatchAPI implements EtcdWatchAPI.
type fakeWatchAPI struct {
	events []*clientv3.Event
}

func (f *fakeWatchAPI) Watch(_ context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	ch := make(chan clientv3.WatchResponse, 1)
	go func() {
		defer close(ch)
		if len(f.events) > 0 {
			ch <- clientv3.WatchResponse{Events: f.events}
		}
	}()
	return ch
}

// fakeStatusAPI implements EtcdStatusAPI.
type fakeStatusAPI struct {
	revision int64
}

func (f *fakeStatusAPI) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{
		Header: &pb.ResponseHeader{Revision: f.revision},
	}, nil
}

// mockSnapstore records saved snapshots for testing.
type mockSnapstore struct {
	saved []snapstore.Snapshot
}

func (m *mockSnapstore) Save(snap snapstore.Snapshot, r io.ReadCloser) error {
	defer r.Close() //nolint:errcheck
	m.saved = append(m.saved, snap)
	return nil
}
func (m *mockSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockSnapstore) List() ([]snapstore.Snapshot, error) { return nil, nil }
func (m *mockSnapstore) Delete(_ snapstore.Snapshot) error   { return nil }

// noopCompressor is a no-op compressor for testing.
type noopCompressor struct{}

func (n *noopCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &nopWriteCloser{w: w}, nil
}
func (n *noopCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}
func (n *noopCompressor) FileExtension() string { return "" }

type nopWriteCloser struct{ w io.Writer }

func (n *nopWriteCloser) Write(p []byte) (int, error) { return n.w.Write(p) }
func (n *nopWriteCloser) Close() error                { return nil }

func TestRunDeltaSnapshotLoop_CancelledImmediately(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{},
		&fakeStatusAPI{revision: 1},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	done := make(chan struct{})
	go func() {
		s.RunDeltaSnapshotLoop(ctx, 1*time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RunDeltaSnapshotLoop did not stop on cancelled context")
	}
}

func TestRunDeltaSnapshotLoop_ErrNotLeaderSilent(_ *testing.T) {
	// Non-leader: delta snapshot returns ErrNotLeader — loop must not crash.
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{},
		&fakeStatusAPI{revision: 1},
		func() bool { return false }, // not leader
		nil,
		nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// Must not panic or log error for ErrNotLeader.
	s.RunDeltaSnapshotLoop(ctx, 5*time.Millisecond)
}

func TestRunDeltaSnapshotLoop_ErrNoFullSnapshot_Silent(_ *testing.T) {
	// Leader but no full snapshot taken — delta returns ErrNoFullSnapshot.
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{},
		&fakeStatusAPI{revision: 1},
		func() bool { return true }, // is leader
		nil,
		nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// No full snapshot taken first → ErrNoFullSnapshot should be handled silently.
	s.RunDeltaSnapshotLoop(ctx, 5*time.Millisecond)
}

func TestRunFullSnapshotSchedule_CancelledImmediately(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{},
		&fakeStatusAPI{revision: 5},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before starting

	done := make(chan struct{})
	go func() {
		s.RunFullSnapshotSchedule(ctx, "24h")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RunFullSnapshotSchedule did not stop on cancelled context")
	}
}

func TestTriggerFullSnapshot_Success(t *testing.T) {
	store := &mockSnapstore{}
	snapAPI := &fakeSnapshotAPI{data: []byte("etcd-snapshot-data")}
	kvAPI := &fakeStatusAPI{revision: 42}

	s := New(
		store,
		&noopCompressor{},
		"etcd-main", "default",
		snapAPI,
		&fakeWatchAPI{},
		kvAPI,
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	snap, err := s.TriggerFullSnapshot(context.Background(), false)
	if err != nil {
		t.Fatalf("TriggerFullSnapshot error: %v", err)
	}

	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if snap.Kind != "Full" {
		t.Errorf("Kind = %q, want Full", snap.Kind)
	}
	if snap.LastRevision != 42 {
		t.Errorf("LastRevision = %d, want 42", snap.LastRevision)
	}
	if len(store.saved) != 1 {
		t.Fatalf("expected 1 saved snapshot, got %d", len(store.saved))
	}
}

func TestTriggerFullSnapshot_NotLeader(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{},
		&fakeStatusAPI{revision: 100},
		func() bool { return false },
		nil,
		nil,
		zap.NewNop(),
	)

	_, err := s.TriggerFullSnapshot(context.Background(), false)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got: %v", err)
	}
}

func TestTriggerDeltaSnapshot_Success(t *testing.T) {
	store := &mockSnapstore{}
	events := []*clientv3.Event{
		{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte("key1"),
				Value:       []byte("value1"),
				ModRevision: 43,
			},
		},
		{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte("key2"),
				Value:       []byte("value2"),
				ModRevision: 44,
			},
		},
	}

	s := New(
		store,
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{events: events},
		&fakeStatusAPI{revision: 42},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	// Set lastRevision so delta knows where to start.
	s.lastRevision = 42

	snap, err := s.TriggerDeltaSnapshot(context.Background())
	if err != nil {
		t.Fatalf("TriggerDeltaSnapshot error: %v", err)
	}

	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if snap.Kind != "Incremental" {
		t.Errorf("Kind = %q, want Incremental", snap.Kind)
	}
	if snap.StartRevision != 42 {
		t.Errorf("StartRevision = %d, want 42", snap.StartRevision)
	}
	if snap.LastRevision != 44 {
		t.Errorf("LastRevision = %d, want 44", snap.LastRevision)
	}
}

func TestTriggerDeltaSnapshot_NotLeader(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{},
		&fakeWatchAPI{},
		&fakeStatusAPI{},
		func() bool { return false },
		nil,
		nil,
		zap.NewNop(),
	)
	s.lastRevision = 10

	_, err := s.TriggerDeltaSnapshot(context.Background())
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got: %v", err)
	}
}

func TestTriggerDeltaSnapshot_NoFullSnapshotYet(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{},
		&fakeWatchAPI{},
		&fakeStatusAPI{},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)
	// lastRevision is 0 (no full snapshot taken).

	_, err := s.TriggerDeltaSnapshot(context.Background())
	if !errors.Is(err, ErrNoFullSnapshot) {
		t.Fatalf("expected ErrNoFullSnapshot, got: %v", err)
	}
}

func TestTriggerDeltaSnapshot_NoEvents(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{},
		&fakeWatchAPI{events: nil}, // No events.
		&fakeStatusAPI{},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)
	s.lastRevision = 42

	snap, err := s.TriggerDeltaSnapshot(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if snap != nil {
		t.Error("expected nil snapshot when no events")
	}
}

func TestTriggerFullSnapshot_UpdatesLastRevision(t *testing.T) {
	store := &mockSnapstore{}
	snapAPI := &fakeSnapshotAPI{data: []byte("data")}
	kvAPI := &fakeStatusAPI{revision: 100}

	s := New(
		store,
		&noopCompressor{},
		"etcd-main", "default",
		snapAPI,
		&fakeWatchAPI{},
		kvAPI,
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	if _, err := s.TriggerFullSnapshot(context.Background(), false); err != nil {
		t.Fatalf("TriggerFullSnapshot error: %v", err)
	}

	// Wait a moment for any goroutines.
	time.Sleep(10 * time.Millisecond)

	s.mu.Lock()
	if s.lastRevision != 100 {
		t.Errorf("lastRevision = %d, want 100", s.lastRevision)
	}
	s.mu.Unlock()
}

func TestProvideInfo_BeforeAnySnapshot(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{},
		&fakeStatusAPI{revision: 10},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	info := s.ProvideInfo()
	if info.Snapshots != nil {
		t.Error("expected nil Snapshots before any snapshot taken")
	}
}

func TestProvideInfo_AfterFullSnapshot(t *testing.T) {
	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("etcd-data")},
		&fakeWatchAPI{},
		&fakeStatusAPI{revision: 42},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	if _, err := s.TriggerFullSnapshot(context.Background(), false); err != nil {
		t.Fatalf("TriggerFullSnapshot error: %v", err)
	}

	info := s.ProvideInfo()
	if info.Snapshots == nil {
		t.Fatal("expected non-nil Snapshots after full snapshot")
	}
	if info.Snapshots.LastFull == nil {
		t.Fatal("expected LastFull to be set")
	}
	if info.Snapshots.LastFull.EndRevision != 42 {
		t.Errorf("LastFull.EndRevision = %d, want 42", info.Snapshots.LastFull.EndRevision)
	}
	if info.Snapshots.AccumulatedDeltaSize == nil {
		t.Error("AccumulatedDeltaSize should be set (to zero) after full snapshot")
	} else if !info.Snapshots.AccumulatedDeltaSize.IsZero() {
		t.Errorf("AccumulatedDeltaSize should be zero after full snapshot, got %v", info.Snapshots.AccumulatedDeltaSize)
	}
}

func TestProvideInfo_FullThenDelta_DeltaAccumulates(t *testing.T) {
	events := []*clientv3.Event{
		{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key: []byte("k"), Value: []byte("v"), ModRevision: 50,
			},
		},
	}

	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("snap")},
		&fakeWatchAPI{events: events},
		&fakeStatusAPI{revision: 42},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	// Take full snapshot first.
	if _, err := s.TriggerFullSnapshot(context.Background(), false); err != nil {
		t.Fatalf("TriggerFullSnapshot error: %v", err)
	}

	// Take delta.
	if _, err := s.TriggerDeltaSnapshot(context.Background()); err != nil {
		t.Fatalf("TriggerDeltaSnapshot error: %v", err)
	}

	info := s.ProvideInfo()
	if info.Snapshots == nil {
		t.Fatal("expected Snapshots to be set")
	}
	if info.Snapshots.LastDelta == nil {
		t.Fatal("expected LastDelta to be set after delta snapshot")
	}
	if info.Snapshots.LastDelta.EndRevision != 50 {
		t.Errorf("LastDelta.EndRevision = %d, want 50", info.Snapshots.LastDelta.EndRevision)
	}
	if info.Snapshots.AccumulatedDeltaSize == nil || info.Snapshots.AccumulatedDeltaSize.IsZero() {
		t.Error("AccumulatedDeltaSize should be non-zero after delta snapshot")
	}
}

func TestProvideInfo_FullAfterDelta_ResetsAccumulation(t *testing.T) {
	events := []*clientv3.Event{
		{
			Type: clientv3.EventTypePut,
			Kv:   &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("v"), ModRevision: 50},
		},
	}

	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("snap")},
		&fakeWatchAPI{events: events},
		&fakeStatusAPI{revision: 42},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	// Full → delta → full.
	if _, err := s.TriggerFullSnapshot(context.Background(), false); err != nil {
		t.Fatalf("first TriggerFullSnapshot error: %v", err)
	}
	if _, err := s.TriggerDeltaSnapshot(context.Background()); err != nil {
		t.Fatalf("TriggerDeltaSnapshot error: %v", err)
	}
	// Second full snapshot should reset delta accumulation.
	if _, err := s.TriggerFullSnapshot(context.Background(), false); err != nil {
		t.Fatalf("second TriggerFullSnapshot error: %v", err)
	}

	info := s.ProvideInfo()
	if info.Snapshots == nil || info.Snapshots.AccumulatedDeltaSize == nil {
		t.Fatal("expected AccumulatedDeltaSize to be set")
	}
	if !info.Snapshots.AccumulatedDeltaSize.IsZero() {
		t.Errorf("AccumulatedDeltaSize should reset to zero after second full snapshot, got %v",
			info.Snapshots.AccumulatedDeltaSize)
	}
}

// TestTriggerDeltaSnapshot_FiltersEventsAlreadyCoveredByFullSnapshot verifies that
// if TriggerFullSnapshot is called between the start and end of a TriggerDeltaSnapshot
// watch, events already covered by the new full snapshot are filtered out of the delta.
// This prevents the delta's StartRevision from being lower than the latest full snapshot.
func TestTriggerDeltaSnapshot_FiltersEventsAlreadyCoveredByFullSnapshot(t *testing.T) {
	store := &mockSnapstore{}

	// Events from rev 43..46 — but a full snapshot at rev 44 will be "taken" mid-watch.
	events := []*clientv3.Event{
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k1"), Value: []byte("v1"), ModRevision: 43}},
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k2"), Value: []byte("v2"), ModRevision: 44}},
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k3"), Value: []byte("v3"), ModRevision: 45}},
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k4"), Value: []byte("v4"), ModRevision: 46}},
	}

	s := New(
		store,
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{events: events},
		&fakeStatusAPI{revision: 44},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	// Simulate: last full snapshot was at rev 42; then a new full snapshot was taken at rev 44
	// (concurrent with the delta watch starting at startRev=42).
	s.lastRevision = 42
	s.lastFullRevision = 44 // full snapshot at 44 happened concurrently

	snap, err := s.TriggerDeltaSnapshot(context.Background())
	if err != nil {
		t.Fatalf("TriggerDeltaSnapshot error: %v", err)
	}
	if snap == nil {
		t.Fatal("expected non-nil snapshot (events 45,46 are beyond fullRev=44)")
	}

	// StartRevision must reflect the latest full snapshot, not the old lastRevision.
	if snap.StartRevision != 44 {
		t.Errorf("StartRevision = %d, want 44 (latest full snapshot rev)", snap.StartRevision)
	}
	if snap.LastRevision != 46 {
		t.Errorf("LastRevision = %d, want 46", snap.LastRevision)
	}

	// Verify lastRevision advanced to 46.
	s.mu.Lock()
	gotLastRev := s.lastRevision
	s.mu.Unlock()
	if gotLastRev != 46 {
		t.Errorf("s.lastRevision = %d, want 46", gotLastRev)
	}
}

// TestTriggerDeltaSnapshot_AllEventsFilteredByFullSnapshot verifies that if ALL watched
// events are already covered by a concurrent full snapshot, TriggerDeltaSnapshot returns nil.
func TestTriggerDeltaSnapshot_AllEventsFilteredByFullSnapshot(t *testing.T) {
	events := []*clientv3.Event{
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k1"), Value: []byte("v1"), ModRevision: 43}},
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k2"), Value: []byte("v2"), ModRevision: 44}},
	}

	s := New(
		&mockSnapstore{},
		&noopCompressor{},
		"etcd-main", "default",
		&fakeSnapshotAPI{data: []byte("data")},
		&fakeWatchAPI{events: events},
		&fakeStatusAPI{revision: 45},
		func() bool { return true },
		nil,
		nil,
		zap.NewNop(),
	)

	s.lastRevision = 42
	s.lastFullRevision = 45 // full snapshot at 45 covers all watched events (43,44)

	snap, err := s.TriggerDeltaSnapshot(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap != nil {
		t.Errorf("expected nil snapshot (all events covered by full at rev 45), got %+v", snap)
	}
}
