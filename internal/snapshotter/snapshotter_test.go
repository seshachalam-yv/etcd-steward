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
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmGzip, time.Hour, time.Minute, logger)

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

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Hour, logger)

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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Hour, logger)

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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

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
	logger := zap.NewNop()

	s := New(kv, maint, store, lock, compression.AlgorithmNone, time.Hour, time.Minute, logger)

	err = s.TakeDeltaSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected error on KV.Get failure")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("expected error to contain 'connection refused', got: %v", err)
	}
}
