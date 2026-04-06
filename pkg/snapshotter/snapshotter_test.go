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
				Key:            []byte("key1"),
				Value:          []byte("value1"),
				ModRevision:    43,
			},
		},
		{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:            []byte("key2"),
				Value:          []byte("value2"),
				ModRevision:    44,
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
		zap.NewNop(),
	)
	// lastRevision is 0 (no full snapshot taken).

	_, err := s.TriggerDeltaSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected error when no full snapshot taken, got nil")
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
