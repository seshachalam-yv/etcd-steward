// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/etcdclient"
	"github.com/gardener/etcd-steward/internal/snapshotter"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// --- mock types used across integration tests ---

// mockKV implements etcdclient.KV and records all Put and Delete calls.
type mockKV struct {
	revision int64
	getErr   error
	putErr   error
	deleteErr error
	calls    []kvCall
}

type kvCall struct {
	op    string // "put" or "delete"
	key   string
	value string
}

var _ etcdclient.KV = (*mockKV)(nil)

func (m *mockKV) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: m.revision},
	}, nil
}

func (m *mockKV) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	m.calls = append(m.calls, kvCall{op: "put", key: key, value: val})
	return &clientv3.PutResponse{}, nil
}

func (m *mockKV) Delete(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	m.calls = append(m.calls, kvCall{op: "delete", key: key})
	return &clientv3.DeleteResponse{}, nil
}

func (m *mockKV) Compact(_ context.Context, _ int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, nil
}

// mockMaintenance implements etcdclient.Maintenance for integration tests.
type mockMaintenance struct {
	snapshotData []byte
	snapshotErr  error
}

var _ etcdclient.Maintenance = (*mockMaintenance)(nil)

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

// mockWatcher implements snapshotter.Watcher for integration tests.
type mockWatcher struct {
	responses   []clientv3.WatchResponse
	watchCalled bool
}

var _ snapshotter.Watcher = (*mockWatcher)(nil)

func (m *mockWatcher) Watch(_ context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	m.watchCalled = true
	ch := make(chan clientv3.WatchResponse, len(m.responses))
	for _, r := range m.responses {
		ch <- r
	}
	close(ch)
	return ch
}

// mockLock implements snapshotter.DistributedLock.
type mockLock struct{}

var _ snapshotter.DistributedLock = (*mockLock)(nil)

func (m *mockLock) Acquire(_ context.Context) error { return nil }
func (m *mockLock) Release(_ context.Context) error { return nil }

// --- helper functions ---

// newLocalStore creates a LocalSnapStore rooted at a temporary directory.
func newLocalStore(t *testing.T) *snapstore.LocalSnapStore {
	t.Helper()
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	return store
}

// uploadSnapshot uploads arbitrary data as a snapshot to the store.
func uploadSnapshot(t *testing.T, store *snapstore.LocalSnapStore, info snapstore.SnapInfo, data []byte) snapstore.SnapInfo {
	t.Helper()
	result, err := store.Upload(context.Background(), info, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	return result
}

// uploadDeltaWithEvents creates a delta snapshot containing NDJSON-encoded events.
func uploadDeltaWithEvents(t *testing.T, store *snapstore.LocalSnapStore, info snapstore.SnapInfo, events []snapshotter.Event) snapstore.SnapInfo {
	t.Helper()
	var buf bytes.Buffer
	if err := snapshotter.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents: %v", err)
	}
	result, err := store.Upload(context.Background(), info, &buf)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	return result
}

// uploadCompressedDeltaWithEvents creates a compressed delta snapshot containing
// NDJSON-encoded events.
func uploadCompressedDeltaWithEvents(
	t *testing.T,
	store *snapstore.LocalSnapStore,
	info snapstore.SnapInfo,
	events []snapshotter.Event,
	algo compression.Algorithm,
) snapstore.SnapInfo {
	t.Helper()
	var buf bytes.Buffer
	if err := snapshotter.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents: %v", err)
	}
	compressed, err := compression.Compress(&buf, algo)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	compressedData, err := io.ReadAll(compressed)
	if err != nil {
		t.Fatalf("ReadAll compressed: %v", err)
	}
	result, err := store.Upload(context.Background(), info, bytes.NewReader(compressedData))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	return result
}

// makeWatchEvents builds a slice of clientv3.Event for testing the Watcher mock.
func makeWatchEvents(count int, startRevision int64) []*clientv3.Event {
	events := make([]*clientv3.Event, count)
	for i := range count {
		rev := startRevision + int64(i)
		events[i] = &clientv3.Event{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte("key-" + time.Now().Format("150405") + "-" + string(rune('a'+i))),
				Value:       []byte("value"),
				ModRevision: rev,
			},
		}
	}
	return events
}
