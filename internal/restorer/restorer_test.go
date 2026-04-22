// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package restorer

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/snapshotter"
	"github.com/gardener/etcd-steward/internal/snapstore"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// --- mock KV that records Put and Delete calls ---

type kvCall struct {
	op    string // "put" or "delete"
	key   string
	value string // empty for delete
}

type mockKV struct {
	calls []kvCall
}

func (m *mockKV) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return nil, nil
}

func (m *mockKV) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	m.calls = append(m.calls, kvCall{op: "put", key: key, value: val})
	return &clientv3.PutResponse{}, nil
}

func (m *mockKV) Delete(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	m.calls = append(m.calls, kvCall{op: "delete", key: key})
	return &clientv3.DeleteResponse{}, nil
}

func (m *mockKV) Compact(_ context.Context, _ int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, nil
}

// --- helpers ---

func setupStore(t *testing.T, infos []snapstore.SnapInfo) *snapstore.LocalSnapStore {
	t.Helper()
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	ctx := context.Background()
	for _, info := range infos {
		_, err := store.Upload(ctx, info, bytes.NewReader([]byte("snapshot-data")))
		if err != nil {
			t.Fatalf("Upload: %v", err)
		}
	}
	return store
}

// uploadDeltaWithEvents creates a delta snapshot containing NDJSON-encoded events
// and uploads it to the given store. It returns the resulting SnapInfo.
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

// --- existing tests ---

func TestFindLatestFullSnapshot(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(time.Second)},
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 200, CreatedAt: now.Add(2 * time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	snap, err := r.FindLatestFullSnapshot(context.Background())
	if err != nil {
		t.Fatalf("FindLatestFullSnapshot: %v", err)
	}
	if snap.EndRevision != 200 {
		t.Fatalf("expected EndRevision 200, got %d", snap.EndRevision)
	}
}

func TestFindLatestFullSnapshotNoFulls(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindDelta, StartRevision: 1, EndRevision: 50, CreatedAt: now},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	_, err := r.FindLatestFullSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected error when no full snapshots exist")
	}
	if !strings.Contains(err.Error(), "no full snapshots found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindDeltaSnapshots(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(time.Second)},
		{Kind: snapstore.SnapKindDelta, StartRevision: 151, EndRevision: 200, CreatedAt: now.Add(2 * time.Second)},
		{Kind: snapstore.SnapKindDelta, StartRevision: 201, EndRevision: 250, CreatedAt: now.Add(3 * time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	deltas, err := r.FindDeltaSnapshots(context.Background(), 150)
	if err != nil {
		t.Fatalf("FindDeltaSnapshots: %v", err)
	}
	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas after revision 150, got %d", len(deltas))
	}
	if deltas[0].StartRevision != 151 {
		t.Fatalf("expected first delta StartRevision 151, got %d", deltas[0].StartRevision)
	}
	if deltas[1].StartRevision != 201 {
		t.Fatalf("expected second delta StartRevision 201, got %d", deltas[1].StartRevision)
	}
}

func TestFindDeltaSnapshotsNoneMatch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	infos := []snapstore.SnapInfo{
		{Kind: snapstore.SnapKindFull, StartRevision: 0, EndRevision: 100, CreatedAt: now},
		{Kind: snapstore.SnapKindDelta, StartRevision: 101, EndRevision: 150, CreatedAt: now.Add(time.Second)},
	}

	store := setupStore(t, infos)
	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	deltas, err := r.FindDeltaSnapshots(context.Background(), 200)
	if err != nil {
		t.Fatalf("FindDeltaSnapshots: %v", err)
	}
	if len(deltas) != 0 {
		t.Fatalf("expected 0 deltas after revision 200, got %d", len(deltas))
	}
}

func TestRestoreFullFailsNoSnapshots(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	err = r.RestoreFull(context.Background())
	if err == nil {
		t.Fatal("expected error when no snapshots exist")
	}
	if !strings.Contains(err.Error(), "no full snapshots found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Task 3: ApplyDeltas tests ---

func TestApplyDeltas_ReplaysPutEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	events := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("key1"), Value: []byte("val1"), Revision: 10},
		{Type: snapshotter.EventTypePut, Key: []byte("key2"), Value: []byte("val2"), Revision: 11},
		{Type: snapshotter.EventTypePut, Key: []byte("key3"), Value: []byte("val3"), Revision: 12},
	}

	info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 10,
		EndRevision:   12,
		CreatedAt:     now,
	}
	result := uploadDeltaWithEvents(t, store, info, events)

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	kv := &mockKV{}
	if err := r.ApplyDeltas(context.Background(), kv, 0, []snapstore.SnapInfo{result}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	if len(kv.calls) != 3 {
		t.Fatalf("expected 3 KV calls, got %d", len(kv.calls))
	}
	for i, c := range kv.calls {
		if c.op != "put" {
			t.Fatalf("call %d: expected op=put, got %s", i, c.op)
		}
	}
	if kv.calls[0].key != "key1" || kv.calls[0].value != "val1" {
		t.Fatalf("call 0: expected key1=val1, got %s=%s", kv.calls[0].key, kv.calls[0].value)
	}
	if kv.calls[1].key != "key2" || kv.calls[1].value != "val2" {
		t.Fatalf("call 1: expected key2=val2, got %s=%s", kv.calls[1].key, kv.calls[1].value)
	}
	if kv.calls[2].key != "key3" || kv.calls[2].value != "val3" {
		t.Fatalf("call 2: expected key3=val3, got %s=%s", kv.calls[2].key, kv.calls[2].value)
	}
}

func TestApplyDeltas_ReplaysDeleteEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	events := []snapshotter.Event{
		{Type: snapshotter.EventTypeDelete, Key: []byte("key-a"), Revision: 20},
		{Type: snapshotter.EventTypeDelete, Key: []byte("key-b"), Revision: 21},
	}

	info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 20,
		EndRevision:   21,
		CreatedAt:     now,
	}
	result := uploadDeltaWithEvents(t, store, info, events)

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	kv := &mockKV{}
	if err := r.ApplyDeltas(context.Background(), kv, 0, []snapstore.SnapInfo{result}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	if len(kv.calls) != 2 {
		t.Fatalf("expected 2 KV calls, got %d", len(kv.calls))
	}
	for i, c := range kv.calls {
		if c.op != "delete" {
			t.Fatalf("call %d: expected op=delete, got %s", i, c.op)
		}
	}
	if kv.calls[0].key != "key-a" {
		t.Fatalf("call 0: expected key-a, got %s", kv.calls[0].key)
	}
	if kv.calls[1].key != "key-b" {
		t.Fatalf("call 1: expected key-b, got %s", kv.calls[1].key)
	}
}

func TestApplyDeltas_SkipsAlreadyAppliedRevisions(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	events := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("old1"), Value: []byte("v1"), Revision: 5},
		{Type: snapshotter.EventTypePut, Key: []byte("old2"), Value: []byte("v2"), Revision: 10},
		{Type: snapshotter.EventTypePut, Key: []byte("new1"), Value: []byte("v3"), Revision: 15},
	}

	info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 5,
		EndRevision:   15,
		CreatedAt:     now,
	}
	result := uploadDeltaWithEvents(t, store, info, events)

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	kv := &mockKV{}
	// currentRevision=10 means only revision 15 should be applied.
	if err := r.ApplyDeltas(context.Background(), kv, 10, []snapstore.SnapInfo{result}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	if len(kv.calls) != 1 {
		t.Fatalf("expected 1 KV call (only rev 15), got %d", len(kv.calls))
	}
	if kv.calls[0].key != "new1" || kv.calls[0].value != "v3" {
		t.Fatalf("expected new1=v3, got %s=%s", kv.calls[0].key, kv.calls[0].value)
	}
}

func TestApplyDeltas_MultipleSnapshots(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)

	// Delta 1: revisions 1-3
	events1 := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("k1"), Value: []byte("v1"), Revision: 1},
		{Type: snapshotter.EventTypePut, Key: []byte("k2"), Value: []byte("v2"), Revision: 2},
		{Type: snapshotter.EventTypePut, Key: []byte("k3"), Value: []byte("v3"), Revision: 3},
	}
	d1 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 1,
		EndRevision:   3,
		CreatedAt:     now,
	}, events1)

	// Delta 2: revisions 4-5
	events2 := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("k4"), Value: []byte("v4"), Revision: 4},
		{Type: snapshotter.EventTypeDelete, Key: []byte("k1"), Revision: 5},
	}
	d2 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 4,
		EndRevision:   5,
		CreatedAt:     now.Add(time.Second),
	}, events2)

	// Delta 3: revisions 6-7
	events3 := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("k5"), Value: []byte("v5"), Revision: 6},
		{Type: snapshotter.EventTypePut, Key: []byte("k6"), Value: []byte("v6"), Revision: 7},
	}
	d3 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 6,
		EndRevision:   7,
		CreatedAt:     now.Add(2 * time.Second),
	}, events3)

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	kv := &mockKV{}
	if err := r.ApplyDeltas(context.Background(), kv, 0, []snapstore.SnapInfo{d1, d2, d3}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	// Total: 3 puts from d1 + 1 put + 1 delete from d2 + 2 puts from d3 = 7 calls
	if len(kv.calls) != 7 {
		t.Fatalf("expected 7 KV calls, got %d", len(kv.calls))
	}

	// Verify order: k1,k2,k3 (puts), k4 (put), k1 (delete), k5,k6 (puts)
	expected := []kvCall{
		{op: "put", key: "k1", value: "v1"},
		{op: "put", key: "k2", value: "v2"},
		{op: "put", key: "k3", value: "v3"},
		{op: "put", key: "k4", value: "v4"},
		{op: "delete", key: "k1"},
		{op: "put", key: "k5", value: "v5"},
		{op: "put", key: "k6", value: "v6"},
	}
	for i, exp := range expected {
		if kv.calls[i].op != exp.op {
			t.Fatalf("call %d: expected op=%s, got %s", i, exp.op, kv.calls[i].op)
		}
		if kv.calls[i].key != exp.key {
			t.Fatalf("call %d: expected key=%s, got %s", i, exp.key, kv.calls[i].key)
		}
		if exp.op == "put" && kv.calls[i].value != exp.value {
			t.Fatalf("call %d: expected value=%s, got %s", i, exp.value, kv.calls[i].value)
		}
	}
}

// --- Task 4: Integration test ---

func TestRestoreFullThenApplyDeltas(t *testing.T) {
	dir := t.TempDir()
	store, err := snapstore.NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)

	// Upload a full snapshot (content doesn't matter for this test since we
	// only verify that ApplyDeltas replays events correctly after a full
	// restore scenario).
	fullInfo := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   100,
		CreatedAt:     now,
	}
	_, err = store.Upload(context.Background(), fullInfo, bytes.NewReader([]byte("full-snapshot-data")))
	if err != nil {
		t.Fatalf("Upload full: %v", err)
	}

	// Delta 1: PUT events for keys written after the full snapshot.
	events1 := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("/app/key1"), Value: []byte("value1"), Revision: 101},
		{Type: snapshotter.EventTypePut, Key: []byte("/app/key2"), Value: []byte("value2"), Revision: 102},
		{Type: snapshotter.EventTypePut, Key: []byte("/app/key3"), Value: []byte("value3"), Revision: 103},
	}
	d1 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 101,
		EndRevision:   103,
		CreatedAt:     now.Add(time.Second),
	}, events1)

	// Delta 2: mixed PUT and DELETE events.
	events2 := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("/app/key4"), Value: []byte("value4"), Revision: 104},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/app/key1"), Revision: 105},
		{Type: snapshotter.EventTypePut, Key: []byte("/app/key5"), Value: []byte("value5"), Revision: 106},
	}
	d2 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 104,
		EndRevision:   106,
		CreatedAt:     now.Add(2 * time.Second),
	}, events2)

	logger := zap.NewNop()
	r := New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	// Verify we can find the full snapshot.
	fullSnap, err := r.FindLatestFullSnapshot(context.Background())
	if err != nil {
		t.Fatalf("FindLatestFullSnapshot: %v", err)
	}
	if fullSnap.EndRevision != 100 {
		t.Fatalf("expected full snapshot EndRevision 100, got %d", fullSnap.EndRevision)
	}

	// Verify we can find delta snapshots after the full.
	deltas, err := r.FindDeltaSnapshots(context.Background(), fullSnap.EndRevision)
	if err != nil {
		t.Fatalf("FindDeltaSnapshots: %v", err)
	}
	if len(deltas) != 2 {
		t.Fatalf("expected 2 delta snapshots, got %d", len(deltas))
	}

	// Apply deltas with currentRevision matching the full snapshot's end revision.
	kv := &mockKV{}
	if err := r.ApplyDeltas(context.Background(), kv, fullSnap.EndRevision, []snapstore.SnapInfo{d1, d2}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	// All 6 events should be applied (all revisions > 100).
	if len(kv.calls) != 6 {
		t.Fatalf("expected 6 KV calls, got %d", len(kv.calls))
	}

	// Verify events were applied in order across both delta snapshots.
	expected := []kvCall{
		{op: "put", key: "/app/key1", value: "value1"},
		{op: "put", key: "/app/key2", value: "value2"},
		{op: "put", key: "/app/key3", value: "value3"},
		{op: "put", key: "/app/key4", value: "value4"},
		{op: "delete", key: "/app/key1"},
		{op: "put", key: "/app/key5", value: "value5"},
	}
	for i, exp := range expected {
		if kv.calls[i].op != exp.op {
			t.Fatalf("call %d: expected op=%s, got %s", i, exp.op, kv.calls[i].op)
		}
		if kv.calls[i].key != exp.key {
			t.Fatalf("call %d: expected key=%s, got %s", i, exp.key, kv.calls[i].key)
		}
		if exp.op == "put" && kv.calls[i].value != exp.value {
			t.Fatalf("call %d: expected value=%s, got %s", i, exp.value, kv.calls[i].value)
		}
	}
}
