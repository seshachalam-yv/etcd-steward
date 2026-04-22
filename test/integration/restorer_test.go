// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/compression"
	"github.com/gardener/etcd-steward/internal/restorer"
	"github.com/gardener/etcd-steward/internal/snapshotter"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.uber.org/zap"
)

// TestRestorerFullAndDeltaCycle tests the complete restore workflow:
// 1. A LocalSnapStore is seeded with a full snapshot (fake .db file)
// 2. Three delta snapshots with NDJSON events are uploaded
// 3. FindLatestFullSnapshot locates the full snapshot
// 4. FindDeltaSnapshots returns deltas after the full's revision
// 5. ApplyDeltas replays all events in correct order via a mock KV
func TestRestorerFullAndDeltaCycle(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	logger := zap.NewNop()

	// Step 1: Upload a full snapshot (fake .db file content).
	fullInfo := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   100,
		CreatedAt:     now,
	}
	uploadSnapshot(t, store, fullInfo, []byte("fake-etcd-db-content-v1"))

	// Step 2: Upload 3 delta snapshots with NDJSON events.

	// Delta 1: revisions 101-103
	delta1Events := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("/app/config"), Value: []byte("v1"), Revision: 101},
		{Type: snapshotter.EventTypePut, Key: []byte("/app/secret"), Value: []byte("s1"), Revision: 102},
		{Type: snapshotter.EventTypePut, Key: []byte("/app/deploy"), Value: []byte("d1"), Revision: 103},
	}
	d1 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 101,
		EndRevision:   103,
		CreatedAt:     now.Add(1 * time.Second),
	}, delta1Events)

	// Delta 2: revisions 104-106 (includes a DELETE)
	delta2Events := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("/app/service"), Value: []byte("svc1"), Revision: 104},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/app/secret"), Revision: 105},
		{Type: snapshotter.EventTypePut, Key: []byte("/app/ingress"), Value: []byte("ing1"), Revision: 106},
	}
	d2 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 104,
		EndRevision:   106,
		CreatedAt:     now.Add(2 * time.Second),
	}, delta2Events)

	// Delta 3: revisions 107-108
	delta3Events := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("/app/endpoint"), Value: []byte("ep1"), Revision: 107},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/app/config"), Revision: 108},
	}
	d3 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 107,
		EndRevision:   108,
		CreatedAt:     now.Add(3 * time.Second),
	}, delta3Events)

	// Step 3: Find the latest full snapshot.
	r := restorer.New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	fullSnap, err := r.FindLatestFullSnapshot(ctx)
	if err != nil {
		t.Fatalf("FindLatestFullSnapshot: %v", err)
	}
	if fullSnap.EndRevision != 100 {
		t.Fatalf("expected full EndRevision=100, got %d", fullSnap.EndRevision)
	}

	// Step 4: Find delta snapshots after the full's revision.
	deltas, err := r.FindDeltaSnapshots(ctx, fullSnap.EndRevision)
	if err != nil {
		t.Fatalf("FindDeltaSnapshots: %v", err)
	}
	if len(deltas) != 3 {
		t.Fatalf("expected 3 delta snapshots, got %d", len(deltas))
	}

	// Verify delta ordering by StartRevision (ascending).
	if deltas[0].StartRevision != 101 {
		t.Fatalf("expected first delta StartRevision=101, got %d", deltas[0].StartRevision)
	}
	if deltas[1].StartRevision != 104 {
		t.Fatalf("expected second delta StartRevision=104, got %d", deltas[1].StartRevision)
	}
	if deltas[2].StartRevision != 107 {
		t.Fatalf("expected third delta StartRevision=107, got %d", deltas[2].StartRevision)
	}

	// Step 5: ApplyDeltas with currentRevision = fullSnap.EndRevision.
	kv := &mockKV{}
	if err := r.ApplyDeltas(ctx, kv, fullSnap.EndRevision, []snapstore.SnapInfo{d1, d2, d3}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	// All 8 events should be applied (all revisions > 100).
	if len(kv.calls) != 8 {
		t.Fatalf("expected 8 KV calls, got %d", len(kv.calls))
	}

	// Verify events were applied in order across all delta snapshots.
	expected := []kvCall{
		{op: "put", key: "/app/config", value: "v1"},
		{op: "put", key: "/app/secret", value: "s1"},
		{op: "put", key: "/app/deploy", value: "d1"},
		{op: "put", key: "/app/service", value: "svc1"},
		{op: "delete", key: "/app/secret"},
		{op: "put", key: "/app/ingress", value: "ing1"},
		{op: "put", key: "/app/endpoint", value: "ep1"},
		{op: "delete", key: "/app/config"},
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

// TestRestorerSkipsAlreadyAppliedRevisions verifies that ApplyDeltas correctly
// skips events whose revisions have already been applied.
func TestRestorerSkipsAlreadyAppliedRevisions(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	logger := zap.NewNop()

	// Upload a full snapshot up to revision 50.
	uploadSnapshot(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   50,
		CreatedAt:     now,
	}, []byte("full-snap"))

	// Upload a delta spanning revisions 45-60 (some overlap with full).
	deltaEvents := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("old-1"), Value: []byte("v1"), Revision: 45},
		{Type: snapshotter.EventTypePut, Key: []byte("old-2"), Value: []byte("v2"), Revision: 50},
		{Type: snapshotter.EventTypePut, Key: []byte("new-1"), Value: []byte("v3"), Revision: 55},
		{Type: snapshotter.EventTypePut, Key: []byte("new-2"), Value: []byte("v4"), Revision: 60},
	}
	d1 := uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 45,
		EndRevision:   60,
		CreatedAt:     now.Add(time.Second),
	}, deltaEvents)

	r := restorer.New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	kv := &mockKV{}
	// currentRevision=50 means revisions 45 and 50 should be skipped.
	if err := r.ApplyDeltas(ctx, kv, 50, []snapstore.SnapInfo{d1}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	if len(kv.calls) != 2 {
		t.Fatalf("expected 2 KV calls (only rev 55 and 60), got %d", len(kv.calls))
	}
	if kv.calls[0].key != "new-1" || kv.calls[0].value != "v3" {
		t.Fatalf("call 0: expected new-1=v3, got %s=%s", kv.calls[0].key, kv.calls[0].value)
	}
	if kv.calls[1].key != "new-2" || kv.calls[1].value != "v4" {
		t.Fatalf("call 1: expected new-2=v4, got %s=%s", kv.calls[1].key, kv.calls[1].value)
	}
}

// TestRestorerFindDeltaSnapshots_FiltersCorrectly verifies that
// FindDeltaSnapshots only returns deltas whose EndRevision is greater than
// the specified afterRevision, and sorts them by StartRevision ascending.
func TestRestorerFindDeltaSnapshots_FiltersCorrectly(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	logger := zap.NewNop()

	// Upload a full and 4 deltas with varying revision ranges.
	uploadSnapshot(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   100,
		CreatedAt:     now,
	}, []byte("full"))

	for i := range 4 {
		start := int64(101 + i*50)
		end := start + 49
		events := []snapshotter.Event{
			{Type: snapshotter.EventTypePut, Key: []byte(fmt.Sprintf("key-%d", i)), Value: []byte("val"), Revision: end},
		}
		uploadDeltaWithEvents(t, store, snapstore.SnapInfo{
			Kind:          snapstore.SnapKindDelta,
			StartRevision: start,
			EndRevision:   end,
			CreatedAt:     now.Add(time.Duration(i+1) * time.Second),
		}, events)
	}

	r := restorer.New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	// Request deltas after revision 200.
	// Delta 1: 101-150 (EndRevision=150 <= 200, excluded)
	// Delta 2: 151-200 (EndRevision=200 <= 200, excluded)
	// Delta 3: 201-250 (EndRevision=250 > 200, included)
	// Delta 4: 251-300 (EndRevision=300 > 200, included)
	deltas, err := r.FindDeltaSnapshots(ctx, 200)
	if err != nil {
		t.Fatalf("FindDeltaSnapshots: %v", err)
	}
	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas after revision 200, got %d", len(deltas))
	}
	if deltas[0].StartRevision != 201 {
		t.Fatalf("expected first delta StartRevision=201, got %d", deltas[0].StartRevision)
	}
	if deltas[1].StartRevision != 251 {
		t.Fatalf("expected second delta StartRevision=251, got %d", deltas[1].StartRevision)
	}
}

// TestRestorerMultipleFullSnapshots_FindsLatest verifies that when multiple
// full snapshots exist, FindLatestFullSnapshot returns the most recent one.
func TestRestorerMultipleFullSnapshots_FindsLatest(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	logger := zap.NewNop()

	// Upload 3 full snapshots at different times.
	for i := range 3 {
		uploadSnapshot(t, store, snapstore.SnapInfo{
			Kind:          snapstore.SnapKindFull,
			StartRevision: 0,
			EndRevision:   int64((i + 1) * 100),
			CreatedAt:     now.Add(time.Duration(i) * time.Second),
		}, []byte(fmt.Sprintf("full-snap-%d", i)))
	}

	r := restorer.New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	fullSnap, err := r.FindLatestFullSnapshot(ctx)
	if err != nil {
		t.Fatalf("FindLatestFullSnapshot: %v", err)
	}
	if fullSnap.EndRevision != 300 {
		t.Fatalf("expected EndRevision=300 (latest), got %d", fullSnap.EndRevision)
	}
}

// TestRestorerNoFullSnapshots_ReturnsError verifies that FindLatestFullSnapshot
// returns an error when no full snapshots exist in the store.
func TestRestorerNoFullSnapshots_ReturnsError(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()
	logger := zap.NewNop()

	r := restorer.New(store, compression.AlgorithmNone, t.TempDir(), t.TempDir(), logger)

	_, err := r.FindLatestFullSnapshot(ctx)
	if err == nil {
		t.Fatal("expected error when no full snapshots exist")
	}
}

// TestRestorerApplyDeltasWithCompression verifies that compressed delta
// snapshots are correctly decompressed and applied.
func TestRestorerApplyDeltasWithCompression(t *testing.T) {
	store := newLocalStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	logger := zap.NewNop()

	// Upload a full snapshot.
	uploadSnapshot(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   50,
		CreatedAt:     now,
	}, []byte("full"))

	// Upload a gzip-compressed delta.
	deltaEvents := []snapshotter.Event{
		{Type: snapshotter.EventTypePut, Key: []byte("compressed-key-1"), Value: []byte("val1"), Revision: 51},
		{Type: snapshotter.EventTypePut, Key: []byte("compressed-key-2"), Value: []byte("val2"), Revision: 52},
	}
	d1 := uploadCompressedDeltaWithEvents(t, store, snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 51,
		EndRevision:   52,
		CreatedAt:     now.Add(time.Second),
		IsCompressed:  true,
	}, deltaEvents, compression.AlgorithmGzip)

	r := restorer.New(store, compression.AlgorithmGzip, t.TempDir(), t.TempDir(), logger)

	kv := &mockKV{}
	if err := r.ApplyDeltas(ctx, kv, 50, []snapstore.SnapInfo{d1}); err != nil {
		t.Fatalf("ApplyDeltas: %v", err)
	}

	if len(kv.calls) != 2 {
		t.Fatalf("expected 2 KV calls, got %d", len(kv.calls))
	}
	if kv.calls[0].key != "compressed-key-1" || kv.calls[0].value != "val1" {
		t.Fatalf("call 0: expected compressed-key-1=val1, got %s=%s", kv.calls[0].key, kv.calls[0].value)
	}
	if kv.calls[1].key != "compressed-key-2" || kv.calls[1].value != "val2" {
		t.Fatalf("call 1: expected compressed-key-2=val2, got %s=%s", kv.calls[1].key, kv.calls[1].value)
	}
}
