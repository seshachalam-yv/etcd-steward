// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
)

func TestUploadListRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   100,
		CreatedAt:     now,
	}

	payload := []byte("snapshot-data-full")
	result, err := store.Upload(ctx, info, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if result.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), result.Size)
	}
	if result.Name == "" {
		t.Fatal("expected non-empty name")
	}
	if result.Kind != SnapKindFull {
		t.Fatalf("expected kind %q, got %q", SnapKindFull, result.Kind)
	}

	// Upload a second snapshot to test list ordering.
	info2 := SnapInfo{
		Kind:          SnapKindDelta,
		StartRevision: 101,
		EndRevision:   200,
		CreatedAt:     now.Add(time.Second),
	}
	_, err = store.Upload(ctx, info2, bytes.NewReader([]byte("delta-data")))
	if err != nil {
		t.Fatalf("Upload delta: %v", err)
	}

	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}

	// Verify ascending order by CreatedAt.
	if !snaps[0].CreatedAt.Before(snaps[1].CreatedAt) {
		t.Fatal("expected snapshots sorted by CreatedAt ascending")
	}
	if snaps[0].Kind != SnapKindFull {
		t.Fatalf("expected first snapshot kind %q, got %q", SnapKindFull, snaps[0].Kind)
	}
	if snaps[1].Kind != SnapKindDelta {
		t.Fatalf("expected second snapshot kind %q, got %q", SnapKindDelta, snaps[1].Kind)
	}
}

func TestDownload(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	ctx := context.Background()
	payload := []byte("downloadable-content")
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   50,
		CreatedAt:     time.Now().UTC().Truncate(time.Second),
	}

	result, err := store.Upload(ctx, info, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := store.Download(ctx, result.Name)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("expected %q, got %q", payload, got)
	}
}

func TestGetInfo(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	info := SnapInfo{
		Kind:          SnapKindDelta,
		StartRevision: 10,
		EndRevision:   20,
		CreatedAt:     now,
	}

	payload := []byte("delta-snapshot-data")
	result, err := store.Upload(ctx, info, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	got, err := store.GetInfo(ctx, result.Name)
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if got.Kind != SnapKindDelta {
		t.Fatalf("expected kind %q, got %q", SnapKindDelta, got.Kind)
	}
	if got.StartRevision != 10 {
		t.Fatalf("expected start revision 10, got %d", got.StartRevision)
	}
	if got.EndRevision != 20 {
		t.Fatalf("expected end revision 20, got %d", got.EndRevision)
	}
	if got.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), got.Size)
	}
	if got.Prefix != dir {
		t.Fatalf("expected prefix %q, got %q", dir, got.Prefix)
	}
	if !got.CreatedAt.Equal(now) {
		t.Fatalf("expected CreatedAt %v, got %v", now, got.CreatedAt)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	ctx := context.Background()
	info := SnapInfo{
		Kind:          SnapKindFull,
		StartRevision: 1,
		EndRevision:   10,
		CreatedAt:     time.Now().UTC().Truncate(time.Second),
	}

	result, err := store.Upload(ctx, info, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	if err := store.Delete(ctx, result.Name); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify it's gone.
	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots after delete, got %d", len(snaps))
	}
}

func TestListEmpty(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	snaps, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots for empty store, got %d", len(snaps))
	}
}

func TestDownloadNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	_, err = store.Download(context.Background(), "nonexistent-file")
	if err == nil {
		t.Fatal("expected error for missing snapshot")
	}

	var stewardErr *errors.Error
	if !errors.As(err, &stewardErr) {
		t.Fatalf("expected *errors.Error, got %T", err)
	}
	if stewardErr.Code() != errors.ErrCodeNotFound {
		t.Fatalf("expected ErrCodeNotFound, got %d", stewardErr.Code())
	}
}

func TestDeleteNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}

	err = store.Delete(context.Background(), "nonexistent-file")
	if err == nil {
		t.Fatal("expected error for missing snapshot")
	}

	var stewardErr *errors.Error
	if !errors.As(err, &stewardErr) {
		t.Fatalf("expected *errors.Error, got %T", err)
	}
	if stewardErr.Code() != errors.ErrCodeNotFound {
		t.Fatalf("expected ErrCodeNotFound, got %d", stewardErr.Code())
	}
}
