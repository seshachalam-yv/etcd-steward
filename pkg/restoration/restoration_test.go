// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package restoration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// mockSnapstore implements snapstore.Snapstore for testing.
type mockSnapstore struct {
	snaps []snapstore.Snapshot
	data  map[string][]byte // path -> data
}

func (m *mockSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (m *mockSnapstore) Fetch(snap snapstore.Snapshot) (io.ReadCloser, error) {
	key := snap.SnapDir + "/" + snap.SnapName
	if data, ok := m.data[key]; ok {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return nil, fmt.Errorf("snapshot not found: %s", key)
}
func (m *mockSnapstore) List() ([]snapstore.Snapshot, error) {
	return m.snaps, nil
}
func (m *mockSnapstore) Delete(_ snapstore.Snapshot) error { return nil }

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

func TestRestore_Success(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	dbContent := []byte("fake-etcd-db-content")

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{
				Kind:          "Full",
				StartRevision: 0,
				LastRevision:  100,
				SnapDir:       "backups",
				SnapName:      "Full-0000000000000000-0000000000000100-100000000000",
			},
		},
		data: map[string][]byte{
			"backups/Full-0000000000000000-0000000000000100-100000000000": dbContent,
		},
	}

	err := Restore(
		context.Background(),
		store,
		&noopCompressor{},
		dataDir,
		tempDir,
		"etcd-main-0",
		"https://etcd-main-0:2380",
		"etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("Restore error: %v", err)
	}

	// Verify the DB was written to the correct location.
	dbPath := filepath.Join(dataDir, "member", "snap", "db")
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read restored DB: %v", err)
	}
	if !bytes.Equal(data, dbContent) {
		t.Errorf("restored DB content mismatch: got %q, want %q", data, dbContent)
	}
}

func TestRestore_NoFullSnapshot(t *testing.T) {
	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{
				Kind:          "Incremental",
				StartRevision: 50,
				LastRevision:  100,
				SnapDir:       "backups",
				SnapName:      "Incremental-0000000000000050-0000000000000100-100000000000",
			},
		},
		data: make(map[string][]byte),
	}

	err := Restore(
		context.Background(),
		store,
		&noopCompressor{},
		t.TempDir(),
		t.TempDir(),
		"etcd-main-0",
		"https://etcd-main-0:2380",
		"etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop(),
	)
	if !errors.Is(err, ErrNoSnapshotFound) {
		t.Fatalf("expected ErrNoSnapshotFound, got: %v", err)
	}
}

func TestRestore_EmptyStore(t *testing.T) {
	store := &mockSnapstore{
		snaps: nil,
		data:  make(map[string][]byte),
	}

	err := Restore(
		context.Background(),
		store,
		&noopCompressor{},
		t.TempDir(),
		t.TempDir(),
		"etcd-main-0",
		"https://etcd-main-0:2380",
		"etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop(),
	)
	if !errors.Is(err, ErrNoSnapshotFound) {
		t.Fatalf("expected ErrNoSnapshotFound, got: %v", err)
	}
}

func TestRestore_WithDeltas(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	dbContent := []byte("fake-etcd-db-content-with-deltas")

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{
				Kind:          "Full",
				StartRevision: 0,
				LastRevision:  100,
				SnapDir:       "backups",
				SnapName:      "Full-0000000000000000-0000000000000100-100000000000",
			},
			{
				Kind:          "Incremental",
				StartRevision: 100,
				LastRevision:  200,
				SnapDir:       "backups",
				SnapName:      "Incremental-0000000000000100-0000000000000200-200000000000",
			},
		},
		data: map[string][]byte{
			"backups/Full-0000000000000000-0000000000000100-100000000000": dbContent,
		},
	}

	// Deltas are found but skipped (stubbed). No error expected.
	err := Restore(
		context.Background(),
		store,
		&noopCompressor{},
		dataDir,
		tempDir,
		"etcd-main-0",
		"https://etcd-main-0:2380",
		"etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("Restore error: %v", err)
	}

	dbPath := filepath.Join(dataDir, "member", "snap", "db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected restored DB file to exist")
	}
}
