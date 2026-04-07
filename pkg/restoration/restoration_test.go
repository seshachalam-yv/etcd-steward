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

// errListSnapstore always returns an error from List.
type errListSnapstore struct{}

func (e *errListSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (e *errListSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (e *errListSnapstore) List() ([]snapstore.Snapshot, error) {
	return nil, fmt.Errorf("store list error")
}
func (e *errListSnapstore) Delete(_ snapstore.Snapshot) error { return nil }

func TestRestore_ListError(t *testing.T) {
	err := Restore(
		context.Background(),
		&errListSnapstore{},
		&noopCompressor{},
		t.TempDir(),
		t.TempDir(),
		"etcd-main-0",
		"https://etcd-main-0:2380",
		"etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("expected error from List, got nil")
	}
}

// errFetchSnapstore returns an error from Fetch.
type errFetchSnapstore struct {
	snaps []snapstore.Snapshot
}

func (e *errFetchSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (e *errFetchSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, fmt.Errorf("fetch error")
}
func (e *errFetchSnapstore) List() ([]snapstore.Snapshot, error) { return e.snaps, nil }
func (e *errFetchSnapstore) Delete(_ snapstore.Snapshot) error   { return nil }

func TestRestore_FetchError(t *testing.T) {
	store := &errFetchSnapstore{
		snaps: []snapstore.Snapshot{
			{
				Kind:          "Full",
				StartRevision: 0,
				LastRevision:  100,
				SnapDir:       "backups",
				SnapName:      "Full-0000000000000000-0000000000000100-100000000000",
			},
		},
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
	if err == nil {
		t.Fatal("expected error from Fetch, got nil")
	}
}

// TestCopyFile_Success tests that copyFile correctly copies content.
func TestCopyFile_Success(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")

	content := []byte("etcd-db-content-1234")
	if err := os.WriteFile(src, content, 0600); err != nil {
		t.Fatalf("failed to write src: %v", err)
	}

	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile error: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("failed to read dst: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("dst content mismatch: got %q, want %q", got, content)
	}
}

// TestCopyFile_SrcNotExist tests that copyFile returns an error when src doesn't exist.
func TestCopyFile_SrcNotExist(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "nonexistent.db")
	dst := filepath.Join(dir, "dst.db")

	if err := copyFile(src, dst); err == nil {
		t.Fatal("expected error for nonexistent src, got nil")
	}
}

// TestCopyFile_DstDirNotWritable tests that copyFile returns an error when dst dir is not writable.
func TestCopyFile_DstDirNotWritable(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	content := []byte("some-data")
	if err := os.WriteFile(src, content, 0600); err != nil {
		t.Fatalf("failed to write src: %v", err)
	}

	// Create a read-only subdirectory.
	roDir := filepath.Join(dir, "readonly")
	if err := os.Mkdir(roDir, 0500); err != nil {
		t.Fatalf("failed to create readonly dir: %v", err)
	}

	dst := filepath.Join(roDir, "dst.db")
	err := copyFile(src, dst)
	if err == nil {
		t.Fatal("expected error writing to read-only directory, got nil")
	}
}

// failingCompressor returns an error from Decompress.
type failingCompressor struct{}

func (f *failingCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &nopWriteCloser{w: w}, nil
}
func (f *failingCompressor) Decompress(_ io.Reader) (io.ReadCloser, error) {
	return nil, fmt.Errorf("decompression error")
}
func (f *failingCompressor) FileExtension() string { return "" }

func TestRestore_DecompressError(t *testing.T) {
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
			"backups/Full-0000000000000000-0000000000000100-100000000000": []byte("data"),
		},
	}

	err := Restore(
		context.Background(),
		store,
		&failingCompressor{},
		t.TempDir(),
		t.TempDir(),
		"etcd-main-0",
		"https://etcd-main-0:2380",
		"etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("expected error from Decompress, got nil")
	}
}

// errorReader always returns an error on Read, for testing io.Copy failures in copyFile.
type errorReader struct{}

func (e *errorReader) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("read error")
}

// failingFetchSnapstore returns a reader that errors on Read.
type failingFetchSnapstore struct {
	snaps []snapstore.Snapshot
}

func (f *failingFetchSnapstore) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (f *failingFetchSnapstore) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return io.NopCloser(&errorReader{}), nil
}
func (f *failingFetchSnapstore) List() ([]snapstore.Snapshot, error) { return f.snaps, nil }
func (f *failingFetchSnapstore) Delete(_ snapstore.Snapshot) error   { return nil }

func TestRestore_WriteTempDBError(t *testing.T) {
	store := &failingFetchSnapstore{
		snaps: []snapstore.Snapshot{
			{
				Kind:          "Full",
				StartRevision: 0,
				LastRevision:  100,
				SnapDir:       "backups",
				SnapName:      "Full-0000000000000000-0000000000000100-100000000000",
			},
		},
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
	if err == nil {
		t.Fatal("expected error from io.Copy during write, got nil")
	}
}
