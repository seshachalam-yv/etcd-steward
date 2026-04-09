// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package restoration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapshotter"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// TestMain enables skipHashCheck so unit tests can use fake bbolt DBs as snapshot
// content without requiring a valid etcd snapshot hash.
func TestMain(m *testing.M) {
	skipHashCheck = true
	os.Exit(m.Run())
}

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

// makeFakeEtcdDB creates a minimal bbolt DB file with the "key" and "meta" buckets
// that etcd restoration expects. Returns the path to the DB file.
func makeFakeEtcdDB(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to create test bbolt DB: %v", err)
	}
	defer db.Close() //nolint:errcheck

	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucket(keyBucketName); err != nil {
			return err
		}
		_, err := tx.CreateBucket(metaBucketName)
		return err
	}); err != nil {
		t.Fatalf("failed to create buckets in test DB: %v", err)
	}

	return dbPath
}

// readFakeEtcdDB reads the "key" bucket and returns all entries as a slice of KV pairs.
// Used to verify delta replay results.
func readFakeEtcdDBKeys(t *testing.T, dbPath string) map[int64][]byte {
	t.Helper()
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("failed to open test bbolt DB: %v", err)
	}
	defer db.Close() //nolint:errcheck

	result := make(map[int64][]byte)
	if err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(keyBucketName)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			// key is {8-byte big-endian mainRev}'_'{8-byte big-endian subRev}; extract mainRev
			if len(k) >= 17 {
				mainRev := int64(k[0])<<56 | int64(k[1])<<48 | int64(k[2])<<40 | int64(k[3])<<32 |
					int64(k[4])<<24 | int64(k[5])<<16 | int64(k[6])<<8 | int64(k[7])
				cp := make([]byte, len(v))
				copy(cp, v)
				result[mainRev] = cp
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("failed to read test bbolt DB: %v", err)
	}
	return result
}

// makeDeltaPayload serializes a slice of DeltaEvents as NDJSON bytes (as the snapshotter writes).
func makeDeltaPayload(events []snapshotter.DeltaEvent) []byte {
	var buf bytes.Buffer
	for _, ev := range events {
		line, _ := json.Marshal(ev)
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func TestRestore_Success(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	dbContent, err := os.ReadFile(makeFakeEtcdDB(t))
	if err != nil {
		t.Fatalf("failed to read fake DB: %v", err)
	}

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

	err = Restore(
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
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected restored DB file to exist")
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

// TestRestore_WithDeltas verifies that delta events are applied to the restored DB.
// After restore, the bbolt "key" bucket should contain entries for both the full
// snapshot and the delta events.
func TestRestore_WithDeltas(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	// Create a valid bbolt DB as the "full snapshot" content.
	fullDBContent, err := os.ReadFile(makeFakeEtcdDB(t))
	if err != nil {
		t.Fatalf("failed to read fake DB: %v", err)
	}

	// Create delta events: one PUT at rev 101, one DELETE at rev 102.
	deltaEvents := []snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/foo"), Value: []byte("bar"), ModRevision: 101, Version: 1},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/baz"), ModRevision: 102, Version: 0},
	}
	deltaPayload := makeDeltaPayload(deltaEvents)

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
				LastRevision:  102,
				SnapDir:       "backups",
				SnapName:      "Incremental-0000000000000100-0000000000000102-200000000000",
			},
		},
		data: map[string][]byte{
			"backups/Full-0000000000000000-0000000000000100-100000000000":        fullDBContent,
			"backups/Incremental-0000000000000100-0000000000000102-200000000000": deltaPayload,
		},
	}

	err = Restore(
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
	entries := readFakeEtcdDBKeys(t, dbPath)

	// Both delta events must be present (rev 101 and 102).
	if _, ok := entries[101]; !ok {
		t.Error("expected entry at revision 101 (PUT /foo) to be present in restored DB")
	}
	if _, ok := entries[102]; !ok {
		t.Error("expected entry at revision 102 (DELETE /baz) to be present in restored DB")
	}
}

// TestRestore_WithDeltas_MultipleSnapshots verifies that multiple delta snapshots
// are applied in order.
func TestRestore_WithDeltas_MultipleSnapshots(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	fullDBContent, err := os.ReadFile(makeFakeEtcdDB(t))
	if err != nil {
		t.Fatalf("failed to read fake DB: %v", err)
	}

	delta1 := makeDeltaPayload([]snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/a"), Value: []byte("1"), ModRevision: 101, Version: 1},
	})
	delta2 := makeDeltaPayload([]snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/b"), Value: []byte("2"), ModRevision: 201, Version: 1},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/a"), ModRevision: 202, Version: 0},
	})

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", StartRevision: 0, LastRevision: 100, SnapDir: "b", SnapName: "Full-0-100-1"},
			{Kind: "Incremental", StartRevision: 100, LastRevision: 101, SnapDir: "b", SnapName: "Incremental-100-101-2"},
			{Kind: "Incremental", StartRevision: 101, LastRevision: 202, SnapDir: "b", SnapName: "Incremental-101-202-3"},
		},
		data: map[string][]byte{
			"b/Full-0-100-1":          fullDBContent,
			"b/Incremental-100-101-2": delta1,
			"b/Incremental-101-202-3": delta2,
		},
	}

	if err := Restore(context.Background(), store, &noopCompressor{}, dataDir, tempDir,
		"etcd-main-0", "https://etcd-main-0:2380", "etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop()); err != nil {
		t.Fatalf("Restore error: %v", err)
	}

	entries := readFakeEtcdDBKeys(t, filepath.Join(dataDir, "member", "snap", "db"))
	for _, rev := range []int64{101, 201, 202} {
		if _, ok := entries[rev]; !ok {
			t.Errorf("expected entry at revision %d to be present", rev)
		}
	}
}

// TestRestore_DeltaOnlyAfterFull verifies that delta snapshots with StartRevision
// BEFORE the full snapshot are ignored.
func TestRestore_DeltaOnlyAfterFull(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	fullDBContent, err := os.ReadFile(makeFakeEtcdDB(t))
	if err != nil {
		t.Fatalf("failed to read fake DB: %v", err)
	}

	oldDelta := makeDeltaPayload([]snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/stale"), Value: []byte("x"), ModRevision: 50, Version: 1},
	})

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			// delta at rev 50 is BEFORE the full snapshot at rev 100 — must be skipped.
			{Kind: "Incremental", StartRevision: 30, LastRevision: 50, SnapDir: "b", SnapName: "Incremental-30-50-1"},
			{Kind: "Full", StartRevision: 0, LastRevision: 100, SnapDir: "b", SnapName: "Full-0-100-2"},
		},
		data: map[string][]byte{
			"b/Full-0-100-2":        fullDBContent,
			"b/Incremental-30-50-1": oldDelta,
		},
	}

	if err := Restore(context.Background(), store, &noopCompressor{}, dataDir, tempDir,
		"etcd-main-0", "https://etcd-main-0:2380", "etcd-main-0=https://etcd-main-0:2380",
		zap.NewNop()); err != nil {
		t.Fatalf("Restore error: %v", err)
	}

	entries := readFakeEtcdDBKeys(t, filepath.Join(dataDir, "member", "snap", "db"))
	if _, ok := entries[50]; ok {
		t.Error("stale delta at revision 50 should NOT have been applied (it predates the full snapshot)")
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

// TestWriteDeltaEventsToDB verifies PUT and DELETE events are correctly persisted.
func TestWriteDeltaEventsToDB(t *testing.T) {
	dbPath := makeFakeEtcdDB(t)

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	events := []snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/k1"), Value: []byte("v1"), ModRevision: 10, Version: 1},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/k2"), ModRevision: 11, Version: 0},
	}

	lastRev, err := writeDeltaEventsToDB(db, events)
	db.Close() //nolint:errcheck
	if err != nil {
		t.Fatalf("writeDeltaEventsToDB error: %v", err)
	}
	if lastRev != 11 {
		t.Errorf("lastRev = %d, want 11", lastRev)
	}

	entries := readFakeEtcdDBKeys(t, dbPath)
	if _, ok := entries[10]; !ok {
		t.Error("rev 10 (PUT /k1) not found in DB")
	}
	if _, ok := entries[11]; !ok {
		t.Error("rev 11 (DELETE /k2) not found in DB")
	}
}

// TestWriteDeltaEventsToDB_EmptyEvents returns 0 and no error for empty input.
func TestWriteDeltaEventsToDB_EmptyEvents(t *testing.T) {
	dbPath := makeFakeEtcdDB(t)
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close() //nolint:errcheck

	lastRev, err := writeDeltaEventsToDB(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lastRev != 0 {
		t.Errorf("lastRev = %d, want 0", lastRev)
	}
}

// TestEncodeRevision verifies that encoded revisions sort correctly (big-endian).
func TestEncodeRevision(t *testing.T) {
	r1 := encodeRevision(1, 0)
	r2 := encodeRevision(2, 0)
	r100 := encodeRevision(100, 0)

	if bytes.Compare(r1, r2) >= 0 {
		t.Error("rev(1) should be less than rev(2)")
	}
	if bytes.Compare(r2, r100) >= 0 {
		t.Error("rev(2) should be less than rev(100)")
	}
	if len(r1) != 17 {
		t.Errorf("revision key length = %d, want 17", len(r1))
	}
}

// TestReadDeltaEvents verifies that readDeltaEvents correctly deserializes events.
func TestReadDeltaEvents(t *testing.T) {
	events := []snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/a"), Value: []byte("1"), ModRevision: 5, Version: 1},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/b"), ModRevision: 6, Version: 0},
	}
	payload := makeDeltaPayload(events)

	store := &mockSnapstore{
		snaps: nil,
		data: map[string][]byte{
			"b/snap": payload,
		},
	}
	snap := snapstore.Snapshot{SnapDir: "b", SnapName: "snap"}

	got, err := readDeltaEvents(store, &noopCompressor{}, snap)
	if err != nil {
		t.Fatalf("readDeltaEvents error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0].ModRevision != 5 || string(got[0].Key) != "/a" {
		t.Errorf("event[0] mismatch: %+v", got[0])
	}
	if got[1].Type != snapshotter.EventTypeDelete || got[1].ModRevision != 6 {
		t.Errorf("event[1] mismatch: %+v", got[1])
	}
}

// TestRestore_DeduplicatesDeltasByLastRevision verifies that when the backup store contains
// multiple incremental snapshots with the same LastRevision (produced by the auto-delta loop
// firing concurrently with a manual trigger), Restore applies only one of them per unique
// LastRevision rather than replaying identical events multiple times.
func TestRestore_DeduplicatesDeltasByLastRevision(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	dbContent, err := os.ReadFile(makeFakeEtcdDB(t))
	if err != nil {
		t.Fatalf("failed to read fake DB: %v", err)
	}

	// Two incremental snapshots with identical LastRevision (20) — the auto-delta loop fired
	// twice covering the same range. Both should be deduplicated: only 1 applied.
	deltaEvents := []snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/k1"), Value: []byte("v1"), ModRevision: 18, Version: 1},
		{Type: snapshotter.EventTypePut, Key: []byte("/k2"), Value: []byte("v2"), ModRevision: 19, Version: 1},
		{Type: snapshotter.EventTypePut, Key: []byte("/k3"), Value: []byte("v3"), ModRevision: 20, Version: 1},
	}
	payload := makeDeltaPayload(deltaEvents)

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", StartRevision: 0, LastRevision: 17, SnapDir: "B1", SnapName: "Full-17"},
			// Two incremental files covering the same range 17→20.
			{Kind: "Incremental", StartRevision: 17, LastRevision: 20, SnapDir: "B2", SnapName: "Inc-20-first"},
			{Kind: "Incremental", StartRevision: 17, LastRevision: 20, SnapDir: "B3", SnapName: "Inc-20-second"},
		},
		data: map[string][]byte{
			"B1/Full-17":       dbContent,
			"B2/Inc-20-first":  payload,
			"B3/Inc-20-second": payload, // identical events — would cause double-write without dedup
		},
	}

	ctx := context.Background()
	if err := Restore(ctx, store, &noopCompressor{}, dataDir, tempDir,
		"etcd-0", "http://etcd-0:2380", "etcd-0=http://etcd-0:2380", zap.NewNop()); err != nil {
		t.Fatalf("Restore error: %v", err)
	}

	// Open the restored bbolt DB and count entries in the key bucket.
	dbPath := filepath.Join(dataDir, "member", "snap", "db")
	kvs := readFakeEtcdDBKeys(t, dbPath)

	// Exactly 3 revisions should be present (rev 18, 19, 20), not 6 (which would happen
	// if the duplicate delta was applied twice).
	if len(kvs) != 3 {
		t.Errorf("expected 3 key entries after dedup, got %d (duplicate delta may have been applied twice)", len(kvs))
	}
}

// TestRestore_FinalRevisionIsLastDeltaRevision verifies that after applying deltas, the
// restored DB contains entries up to the last delta's LastRevision (not just the full
// snapshot's revision). This proves the logging fix: finalRevision = last delta revision.
func TestRestore_FinalRevisionIsLastDeltaRevision(t *testing.T) {
	dataDir := t.TempDir()
	tempDir := t.TempDir()

	dbContent, err := os.ReadFile(makeFakeEtcdDB(t))
	if err != nil {
		t.Fatalf("failed to read fake DB: %v", err)
	}

	deltaEvents := []snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/k1"), Value: []byte("v1"), ModRevision: 51, Version: 1},
		{Type: snapshotter.EventTypePut, Key: []byte("/k2"), Value: []byte("v2"), ModRevision: 52, Version: 1},
	}
	payload := makeDeltaPayload(deltaEvents)

	store := &mockSnapstore{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", StartRevision: 0, LastRevision: 50, SnapDir: "B1", SnapName: "Full-50"},
			{Kind: "Incremental", StartRevision: 50, LastRevision: 52, SnapDir: "B2", SnapName: "Inc-50-52"},
		},
		data: map[string][]byte{
			"B1/Full-50":   dbContent,
			"B2/Inc-50-52": payload,
		},
	}

	ctx := context.Background()
	if err := Restore(ctx, store, &noopCompressor{}, dataDir, tempDir,
		"etcd-0", "http://etcd-0:2380", "etcd-0=http://etcd-0:2380", zap.NewNop()); err != nil {
		t.Fatalf("Restore error: %v", err)
	}

	// Verify the DB has revisions 51 and 52 (from the delta), proving full+delta were applied.
	dbPath := filepath.Join(dataDir, "member", "snap", "db")
	kvs := readFakeEtcdDBKeys(t, dbPath)
	if _, ok := kvs[51]; !ok {
		t.Error("expected revision 51 in restored DB (from delta), not found")
	}
	if _, ok := kvs[52]; !ok {
		t.Error("expected revision 52 in restored DB (from delta), not found")
	}
}

// readFakeEtcdDBRaw returns all raw (bbolt key → value) entries from the "key" bucket.
// Unlike readFakeEtcdDBKeys, it includes tombstone entries (18-byte keys ending in 't').
func readFakeEtcdDBRaw(t *testing.T, dbPath string) map[string][]byte {
	t.Helper()
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("failed to open test bbolt DB: %v", err)
	}
	defer db.Close() //nolint:errcheck

	result := make(map[string][]byte)
	if err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(keyBucketName)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			cp := make([]byte, len(v))
			copy(cp, v)
			result[string(k)] = cp
			return nil
		})
	}); err != nil {
		t.Fatalf("failed to read test bbolt DB: %v", err)
	}
	return result
}

// TestWriteDeltaEventsToDB_DeleteWritesTombstone verifies that a DELETE event is written
// using an 18-byte tombstone key (17-byte revision + 't') rather than a plain 17-byte key.
// This matches etcd's internal MVCC tombstone format so the B-tree index is correctly rebuilt
// on startup — without the 't' suffix, etcd treats the entry as a ghost PUT and the deleted
// key remains visible.
func TestWriteDeltaEventsToDB_DeleteWritesTombstone(t *testing.T) {
	dbPath := makeFakeEtcdDB(t)

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	events := []snapshotter.DeltaEvent{
		{Type: snapshotter.EventTypePut, Key: []byte("/alive"), Value: []byte("yes"), CreateRevision: 10, ModRevision: 10, Version: 1},
		{Type: snapshotter.EventTypeDelete, Key: []byte("/dead"), ModRevision: 11},
	}

	_, err = writeDeltaEventsToDB(db, events)
	db.Close() //nolint:errcheck
	if err != nil {
		t.Fatalf("writeDeltaEventsToDB error: %v", err)
	}

	raw := readFakeEtcdDBRaw(t, dbPath)

	// PUT at rev 10: 17-byte key, no 't' suffix.
	if _, ok := raw[string(encodeRevision(10, 0))]; !ok {
		t.Error("expected 17-byte key entry for PUT at rev 10, not found")
	}
	// Same revision with 't' appended must NOT exist (PUT must not become tombstone).
	if _, ok := raw[string(encodeRevision(10, 0))+"t"]; ok {
		t.Error("PUT at rev 10 must NOT have a tombstone key (18-byte)")
	}

	// DELETE at rev 11: 18-byte tombstone key (17 + 't'), NOT a plain 17-byte key.
	if _, ok := raw[string(append(encodeRevision(11, 0), markTombstone))]; !ok {
		t.Errorf("expected 18-byte tombstone key for DELETE at rev 11, not found (ghost bug — 't' marker missing)")
	}
	if _, ok := raw[string(encodeRevision(11, 0))]; ok {
		t.Error("DELETE at rev 11 must NOT have a plain 17-byte key (would create ghost on etcd startup)")
	}
}

// TestWriteDeltaEventsToDB_PutPreservesCreateRevision verifies that PUT events written to the
// bbolt DB include the CreateRevision field. Without it, etcd reports create_revision=0 for
// keys that were only in incremental snapshots (not in the full snapshot).
func TestWriteDeltaEventsToDB_PutPreservesCreateRevision(t *testing.T) {
	dbPath := makeFakeEtcdDB(t)

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	events := []snapshotter.DeltaEvent{
		// key created at rev 5, updated at rev 8 — CreateRevision must be 5, not 8.
		{Type: snapshotter.EventTypePut, Key: []byte("/k"), Value: []byte("v"), CreateRevision: 5, ModRevision: 8, Version: 2},
	}

	_, err = writeDeltaEventsToDB(db, events)
	db.Close() //nolint:errcheck
	if err != nil {
		t.Fatalf("writeDeltaEventsToDB error: %v", err)
	}

	raw := readFakeEtcdDBRaw(t, dbPath)
	kvBytes, ok := raw[string(encodeRevision(8, 0))]
	if !ok {
		t.Fatal("expected entry at rev 8, not found")
	}

	var kv mvccpb.KeyValue
	if err := kv.Unmarshal(kvBytes); err != nil {
		t.Fatalf("unmarshal KeyValue: %v", err)
	}
	if kv.CreateRevision != 5 {
		t.Errorf("CreateRevision = %d, want 5 (key created at rev 5, updated at rev 8)", kv.CreateRevision)
	}
	if kv.ModRevision != 8 {
		t.Errorf("ModRevision = %d, want 8", kv.ModRevision)
	}
	if kv.Version != 2 {
		t.Errorf("Version = %d, want 2", kv.Version)
	}
}
