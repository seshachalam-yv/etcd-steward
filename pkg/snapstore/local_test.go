// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func makeSnap(kind string, startRev, lastRev int64, snapDir string) Snapshot {
	snap := Snapshot{
		Kind:          kind,
		StartRevision: startRev,
		LastRevision:  lastRev,
		CreatedOn:     time.Unix(0, lastRev*1_000_000_000).UTC(),
		SnapDir:       snapDir,
	}
	snap.SnapName = FormatSnapshotName(snap)
	return snap
}

func readAll(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	rc.Close() //nolint:errcheck
	return data
}

func TestLocalSnapstore_SaveAndFetch(t *testing.T) {
	baseDir := t.TempDir()
	store := NewLocal(baseDir)

	snap := makeSnap("Full", 0, 42, "backups/v1")
	data := []byte("snapshot-payload-full")

	err := store.Save(snap, io.NopCloser(bytes.NewReader(data)))
	if err != nil {
		t.Fatalf("Save error: %v", err)
	}

	rc, err := store.Fetch(snap)
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	got := readAll(t, rc)
	if !bytes.Equal(got, data) {
		t.Errorf("Fetch returned %q, want %q", got, data)
	}
}

func TestLocalSnapstore_List_SortedByLastRevision(t *testing.T) {
	baseDir := t.TempDir()
	store := NewLocal(baseDir)

	snapA := makeSnap("Full", 0, 100, "backups")
	snapB := makeSnap("Incremental", 100, 200, "backups")
	snapC := makeSnap("Incremental", 200, 300, "backups")

	// Save in reverse order to confirm sorting is not insertion-order dependent.
	for _, s := range []Snapshot{snapC, snapA, snapB} {
		if err := store.Save(s, io.NopCloser(bytes.NewReader([]byte(s.SnapName)))); err != nil {
			t.Fatalf("Save error: %v", err)
		}
	}

	list, err := store.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(list))
	}

	if list[0].LastRevision != 100 {
		t.Errorf("list[0].LastRevision = %d, want 100", list[0].LastRevision)
	}
	if list[1].LastRevision != 200 {
		t.Errorf("list[1].LastRevision = %d, want 200", list[1].LastRevision)
	}
	if list[2].LastRevision != 300 {
		t.Errorf("list[2].LastRevision = %d, want 300", list[2].LastRevision)
	}
}

func TestLocalSnapstore_Delete(t *testing.T) {
	baseDir := t.TempDir()
	store := NewLocal(baseDir)

	snapA := makeSnap("Full", 0, 10, "backups")
	snapB := makeSnap("Incremental", 10, 20, "backups")

	for _, s := range []Snapshot{snapA, snapB} {
		if err := store.Save(s, io.NopCloser(bytes.NewReader([]byte("data")))); err != nil {
			t.Fatalf("Save error: %v", err)
		}
	}

	if err := store.Delete(snapB); err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	if _, err := store.Fetch(snapB); err == nil {
		t.Error("expected error fetching deleted snapshot, got nil")
	}

	list, err := store.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 snapshot after delete, got %d", len(list))
	}
}

func TestParseSnapshotName_Valid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantKind string
		wantStart int64
		wantLast  int64
	}{
		{
			name:      "full snapshot",
			input:     "Full-0000000000000001-0000000000000042-1717000000000000000",
			wantKind:  "Full",
			wantStart: 1,
			wantLast:  42,
		},
		{
			name:      "incremental snapshot",
			input:     "Incremental-0000000000000100-0000000000000200-1717000000000000000",
			wantKind:  "Incremental",
			wantStart: 100,
			wantLast:  200,
		},
		{
			name:      "full snapshot with zstd extension",
			input:     "Full-0000000000000000-0000000000000004-1775676838670629376.zst",
			wantKind:  "Full",
			wantStart: 0,
			wantLast:  4,
		},
		{
			name:      "incremental snapshot with gzip extension",
			input:     "Incremental-0000000000000004-0000000000000010-1775676900000000000.gz",
			wantKind:  "Incremental",
			wantStart: 4,
			wantLast:  10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snap, err := ParseSnapshotName(tc.input)
			if err != nil {
				t.Fatalf("ParseSnapshotName(%q) error: %v", tc.input, err)
			}
			if snap.Kind != tc.wantKind {
				t.Errorf("Kind = %q, want %q", snap.Kind, tc.wantKind)
			}
			if snap.StartRevision != tc.wantStart {
				t.Errorf("StartRevision = %d, want %d", snap.StartRevision, tc.wantStart)
			}
			if snap.LastRevision != tc.wantLast {
				t.Errorf("LastRevision = %d, want %d", snap.LastRevision, tc.wantLast)
			}
		})
	}
}

func TestParseSnapshotName_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "wrong format", input: "not-a-snapshot"},
		{name: "unknown kind", input: "Delta-0000000000000001-0000000000000042-1717000000000000000"},
		{name: "bad start rev", input: "Full-notanumber-0000000000000042-1717000000000000000"},
		{name: "too few parts", input: "Full-0000000000000001-0000000000000042"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSnapshotName(tc.input)
			if err == nil {
				t.Errorf("ParseSnapshotName(%q) expected error, got nil", tc.input)
			}
		})
	}
}

func TestFormatSnapshotName_RoundTrip(t *testing.T) {
	snap := Snapshot{
		Kind:          "Full",
		StartRevision: 1,
		LastRevision:  42,
		CreatedOn:     time.Unix(0, 1717000000000000000).UTC(),
	}
	name := FormatSnapshotName(snap)
	parsed, err := ParseSnapshotName(name)
	if err != nil {
		t.Fatalf("ParseSnapshotName(%q) error: %v", name, err)
	}
	if parsed.Kind != snap.Kind || parsed.StartRevision != snap.StartRevision || parsed.LastRevision != snap.LastRevision {
		t.Errorf("round-trip mismatch: got %+v, want %+v", parsed, snap)
	}
}

func TestLocalSnapstore_TmpFilesSkippedInList(t *testing.T) {
	baseDir := t.TempDir()
	store := NewLocal(baseDir)

	snap := makeSnap("Full", 0, 10, "backups")
	if err := store.Save(snap, io.NopCloser(bytes.NewReader([]byte("data")))); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	// Create a .tmp file that should be skipped
	tmpPath := filepath.Join(baseDir, "backups", snap.SnapName+".tmp")
	if err := os.WriteFile(tmpPath, []byte("partial"), 0600); err != nil {
		t.Fatalf("failed to create tmp file: %v", err)
	}

	list, err := store.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 snapshot (tmp skipped), got %d", len(list))
	}
}

func TestLocalSnapstore_List_CompressedFilenames(t *testing.T) {
	// Verify that List() correctly parses snapshot files that have compression extensions
	// (e.g. .zst, .gz) appended to the snapshot name by the snapshotter.
	baseDir := t.TempDir()
	store := NewLocal(baseDir)

	// Simulate a .zst-compressed snapshot written by the snapshotter.
	zstSnap := makeSnap("Full", 0, 100, "Backup-v1")
	zstSnap.SnapName = zstSnap.SnapName + ".zst"
	if err := store.Save(zstSnap, io.NopCloser(bytes.NewReader([]byte("compressed-data")))); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	// Simulate a .gz-compressed incremental snapshot.
	gzSnap := makeSnap("Incremental", 100, 200, "Backup-v1")
	gzSnap.SnapName = gzSnap.SnapName + ".gz"
	if err := store.Save(gzSnap, io.NopCloser(bytes.NewReader([]byte("gz-data")))); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	list, err := store.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(list))
	}

	if list[0].Kind != "Full" {
		t.Errorf("list[0].Kind = %q, want Full", list[0].Kind)
	}
	if list[0].LastRevision != 100 {
		t.Errorf("list[0].LastRevision = %d, want 100", list[0].LastRevision)
	}
	if list[1].Kind != "Incremental" {
		t.Errorf("list[1].Kind = %q, want Incremental", list[1].Kind)
	}
}


func TestNewSnapstore_Local(t *testing.T) {
	baseDir := t.TempDir()
	s, err := NewSnapstore(SnapstoreConfig{Provider: "Local", Container: baseDir})
	if err != nil {
		t.Fatalf("NewSnapstore error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil snapstore")
	}
}

func TestNewSnapstore_UnsupportedProviders(t *testing.T) {
	for _, provider := range []string{"S3", "GCS", "ABS"} {
		t.Run(provider, func(t *testing.T) {
			_, err := NewSnapstore(SnapstoreConfig{Provider: provider})
			if err == nil {
				t.Errorf("NewSnapstore(provider=%q) expected error, got nil", provider)
			}
		})
	}
}
