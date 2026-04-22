// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// createValidDataDir sets up a minimal etcd data directory structure with a
// WAL directory and a valid bbolt DB file containing one bucket and one key.
func createValidDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	walDir := filepath.Join(dir, walDirName)
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("failed to create WAL dir: %v", err)
	}

	snapDir := filepath.Join(dir, "member/snap")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("failed to create snap dir: %v", err)
	}

	dbFile := filepath.Join(dir, dbFileName)
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		t.Fatalf("failed to create bbolt DB: %v", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("key1"), []byte("value1"))
	}); err != nil {
		t.Fatalf("failed to populate DB: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close DB: %v", err)
	}

	return dir
}

func TestValidator_SanityCheck_Valid(t *testing.T) {
	dir := createValidDataDir(t)
	logger := zap.NewNop()
	v := New(dir, logger)

	if err := v.SanityCheck(); err != nil {
		t.Fatalf("expected SanityCheck to pass, got: %v", err)
	}
}

func TestValidator_SanityCheck_NoWAL(t *testing.T) {
	dir := t.TempDir()
	// Create the snap dir + DB but no WAL dir.
	snapDir := filepath.Join(dir, "member/snap")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("failed to create snap dir: %v", err)
	}
	dbFile := filepath.Join(dir, dbFileName)
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		t.Fatalf("failed to create bbolt DB: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close DB: %v", err)
	}

	logger := zap.NewNop()
	v := New(dir, logger)

	if err := v.SanityCheck(); err == nil {
		t.Fatal("expected SanityCheck to fail for missing WAL dir, got nil")
	}
}

func TestValidator_SanityCheck_NoDB(t *testing.T) {
	dir := t.TempDir()
	// Create the WAL dir but no DB file.
	walDir := filepath.Join(dir, walDirName)
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("failed to create WAL dir: %v", err)
	}

	logger := zap.NewNop()
	v := New(dir, logger)

	if err := v.SanityCheck(); err == nil {
		t.Fatal("expected SanityCheck to fail for missing DB file, got nil")
	}
}

func TestValidator_FullCheck_ValidDB(t *testing.T) {
	dir := createValidDataDir(t)
	logger := zap.NewNop()
	v := New(dir, logger)

	if err := v.FullCheck(true); err != nil {
		t.Fatalf("expected FullCheck to pass, got: %v", err)
	}

	// Also test multi-node mode.
	if err := v.FullCheck(false); err != nil {
		t.Fatalf("expected FullCheck (multi-node) to pass, got: %v", err)
	}
}

func TestValidator_IsCorrupt_InvalidDB(t *testing.T) {
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "member/snap")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("failed to create snap dir: %v", err)
	}

	// Write garbage data to the DB file.
	dbFile := filepath.Join(dir, dbFileName)
	if err := os.WriteFile(dbFile, []byte("this is garbage data not a valid bbolt file"), 0600); err != nil {
		t.Fatalf("failed to write garbage DB: %v", err)
	}

	logger := zap.NewNop()
	v := New(dir, logger)

	if !v.IsCorrupt() {
		t.Fatal("expected IsCorrupt to return true for garbage DB file")
	}
}
