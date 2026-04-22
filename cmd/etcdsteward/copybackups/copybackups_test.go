// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package copybackups

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/gardener/etcd-steward/internal/snapstore"
	"go.uber.org/zap"
)

func TestCopyBackups_Name(t *testing.T) {
	cmd := NewCommand()
	if cmd.Use != "copy-backups" {
		t.Errorf("expected command name %q, got %q", "copy-backups", cmd.Use)
	}
}

func TestCopyBackups_HasFlags(t *testing.T) {
	cmd := NewCommand()

	expectedFlags := []string{"source-prefix", "source-container", "dest-prefix", "dest-container", "source-provider", "dest-provider"}
	for _, name := range expectedFlags {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("expected flag %q to exist", name)
		}
	}
}

func TestCopyBackups_ProviderDefaults(t *testing.T) {
	cmd := NewCommand()

	srcProv := cmd.Flags().Lookup("source-provider")
	if srcProv == nil {
		t.Fatal("expected source-provider flag to exist")
	}
	if srcProv.DefValue != "Local" {
		t.Errorf("expected source-provider default %q, got %q", "Local", srcProv.DefValue)
	}

	dstProv := cmd.Flags().Lookup("dest-provider")
	if dstProv == nil {
		t.Fatal("expected dest-provider flag to exist")
	}
	if dstProv.DefValue != "Local" {
		t.Errorf("expected dest-provider default %q, got %q", "Local", dstProv.DefValue)
	}
}

func TestCopyBackups_CrossProvider(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	// Create source store with some snapshots.
	srcDir := t.TempDir()
	srcStore, err := snapstore.NewLocal(srcDir)
	if err != nil {
		t.Fatalf("NewLocal source: %v", err)
	}

	// Upload a full snapshot and two deltas.
	fullPayload := []byte("full-snapshot-data")
	fullInfo := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindFull,
		StartRevision: 0,
		EndRevision:   100,
		CreatedAt:     now,
	}
	fullResult, err := srcStore.Upload(ctx, fullInfo, bytes.NewReader(fullPayload))
	if err != nil {
		t.Fatalf("Upload full: %v", err)
	}

	delta1Payload := []byte("delta-1-events")
	delta1Info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 101,
		EndRevision:   150,
		CreatedAt:     now.Add(time.Second),
	}
	delta1Result, err := srcStore.Upload(ctx, delta1Info, bytes.NewReader(delta1Payload))
	if err != nil {
		t.Fatalf("Upload delta1: %v", err)
	}

	delta2Payload := []byte("delta-2-events")
	delta2Info := snapstore.SnapInfo{
		Kind:          snapstore.SnapKindDelta,
		StartRevision: 151,
		EndRevision:   200,
		CreatedAt:     now.Add(2 * time.Second),
	}
	delta2Result, err := srcStore.Upload(ctx, delta2Info, bytes.NewReader(delta2Payload))
	if err != nil {
		t.Fatalf("Upload delta2: %v", err)
	}

	// Create destination store (different directory simulates cross-provider).
	dstDir := t.TempDir()
	dstStore, err := snapstore.NewLocal(dstDir)
	if err != nil {
		t.Fatalf("NewLocal dest: %v", err)
	}

	logger := zap.NewNop()
	copied, skipped, err := CopySnapshots(ctx, srcStore, dstStore, logger)
	if err != nil {
		t.Fatalf("CopySnapshots: %v", err)
	}

	if copied != 3 {
		t.Fatalf("expected 3 copied, got %d", copied)
	}
	if skipped != 0 {
		t.Fatalf("expected 0 skipped, got %d", skipped)
	}

	// Verify all snapshots are in the destination.
	dstSnaps, err := dstStore.List(ctx)
	if err != nil {
		t.Fatalf("List dest: %v", err)
	}
	if len(dstSnaps) != 3 {
		t.Fatalf("expected 3 snapshots in dest, got %d", len(dstSnaps))
	}

	// Verify snapshot content matches.
	verifyContent := func(name string, expected []byte) {
		t.Helper()
		rc, err := dstStore.Download(ctx, name)
		if err != nil {
			t.Fatalf("Download %q from dest: %v", name, err)
		}
		defer func() { _ = rc.Close() }()
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll %q: %v", name, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("content mismatch for %q: got %q, want %q", name, got, expected)
		}
	}

	verifyContent(fullResult.Name, fullPayload)
	verifyContent(delta1Result.Name, delta1Payload)
	verifyContent(delta2Result.Name, delta2Payload)

	// Run again -- all should be skipped.
	copied2, skipped2, err := CopySnapshots(ctx, srcStore, dstStore, logger)
	if err != nil {
		t.Fatalf("CopySnapshots (second run): %v", err)
	}
	if copied2 != 0 {
		t.Fatalf("expected 0 copied on second run, got %d", copied2)
	}
	if skipped2 != 3 {
		t.Fatalf("expected 3 skipped on second run, got %d", skipped2)
	}
}
