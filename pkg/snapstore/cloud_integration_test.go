// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

//go:build integration

// Package snapstore provides integration tests for cloud snapstore backends.
// These tests require running cloud emulators or real cloud credentials.
//
// S3 (LocalStack):
//   Start:  docker run --rm -p 4566:4566 localstack/localstack
//   Run:    ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS_JSON=/tmp/s3creds.json \
//           go test -v -tags integration -run TestS3Snapstore ./pkg/snapstore/...
//
// GCS (fake-gcs-server):
//   Start:  docker run --rm -p 4443:4443 fsouza/fake-gcs-server -scheme http
//   Run:    STORAGE_EMULATOR_HOST=localhost:4443 \
//           go test -v -tags integration -run TestGCSSnapstore ./pkg/snapstore/...
//
// ABS (Azurite):
//   Start:  docker run --rm -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
//   Run:    ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS_JSON=/tmp/abscreds.json \
//           go test -v -tags integration -run TestABSSnapstore ./pkg/snapstore/...

package snapstore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

// roundTripTest exercises Save, Fetch, List, Delete on any Snapstore backend.
func roundTripTest(t *testing.T, store Snapstore) {
	t.Helper()

	// Save two snapshots.
	snapA := Snapshot{
		Kind:          "Full",
		StartRevision: 0,
		LastRevision:  100,
		CreatedOn:     time.Now().UTC(),
		SnapDir:       "Backup-integration",
		SnapName:      "Full-0000000000000000-0000000000000100-" + fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	snapB := Snapshot{
		Kind:          "Incremental",
		StartRevision: 100,
		LastRevision:  200,
		CreatedOn:     time.Now().UTC(),
		SnapDir:       "Backup-integration",
		SnapName:      "Incremental-0000000000000100-0000000000000200-" + fmt.Sprintf("%d", time.Now().UnixNano()+1),
	}

	dataA := []byte("full-snapshot-payload")
	dataB := []byte("delta-snapshot-payload")

	if err := store.Save(snapA, io.NopCloser(bytes.NewReader(dataA))); err != nil {
		t.Fatalf("Save(snapA) error: %v", err)
	}
	if err := store.Save(snapB, io.NopCloser(bytes.NewReader(dataB))); err != nil {
		t.Fatalf("Save(snapB) error: %v", err)
	}

	// Fetch snapA.
	rc, err := store.Fetch(snapA)
	if err != nil {
		t.Fatalf("Fetch(snapA) error: %v", err)
	}
	gotA, _ := io.ReadAll(rc)
	rc.Close() //nolint:errcheck
	if !bytes.Equal(gotA, dataA) {
		t.Errorf("Fetch(snapA): got %q, want %q", gotA, dataA)
	}

	// List — both should be present.
	list, err := store.List()
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(list) < 2 {
		t.Fatalf("List() returned %d snapshots, want >= 2", len(list))
	}

	// Delete snapB.
	if err := store.Delete(snapB); err != nil {
		t.Fatalf("Delete(snapB) error: %v", err)
	}

	// List after delete — snapB should be gone.
	list, err = store.List()
	if err != nil {
		t.Fatalf("List() after delete error: %v", err)
	}
	for _, s := range list {
		if s.SnapName == snapB.SnapName {
			t.Errorf("deleted snapshot %q still in List()", snapB.SnapName)
		}
	}

	// Clean up snapA.
	if err := store.Delete(snapA); err != nil {
		t.Logf("cleanup Delete(snapA) error (non-fatal): %v", err)
	}
}

// TestS3Snapstore_Integration tests the S3 backend against LocalStack.
// Requires ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS_JSON set to a JSON file with:
//
//	{"region":"us-east-1","accessKeyID":"test","secretAccessKey":"test","endpoint":"http://localhost:4566","s3ForcePathStyle":true,"bucketName":"test-bucket"}
func TestS3Snapstore_Integration(t *testing.T) {
	if os.Getenv("ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS_JSON") == "" &&
		os.Getenv("ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS") == "" {
		t.Skip("skipping S3 integration test: ETCD_STEWARD_AWS_APPLICATION_CREDENTIALS_JSON not set")
	}

	store, err := NewS3(SnapstoreConfig{
		Provider: "S3",
		Prefix:   "integration-test",
		TempDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewS3 error: %v", err)
	}

	roundTripTest(t, store)
}

// TestGCSSnapstore_Integration tests the GCS backend against fake-gcs-server.
// Requires STORAGE_EMULATOR_HOST=localhost:4443 and a container named "test-bucket".
func TestGCSSnapstore_Integration(t *testing.T) {
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("skipping GCS integration test: STORAGE_EMULATOR_HOST not set")
	}

	store, err := NewGCS(SnapstoreConfig{
		Provider:  "GCS",
		Container: "test-bucket",
		Prefix:    "integration-test",
		TempDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewGCS error: %v", err)
	}

	roundTripTest(t, store)
}

// TestABSSnapstore_Integration tests the ABS backend against Azurite.
// Requires ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS_JSON set to a JSON file with:
//
//	{"storageAccount":"devstoreaccount1","storageKey":"Eby8vdM02...==","bucketName":"test-container"}
func TestABSSnapstore_Integration(t *testing.T) {
	if os.Getenv("ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS_JSON") == "" &&
		os.Getenv("ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("skipping ABS integration test: ETCD_STEWARD_AZURE_APPLICATION_CREDENTIALS_JSON not set")
	}

	store, err := NewABS(SnapstoreConfig{
		Provider: "ABS",
		Prefix:   "integration-test",
		TempDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewABS error: %v", err)
	}

	roundTripTest(t, store)
}
