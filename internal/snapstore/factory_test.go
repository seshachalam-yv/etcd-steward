// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"testing"
)

func TestNewSnapStore_Local(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSnapStore(ProviderLocal, "", dir, nil)
	if err != nil {
		t.Fatalf("NewSnapStore(Local): %v", err)
	}
	if _, ok := store.(*LocalSnapStore); !ok {
		t.Fatalf("expected *LocalSnapStore, got %T", store)
	}
}

func TestNewSnapStore_S3(t *testing.T) {
	// NewS3 calls awsconfig.LoadDefaultConfig which works with default credentials.
	// In test environments without AWS credentials this still returns a valid client.
	store, err := NewSnapStore(ProviderS3, "backups", "my-bucket", map[string]string{
		"region": "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewSnapStore(S3): %v", err)
	}
	if _, ok := store.(*S3SnapStore); !ok {
		t.Fatalf("expected *S3SnapStore, got %T", store)
	}
}

func TestNewSnapStore_GCS(t *testing.T) {
	// GCS client creation may fail if no credentials/emulator is configured.
	// We accept both success (correct type) and failure (credential error).
	store, err := NewSnapStore(ProviderGCS, "backups", "my-bucket", nil)
	if err != nil {
		t.Skipf("skipping GCS test: %v (no credentials available)", err)
	}
	if _, ok := store.(*GCSSnapStore); !ok {
		t.Fatalf("expected *GCSSnapStore, got %T", store)
	}
}

func TestNewSnapStore_ABS(t *testing.T) {
	store, err := NewSnapStore(ProviderABS, "backups", "my-container", map[string]string{
		"storageAccount": "devstoreaccount1",
		// Azurite well-known base64 key for testing.
		"storageKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
	})
	if err != nil {
		t.Fatalf("NewSnapStore(ABS): %v", err)
	}
	if _, ok := store.(*ABSSnapStore); !ok {
		t.Fatalf("expected *ABSSnapStore, got %T", store)
	}
}

func TestNewSnapStore_Unknown(t *testing.T) {
	_, err := NewSnapStore("UnknownProvider", "", "", nil)
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}
