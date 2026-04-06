// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"os"
	"path/filepath"
	"testing"

	bbolt "go.etcd.io/bbolt"
)

func createValidBboltDB(t *testing.T, dbPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		t.Fatalf("failed to create DB directory: %v", err)
	}
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to create bbolt DB: %v", err)
	}
	db.Close() //nolint:errcheck
}

func TestDetermineMode(t *testing.T) {
	tests := []struct {
		name     string
		content  *string // nil means do not write file
		expected ValidationMode
	}{
		{
			name:     "terminated returns sanity",
			content:  strPtr("terminated"),
			expected: ValidationModeSanity,
		},
		{
			name:     "interrupt returns sanity",
			content:  strPtr("interrupt"),
			expected: ValidationModeSanity,
		},
		{
			name:     "missing file returns full",
			content:  nil,
			expected: ValidationModeFull,
		},
		{
			name:     "other content returns full",
			content:  strPtr("crashed"),
			expected: ValidationModeFull,
		},
		{
			name:     "empty content returns full",
			content:  strPtr(""),
			expected: ValidationModeFull,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			if tc.content != nil {
				if err := os.WriteFile(filepath.Join(dataDir, "exit_code"), []byte(*tc.content), 0600); err != nil {
					t.Fatalf("failed to write exit_code: %v", err)
				}
			}
			got := DetermineMode(dataDir)
			if got != tc.expected {
				t.Errorf("DetermineMode() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestWriteExitMarker(t *testing.T) {
	dataDir := t.TempDir()
	if err := WriteExitMarker(dataDir, "terminated"); err != nil {
		t.Fatalf("WriteExitMarker() error: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dataDir, "exit_code"))
	if err != nil {
		t.Fatalf("failed to read exit_code: %v", err)
	}
	if string(data) != "terminated" {
		t.Errorf("exit_code content = %q, want %q", string(data), "terminated")
	}
}

func TestHasSafeguardFile(t *testing.T) {
	tests := []struct {
		name     string
		create   bool
		expected bool
	}{
		{
			name:     "missing returns false",
			create:   false,
			expected: false,
		},
		{
			name:     "exists returns true",
			create:   true,
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			if tc.create {
				safeguardPath := filepath.Join(dataDir, "safeguard.db")
				if err := os.WriteFile(safeguardPath, []byte{}, 0600); err != nil {
					t.Fatalf("failed to create safeguard file: %v", err)
				}
			}
			got := HasSafeguardFile(dataDir)
			if got != tc.expected {
				t.Errorf("HasSafeguardFile() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name      string
		mode      ValidationMode
		setupDB   func(t *testing.T, dataDir string)
		wantValid bool
		wantErr   bool
	}{
		{
			name:      "sanity mode missing DB is valid",
			mode:      ValidationModeSanity,
			setupDB:   func(t *testing.T, dataDir string) {},
			wantValid: true,
		},
		{
			name:      "full mode missing DB is valid",
			mode:      ValidationModeFull,
			setupDB:   func(t *testing.T, dataDir string) {},
			wantValid: true,
		},
		{
			name: "sanity mode valid DB",
			mode: ValidationModeSanity,
			setupDB: func(t *testing.T, dataDir string) {
				createValidBboltDB(t, filepath.Join(dataDir, "member", "snap", "db"))
			},
			wantValid: true,
		},
		{
			name: "full mode valid DB",
			mode: ValidationModeFull,
			setupDB: func(t *testing.T, dataDir string) {
				createValidBboltDB(t, filepath.Join(dataDir, "member", "snap", "db"))
			},
			wantValid: true,
		},
		{
			name: "corrupt DB fails validation",
			mode: ValidationModeSanity,
			setupDB: func(t *testing.T, dataDir string) {
				dbPath := filepath.Join(dataDir, "member", "snap", "db")
				if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
					t.Fatalf("failed to create DB directory: %v", err)
				}
				corruptData := make([]byte, 4096)
				for i := range corruptData {
					corruptData[i] = byte(i % 256)
				}
				if err := os.WriteFile(dbPath, corruptData, 0600); err != nil {
					t.Fatalf("failed to write corrupt DB: %v", err)
				}
			},
			wantValid: false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			tc.setupDB(t, dataDir)
			result := Validate(dataDir, tc.mode)
			if result.Valid != tc.wantValid {
				t.Errorf("Validate().Valid = %v, want %v", result.Valid, tc.wantValid)
			}
			if tc.wantErr && result.Err == nil {
				t.Error("expected non-nil Err, got nil")
			}
			if !tc.wantErr && result.Err != nil {
				t.Errorf("unexpected Err: %v", result.Err)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}
