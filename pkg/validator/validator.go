// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package validator checks the etcd data directory to determine bootstrap mode and validate the DB.
package validator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	bbolt "go.etcd.io/bbolt"
)

// ValidationMode is the type of DB validation to perform.
type ValidationMode string

const (
	// ValidationModeSanity performs a quick sanity check of the etcd DB.
	ValidationModeSanity ValidationMode = "sanity"
	// ValidationModeFull performs a full integrity check of the etcd DB.
	ValidationModeFull ValidationMode = "full"
)

// Result is the outcome of a DB validation.
type Result struct {
	// Valid is true if the DB passed validation.
	Valid bool
	// Err holds the error if validation failed.
	Err error
}

// DetermineMode reads the exit_code file and returns the appropriate validation mode.
// If the exit_code file contains "terminated" or "interrupt", returns ValidationModeSanity.
// If the file is missing or has other content, returns ValidationModeFull.
func DetermineMode(dataDir string) ValidationMode {
	exitCodeFile := filepath.Join(dataDir, "exit_code")
	data, err := os.ReadFile(exitCodeFile)
	if err != nil {
		return ValidationModeFull
	}
	content := strings.TrimSpace(string(data))
	if content == "terminated" || content == "interrupt" {
		return ValidationModeSanity
	}
	return ValidationModeFull
}

// WriteExitMarker writes the given reason string to the exit_code file in the data directory.
func WriteExitMarker(dataDir, reason string) error {
	exitCodeFile := filepath.Join(dataDir, "exit_code")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory %s: %w", dataDir, err)
	}
	if err := os.WriteFile(exitCodeFile, []byte(reason), 0600); err != nil {
		return fmt.Errorf("failed to write exit marker to %s: %w", exitCodeFile, err)
	}
	return nil
}

// HasSafeguardFile returns true if the safeguard file exists in the data directory.
func HasSafeguardFile(dataDir string) bool {
	safeguardPath := filepath.Join(dataDir, "safeguard.db")
	_, err := os.Stat(safeguardPath)
	return err == nil
}

// Validate performs the requested DB validation mode on the etcd DB at dataDir/member/snap/db.
// For Full mode it runs tx.Check(); for Sanity mode just opening is sufficient.
// Returns Result{Valid: true} if the DB passes or does not exist (fresh start).
func Validate(dataDir string, mode ValidationMode) Result {
	dbPath := filepath.Join(dataDir, "member", "snap", "db")

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return Result{Valid: true}
	}

	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
		ReadOnly: true,
		Timeout:  5 * time.Second,
	})
	if err != nil {
		return Result{Valid: false, Err: fmt.Errorf("failed to open DB: %w", err)}
	}
	defer db.Close() //nolint:errcheck

	if mode == ValidationModeFull {
		if err := db.View(func(tx *bbolt.Tx) error {
			for err := range tx.Check() {
				return err
			}
			return nil
		}); err != nil {
			return Result{Valid: false, Err: fmt.Errorf("DB integrity check failed: %w", err)}
		}
	}

	return Result{Valid: true}
}
