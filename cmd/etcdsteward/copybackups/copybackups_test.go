// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package copybackups

import (
	"testing"
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
