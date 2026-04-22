// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compact

import (
	"testing"
)

func TestCompact_Name(t *testing.T) {
	cmd := NewCommand()
	if cmd.Use != "compact" {
		t.Errorf("expected command name %q, got %q", "compact", cmd.Use)
	}
}

func TestCompact_HasFlags(t *testing.T) {
	cmd := NewCommand()

	expectedFlags := []string{"data-dir", "store-provider", "store-prefix", "store-container", "compression-algo"}
	for _, name := range expectedFlags {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("expected flag %q to exist", name)
		}
	}
}

func TestCompact_DataDirDefault(t *testing.T) {
	cmd := NewCommand()
	f := cmd.Flags().Lookup("data-dir")
	if f == nil {
		t.Fatal("expected data-dir flag to exist")
	}
	if f.DefValue != "/var/etcd/data/new.etcd" {
		t.Errorf("expected data-dir default %q, got %q", "/var/etcd/data/new.etcd", f.DefValue)
	}
}

func TestCompactCommand_AllFlags(t *testing.T) {
	cmd := NewCommand()

	flags := map[string]string{
		"data-dir":         "/var/etcd/data/new.etcd",
		"store-provider":   "Local",
		"store-prefix":     "",
		"store-container":  "",
		"compression-algo": "none",
	}

	for name, expectedDefault := range flags {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("flag %q not registered", name)
			continue
		}
		if f.DefValue != expectedDefault {
			t.Errorf("flag %q: expected default %q, got %q", name, expectedDefault, f.DefValue)
		}
	}
}
