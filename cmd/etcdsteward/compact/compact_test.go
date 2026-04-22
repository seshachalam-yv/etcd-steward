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

	expectedFlags := []string{"config", "data-dir", "etcd-endpoints", "store-prefix", "store-container"}
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
