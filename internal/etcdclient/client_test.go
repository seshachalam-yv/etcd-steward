// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdclient

import (
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestClient_ImplementsKV verifies at compile time that *Client satisfies the KV interface.
func TestClient_ImplementsKV(t *testing.T) {
	var _ KV = (*Client)(nil)
}

// TestClient_ImplementsMaintenance verifies at compile time that *Client satisfies the Maintenance interface.
func TestClient_ImplementsMaintenance(t *testing.T) {
	var _ Maintenance = (*Client)(nil)
}

// TestClient_ImplementsCluster verifies at compile time that *Client satisfies the Cluster interface.
func TestClient_ImplementsCluster(t *testing.T) {
	var _ Cluster = (*Client)(nil)
}

// TestLock_NewLock verifies that NewLock correctly initialises a Lock struct.
func TestLock_NewLock(t *testing.T) {
	// Use a nil *clientv3.Client; we only test field assignment, not connectivity.
	var raw *clientv3.Client
	prefix := "/etcd-steward/lock"
	ttl := 15

	l := NewLock(raw, prefix, ttl)

	if l == nil {
		t.Fatal("expected non-nil Lock")
	}
	if l.client != raw {
		t.Fatal("client field not set correctly")
	}
	if l.prefix != prefix {
		t.Fatalf("expected prefix %q, got %q", prefix, l.prefix)
	}
	if l.ttl != ttl {
		t.Fatalf("expected ttl %d, got %d", ttl, l.ttl)
	}
	if l.session != nil {
		t.Fatal("expected nil session on new lock")
	}
	if l.mutex != nil {
		t.Fatal("expected nil mutex on new lock")
	}
}
