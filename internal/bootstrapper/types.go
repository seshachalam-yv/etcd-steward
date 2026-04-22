// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package bootstrapper

import (
	"context"
)

// RestoreProvider abstracts the snapshot restore capability. Implementations
// locate the latest full snapshot and restore the etcd data directory from it.
type RestoreProvider interface {
	// FindLatestFullSnapshot returns the name of the latest full snapshot, or
	// an empty string if no snapshot is available.
	FindLatestFullSnapshot(ctx context.Context) (string, error)
	// Restore restores the etcd data directory from the snapshot identified by name.
	Restore(ctx context.Context, snapshotName string) error
}

// WrapperClient abstracts communication with the etcd-wrapper sidecar that
// manages the embedded etcd process.
type WrapperClient interface {
	// StartEmbeddedEtcd asks the wrapper to start the embedded etcd process.
	StartEmbeddedEtcd(ctx context.Context) error
	// CheckReady returns nil when the embedded etcd process is ready to serve.
	CheckReady(ctx context.Context) error
}

// ClusterOperator abstracts etcd cluster membership operations needed during
// bootstrap (e.g., removing a corrupted member).
type ClusterOperator interface {
	// RemoveMember removes the member with the given name from the etcd cluster.
	RemoveMember(ctx context.Context, memberName string) error
}

// Recorder records state transitions during the bootstrap lifecycle.
type Recorder interface {
	// RecordBootstrapState records the given bootstrap state for the member.
	RecordBootstrapState(ctx context.Context, memberName, namespace, state string) error
}

// StateMachineProvider abstracts the state machine used to track member
// lifecycle transitions during bootstrap.
type StateMachineProvider interface {
	// TriggerStartAsNew triggers the transition for a brand new member.
	TriggerStartAsNew() error
	// TriggerStartAsFollower triggers the transition for a member restored from snapshot.
	TriggerStartAsFollower() error
}
