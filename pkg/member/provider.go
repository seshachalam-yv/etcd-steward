// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InfoProvider supplies a snapshot of operational status to StatusReconciler.
// Each component that needs to update EtcdMember.status implements this interface.
// ProvideInfo must be fast and non-blocking; it is called on every reconcile tick.
type InfoProvider interface {
	ProvideInfo() StatusInfo
}

// StatusInfo is the aggregated status contribution from one InfoProvider.
// Only non-nil fields are included in the EtcdMember status patch.
type StatusInfo struct {
	// DBSize is the total storage space used by the etcd DB.
	DBSize *resource.Quantity
	// DBSizeInUse is the logical storage actively in use (excludes free pages).
	DBSizeInUse *resource.Quantity
	// PeerTLSEnabled indicates whether peer TLS is enabled for this member.
	PeerTLSEnabled *bool
	// Snapshots carries the latest snapshot metadata.
	Snapshots *SnapshotInfo
	// LastDefragmentation carries the outcome of the most recent defragmentation.
	LastDefragmentation *DefragInfo
}

// SnapshotInfo mirrors EtcdMemberSnapshots from the etcd-druid API.
type SnapshotInfo struct {
	// LastFull is metadata for the most recent full snapshot.
	LastFull *SnapshotEntry
	// LastDelta is metadata for the most recent delta snapshot.
	LastDelta *SnapshotEntry
	// AccumulatedDeltaSize is the total size of all delta snapshots since the last full snapshot.
	AccumulatedDeltaSize *resource.Quantity
}

// SnapshotEntry mirrors EtcdMemberSnapshotInfo from the etcd-druid API.
type SnapshotEntry struct {
	// Name is the filename of the snapshot as stored in the snapstore.
	Name string
	// Timestamp is when the snapshot was taken.
	Timestamp metav1.Time
	// StartRevision is the first etcd revision captured in this snapshot.
	StartRevision int64
	// EndRevision is the last etcd revision captured in this snapshot.
	EndRevision int64
	// Size is the uncompressed snapshot size.
	Size *resource.Quantity
}

// DefragInfo mirrors EtcdMemberDefragmentation from the etcd-druid API.
type DefragInfo struct {
	// StartTime is when defragmentation started.
	StartTime metav1.Time
	// EndTime is when defragmentation completed (nil if still in progress).
	EndTime *metav1.Time
	// InitialDBSize is the DB size before defragmentation.
	InitialDBSize *resource.Quantity
	// FinalDBSize is the DB size after defragmentation.
	FinalDBSize *resource.Quantity
	// Reason describes why defragmentation was triggered.
	Reason *string
	// Message is an optional human-readable result message.
	Message *string
}

// InfoProviderFunc is a function adapter that implements InfoProvider.
// It is useful in tests to create a lightweight provider without defining a struct.
type InfoProviderFunc func() StatusInfo

// ProvideInfo calls the underlying function.
func (f InfoProviderFunc) ProvideInfo() StatusInfo { return f() }

// mergeStatusInfo merges src into dst. Non-nil fields in src overwrite the
// corresponding fields in dst.
func mergeStatusInfo(dst, src StatusInfo) StatusInfo {
	if src.DBSize != nil {
		dst.DBSize = src.DBSize
	}
	if src.DBSizeInUse != nil {
		dst.DBSizeInUse = src.DBSizeInUse
	}
	if src.PeerTLSEnabled != nil {
		dst.PeerTLSEnabled = src.PeerTLSEnabled
	}
	if src.Snapshots != nil {
		dst.Snapshots = src.Snapshots
	}
	if src.LastDefragmentation != nil {
		dst.LastDefragmentation = src.LastDefragmentation
	}
	return dst
}
