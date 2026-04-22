// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"io"
	"time"
)

// SnapKind identifies the type of snapshot.
type SnapKind string

const (
	// SnapKindFull represents a full (base) snapshot.
	SnapKindFull SnapKind = "Full"
	// SnapKindDelta represents a delta (incremental) snapshot.
	SnapKindDelta SnapKind = "Delta"
)

// SnapInfo holds metadata about a single snapshot file.
type SnapInfo struct {
	// Kind is the type of snapshot (Full or Delta).
	Kind SnapKind
	// StartRevision is the first etcd revision included in this snapshot.
	StartRevision int64
	// EndRevision is the last etcd revision included in this snapshot.
	EndRevision int64
	// CreatedAt is the time the snapshot was created.
	CreatedAt time.Time
	// Size is the size of the snapshot in bytes.
	Size int64
	// Name is the filename of the snapshot.
	Name string
	// Prefix is the directory prefix under which the snapshot is stored.
	Prefix string
	// IsCompressed indicates whether the snapshot data is compressed.
	IsCompressed bool
}

// SnapStore defines operations for storing and retrieving snapshots.
type SnapStore interface {
	// Upload stores snapshot data and returns the resulting metadata.
	Upload(ctx context.Context, info SnapInfo, data io.Reader) (SnapInfo, error)
	// Download returns a reader for the snapshot identified by name.
	Download(ctx context.Context, name string) (io.ReadCloser, error)
	// GetInfo returns metadata for the snapshot identified by name.
	GetInfo(ctx context.Context, name string) (SnapInfo, error)
	// List returns all snapshots sorted by CreatedAt ascending.
	List(ctx context.Context) ([]SnapInfo, error)
	// Delete removes the snapshot identified by name.
	Delete(ctx context.Context, name string) error
}
