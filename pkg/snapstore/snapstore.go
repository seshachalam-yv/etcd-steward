// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Snapshot represents a snapshot file stored in a snapstore.
type Snapshot struct {
	Kind          string
	StartRevision int64
	LastRevision  int64
	CreatedOn     time.Time
	SnapDir       string
	SnapName      string
}

// Path returns the full relative path: SnapDir/SnapName.
func (s *Snapshot) Path() string {
	return s.SnapDir + "/" + s.SnapName
}

// Snapstore defines the interface for snapshot storage backends.
type Snapstore interface {
	// Save writes the snapshot data from r into the store.
	Save(snap Snapshot, r io.ReadCloser) error
	// Fetch returns a reader for the given snapshot.
	Fetch(snap Snapshot) (io.ReadCloser, error)
	// List returns all snapshots in the store, sorted by LastRevision ascending.
	List() ([]Snapshot, error)
	// Delete removes the given snapshot from the store.
	Delete(snap Snapshot) error
}

// SnapstoreConfig holds configuration for creating a Snapstore instance.
type SnapstoreConfig struct { //nolint:revive // name intentionally includes package prefix for clarity
	Provider         string
	Container        string
	Prefix           string
	TempDir          string
	EndpointOverride string
}

// NewSnapstore creates a Snapstore for the given configuration.
func NewSnapstore(cfg SnapstoreConfig) (Snapstore, error) {
	switch cfg.Provider {
	case "Local", "local", "":
		baseDir := cfg.Container
		if cfg.Prefix != "" {
			baseDir = filepath.Join(baseDir, cfg.Prefix)
		}
		return NewLocal(baseDir), nil
	case "S3", "s3":
		return NewS3(cfg)
	case "GCS", "gcs":
		return NewGCS(cfg)
	case "ABS", "abs":
		return NewABS(cfg)
	default:
		return nil, fmt.Errorf("unsupported snapstore provider %q", cfg.Provider)
	}
}

// FormatSnapshotName returns a filename for the given Snapshot following the
// naming convention: {Kind}-{StartRevision:016d}-{LastRevision:016d}-{UnixNano}
func FormatSnapshotName(snap Snapshot) string {
	return fmt.Sprintf("%s-%016d-%016d-%d",
		snap.Kind,
		snap.StartRevision,
		snap.LastRevision,
		snap.CreatedOn.UnixNano(),
	)
}

// ParseSnapshotName parses a snapshot filename into a Snapshot struct.
// The expected format is: {Kind}-{StartRevision:016d}-{LastRevision:016d}-{UnixNano}[.ext]
// Kind must be "Full" or "Incremental".
// An optional compression extension (.gz, .zst, .lz4) is stripped before parsing.
func ParseSnapshotName(name string) (Snapshot, error) {
	// Strip known compression extensions.
	for _, ext := range []string{".zst", ".gz", ".lz4", ".zlib", ".snappy"} {
		if strings.HasSuffix(name, ext) {
			name = name[:len(name)-len(ext)]
			break
		}
	}

	parts := strings.SplitN(name, "-", 4)
	if len(parts) != 4 {
		return Snapshot{}, fmt.Errorf("unexpected snapshot filename format: %q", name)
	}

	kind := parts[0]
	if kind != "Full" && kind != "Incremental" {
		return Snapshot{}, fmt.Errorf("unknown snapshot kind %q in filename %q", kind, name)
	}

	startRev, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to parse StartRevision in %q: %w", name, err)
	}

	lastRev, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to parse LastRevision in %q: %w", name, err)
	}

	unixNano, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to parse timestamp in %q: %w", name, err)
	}

	return Snapshot{
		Kind:          kind,
		StartRevision: startRev,
		LastRevision:  lastRev,
		CreatedOn:     time.Unix(0, unixNano).UTC(),
		SnapName:      name,
	}, nil
}
