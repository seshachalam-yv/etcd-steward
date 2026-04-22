// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
)

// Compile-time assertion that LocalSnapStore implements SnapStore.
var _ SnapStore = (*LocalSnapStore)(nil)

// LocalSnapStore stores snapshots as files on the local filesystem.
type LocalSnapStore struct {
	rootDir string
}

// NewLocal creates a LocalSnapStore rooted at the given directory.
// The directory is created if it does not exist.
func NewLocal(rootDir string) (*LocalSnapStore, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to create snapstore directory %q", rootDir), err)
	}
	return &LocalSnapStore{rootDir: rootDir}, nil
}

// snapFileName builds a deterministic filename for a snapshot.
// Format: {Kind}-{StartRevision}-{EndRevision}-{UnixTimestamp}
func snapFileName(info SnapInfo) string {
	return fmt.Sprintf("%s-%d-%d-%d", info.Kind, info.StartRevision, info.EndRevision, info.CreatedAt.Unix())
}

// parseSnapFileName parses a snapshot filename back into a SnapInfo.
// The Size field is not set by this function.
func parseSnapFileName(name string) (SnapInfo, error) {
	parts := strings.SplitN(name, "-", 4)
	if len(parts) != 4 {
		return SnapInfo{}, errors.New(errors.ErrCodeValidation, fmt.Sprintf("invalid snapshot filename %q", name))
	}

	kind := SnapKind(parts[0])
	if kind != SnapKindFull && kind != SnapKindDelta {
		return SnapInfo{}, errors.New(errors.ErrCodeValidation, fmt.Sprintf("unknown snap kind %q in filename %q", parts[0], name))
	}

	startRev, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeValidation, fmt.Sprintf("invalid start revision in filename %q", name), err)
	}

	endRev, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeValidation, fmt.Sprintf("invalid end revision in filename %q", name), err)
	}

	unixTS, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeValidation, fmt.Sprintf("invalid timestamp in filename %q", name), err)
	}

	return SnapInfo{
		Kind:          kind,
		StartRevision: startRev,
		EndRevision:   endRev,
		CreatedAt:     time.Unix(unixTS, 0).UTC(),
		Name:          name,
	}, nil
}

// Upload writes snapshot data to a file and returns the resulting metadata.
func (s *LocalSnapStore) Upload(_ context.Context, info SnapInfo, data io.Reader) (SnapInfo, error) {
	name := snapFileName(info)
	path := filepath.Join(s.rootDir, name)

	f, err := os.Create(path)
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to create snapshot file %q", path), err)
	}
	defer func() { _ = f.Close() }()

	n, err := io.Copy(f, data)
	if err != nil {
		return SnapInfo{}, errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to write snapshot file %q", path), err)
	}

	result := info
	result.Name = name
	result.Size = n
	return result, nil
}

// Download returns a reader for the snapshot identified by name.
func (s *LocalSnapStore) Download(_ context.Context, name string) (io.ReadCloser, error) {
	path := filepath.Join(s.rootDir, name)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Wrap(errors.ErrCodeNotFound, fmt.Sprintf("snapshot %q not found", name), err)
		}
		return nil, errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to open snapshot %q", name), err)
	}
	return f, nil
}

// GetInfo returns metadata for the snapshot identified by name.
func (s *LocalSnapStore) GetInfo(_ context.Context, name string) (SnapInfo, error) {
	path := filepath.Join(s.rootDir, name)
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return SnapInfo{}, errors.Wrap(errors.ErrCodeNotFound, fmt.Sprintf("snapshot %q not found", name), err)
		}
		return SnapInfo{}, errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to stat snapshot %q", name), err)
	}

	info, err := parseSnapFileName(name)
	if err != nil {
		return SnapInfo{}, err
	}
	info.Size = fi.Size()
	info.Prefix = s.rootDir
	return info, nil
}

// List returns all snapshots sorted by CreatedAt ascending.
func (s *LocalSnapStore) List(_ context.Context) ([]SnapInfo, error) {
	entries, err := os.ReadDir(s.rootDir)
	if err != nil {
		return nil, errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to list snapstore directory %q", s.rootDir), err)
	}

	var snaps []SnapInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := parseSnapFileName(entry.Name())
		if err != nil {
			// Skip files that don't match the expected naming convention.
			continue
		}
		fi, err := entry.Info()
		if err != nil {
			continue
		}
		info.Size = fi.Size()
		info.Prefix = s.rootDir
		snaps = append(snaps, info)
	}

	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].CreatedAt.Before(snaps[j].CreatedAt)
	})

	return snaps, nil
}

// Delete removes the snapshot identified by name.
func (s *LocalSnapStore) Delete(_ context.Context, name string) error {
	path := filepath.Join(s.rootDir, name)
	err := os.Remove(path)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrap(errors.ErrCodeNotFound, fmt.Sprintf("snapshot %q not found", name), err)
		}
		return errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to delete snapshot %q", name), err)
	}
	return nil
}
