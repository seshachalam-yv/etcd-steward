// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package restoration provides native snapshot restoration for etcd without any
// dependency on etcd-backup-restore.
package restoration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.etcd.io/bbolt"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	snapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/snapshotter"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// ErrNoSnapshotFound is returned when no full snapshot exists in the store.
var ErrNoSnapshotFound = errors.New("no full snapshot found in snapstore")

// skipHashCheck controls whether etcdutl hash verification is skipped.
// Set to true only in tests — real snapshots always have valid hashes.
var skipHashCheck = false

// SetSkipHashCheckForTests disables etcdutl snapshot hash verification.
// Call this only from test code (TestMain or test helper functions).
func SetSkipHashCheckForTests(skip bool) {
	skipHashCheck = skip
}

// etcd bbolt bucket names — must match go.etcd.io/etcd/server/v3/mvcc/kvstore.go.
var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	// metaFinishedCompactKey tracks the last compacted revision — must match etcd's kvstore.go.
	metaFinishedCompactKey = []byte("finishedCompactRev")
)

// Restore restores the etcd data directory from snapshots in the given store.
//
// Steps:
//  1. List snapshots and find the latest Full snapshot
//  2. Fetch and decompress the full snapshot to a temp file
//  3. Use etcdutl snapshot.Restore to create a proper data directory with WAL+snap
//  4. Find all Incremental snapshots after the full snapshot
//  5. Replay delta events onto the bbolt DB
func Restore(
	_ context.Context,
	store snapstore.Snapstore,
	compressor compression.Compressor,
	dataDir string,
	tempDir string,
	memberName string,
	peerURL string,
	initialCluster string,
	logger *zap.Logger,
) error {
	snaps, err := store.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	// Find the latest Full snapshot (list is sorted by LastRevision ascending).
	var latestFull *snapstore.Snapshot
	for i := len(snaps) - 1; i >= 0; i-- {
		if snaps[i].Kind == "Full" {
			s := snaps[i]
			latestFull = &s
			break
		}
	}

	if latestFull == nil {
		return ErrNoSnapshotFound
	}

	logger.Info("restoring from full snapshot",
		zap.String("snapshot", latestFull.Path()),
		zap.Int64("revision", latestFull.LastRevision),
	)

	// Ensure temp directory exists.
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir %s: %w", tempDir, err)
	}

	// Fetch the full snapshot.
	reader, err := store.Fetch(*latestFull)
	if err != nil {
		return fmt.Errorf("failed to fetch full snapshot %s: %w", latestFull.Path(), err)
	}
	defer reader.Close() //nolint:errcheck

	// Decompress.
	decompressed, err := compressor.Decompress(reader)
	if err != nil {
		return fmt.Errorf("failed to decompress full snapshot: %w", err)
	}
	defer decompressed.Close() //nolint:errcheck

	// Write decompressed snapshot data to a temp file.
	tmpSnapshotPath := filepath.Join(tempDir, "snapshot.db")
	tmpFile, err := os.OpenFile(tmpSnapshotPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp snapshot file %s: %w", tmpSnapshotPath, err)
	}

	if _, err := io.Copy(tmpFile, decompressed); err != nil {
		tmpFile.Close() //nolint:errcheck
		return fmt.Errorf("failed to write snapshot to temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp snapshot file: %w", err)
	}

	// Remove existing dataDir contents so etcdutl Restore can create a fresh directory.
	// (tryRestore in initializer.go already calls os.RemoveAll(dataDir) before calling us,
	// but we call it here too to ensure idempotency.)
	if err := os.RemoveAll(dataDir); err != nil {
		return fmt.Errorf("failed to clear data dir before restore: %w", err)
	}

	// Derive peer URLs and initial cluster from the inputs.
	// peerURL may be empty for single-node clusters that haven't started yet.
	if peerURL == "" {
		peerURL = "http://127.0.0.1:2380"
	}
	if initialCluster == "" {
		initialCluster = memberName + "=" + peerURL
	}
	// Derive cluster token from initial cluster string (use first peer URL host).
	clusterToken := deriveClusterToken(initialCluster)

	// Use etcdutl snapshot Restore to create a proper data directory with WAL+snap files.
	// This is the only correct way to restore etcd — just copying the DB file is insufficient
	// because etcd needs a WAL and snap file to bootstrap raft state.
	mgr := snapshot.NewV3(logger)
	restoreOut := filepath.Join(tempDir, "restore-out")
	if err := mgr.Restore(snapshot.RestoreConfig{
		SnapshotPath:        tmpSnapshotPath,
		Name:                memberName,
		OutputDataDir:       restoreOut,
		PeerURLs:            []string{peerURL},
		InitialCluster:      initialCluster,
		InitialClusterToken: clusterToken,
		SkipHashCheck:       skipHashCheck,
	}); err != nil {
		return fmt.Errorf("failed to restore snapshot using etcdutl: %w", err)
	}

	// Find incremental snapshots that cover revisions after the full snapshot.
	// Include a delta if its LastRevision > latestFull.LastRevision, regardless of
	// StartRevision — some deltas start before the full snapshot but extend beyond it.
	var deltas []snapstore.Snapshot
	for _, s := range snaps {
		if s.Kind == "Incremental" && s.LastRevision > latestFull.LastRevision {
			deltas = append(deltas, s)
		}
	}

	dbPath := filepath.Join(restoreOut, "member", "snap", "db")
	if len(deltas) > 0 {
		logger.Info("applying delta snapshots",
			zap.Int("deltaCount", len(deltas)),
			zap.Int64("fromRevision", latestFull.LastRevision),
		)
		if err := applyDeltas(dbPath, store, compressor, deltas, logger); err != nil {
			return fmt.Errorf("failed to apply delta snapshots: %w", err)
		}
	}

	// Move the restored data directory to the target location.
	// The restoreOut directory is dataDir in etcd's view, so we rename it to dataDir.
	if err := os.Rename(restoreOut, dataDir); err != nil {
		// Rename may fail across filesystems — fall back to copy.
		if copyErr := copyDir(restoreOut, dataDir); copyErr != nil {
			return fmt.Errorf("failed to move restored data dir to %s: rename=%w, copy=%v", dataDir, err, copyErr)
		}
		os.RemoveAll(restoreOut) //nolint:errcheck
	}

	logger.Info("restoration completed",
		zap.String("dataDir", dataDir),
		zap.Int64("revision", latestFull.LastRevision),
	)

	return nil
}

// deriveClusterToken creates a deterministic cluster token from the initial cluster string.
func deriveClusterToken(initialCluster string) string {
	// Use the first member's name as the token base.
	// Format: "name=peerURL,name2=peerURL2"
	if idx := strings.IndexAny(initialCluster, "=,"); idx > 0 {
		return "etcd-cluster-" + initialCluster[:idx]
	}
	return "etcd-cluster"
}

// applyDeltas replays all delta events from the given incremental snapshots onto
// the bbolt DB at dbPath. Events are applied in snapshot order (ascending revision).
func applyDeltas(
	dbPath string,
	store snapstore.Snapstore,
	compressor compression.Compressor,
	deltas []snapstore.Snapshot,
	logger *zap.Logger,
) error {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 0})
	if err != nil {
		return fmt.Errorf("failed to open bbolt DB: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var lastAppliedRev int64
	for _, delta := range deltas {
		events, err := readDeltaEvents(store, compressor, delta)
		if err != nil {
			return fmt.Errorf("failed to read delta %s: %w", delta.Path(), err)
		}
		applied, err := writeDeltaEventsToDB(db, events)
		if err != nil {
			return fmt.Errorf("failed to write delta %s to DB: %w", delta.Path(), err)
		}
		if applied > lastAppliedRev {
			lastAppliedRev = applied
		}
		logger.Info("applied delta snapshot",
			zap.String("snapshot", delta.Path()),
			zap.Int("events", len(events)),
			zap.Int64("lastRevision", applied),
		)
	}

	return nil
}

// readDeltaEvents fetches and decodes all DeltaEvents from one incremental snapshot.
func readDeltaEvents(
	store snapstore.Snapstore,
	compressor compression.Compressor,
	snap snapstore.Snapshot,
) ([]snapshotter.DeltaEvent, error) {
	r, err := store.Fetch(snap)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer r.Close() //nolint:errcheck

	decompressed, err := compressor.Decompress(r)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}
	defer decompressed.Close() //nolint:errcheck

	var events []snapshotter.DeltaEvent
	scanner := bufio.NewScanner(decompressed)
	// Increase buffer size for large values.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		ev, err := snapshotter.UnmarshalDeltaEvent(line)
		if err != nil {
			return nil, fmt.Errorf("unmarshal event: %w", err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return events, nil
}

// writeDeltaEventsToDB writes PUT and DELETE events directly into the etcd bbolt MVCC keyspace.
// Returns the highest revision written, or 0 if no events.
//
// etcd's bbolt MVCC layout (stable across 3.5.x):
//   - Bucket "key": revision_key(mainRev, subRev) → protobuf(mvccpb.KeyValue)
//     revision_key = big-endian uint64(mainRev) + '_' + big-endian uint64(subRev) (17 bytes)
//   - Bucket "meta": "finishedCompact" → big-endian int64 (last compacted revision)
//
// The in-memory B-tree index is rebuilt from the "key" bucket on etcd startup,
// so we only need to write to "key" (and optionally update "meta").
func writeDeltaEventsToDB(db *bbolt.DB, events []snapshotter.DeltaEvent) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	var lastRev int64

	return lastRev, db.Update(func(tx *bbolt.Tx) error {
		keyBucket := tx.Bucket(keyBucketName)
		if keyBucket == nil {
			return fmt.Errorf("bbolt bucket %q not found — snapshot may be corrupted or format changed", keyBucketName)
		}

		for _, ev := range events {
			kv := &mvccpb.KeyValue{
				Key:         ev.Key,
				Value:       ev.Value,
				ModRevision: ev.ModRevision,
				Version:     ev.Version,
			}
			if ev.Type == snapshotter.EventTypeDelete {
				// Deletions set Value=nil, Version=0 in etcd MVCC.
				kv.Value = nil
				kv.Version = 0
			}

			// Encode revision as big-endian uint64 pair (mainRev, subRev=0).
			revKey := encodeRevision(ev.ModRevision, 0)

			kvBytes, err := kv.Marshal()
			if err != nil {
				return fmt.Errorf("marshal KeyValue for key %q rev %d: %w", ev.Key, ev.ModRevision, err)
			}

			if err := keyBucket.Put(revKey, kvBytes); err != nil {
				return fmt.Errorf("bbolt put key %q rev %d: %w", ev.Key, ev.ModRevision, err)
			}

			if ev.ModRevision > lastRev {
				lastRev = ev.ModRevision
			}
		}

		// Update finishedCompact in meta bucket to reflect applied revisions.
		// This prevents etcd from trying to compact revisions we've already applied.
		if metaBucket := tx.Bucket(metaBucketName); metaBucket != nil {
			_ = metaBucket.Put(metaFinishedCompactKey, encodeRevision(lastRev, 0))
		}

		return nil
	})
}

// encodeRevision encodes a main/sub revision pair as a 17-byte key matching
// etcd's internal MVCC revision encoding: {8-byte big-endian main}'_'{8-byte big-endian sub}.
func encodeRevision(main, sub int64) []byte {
	b := make([]byte, 17)
	binary.BigEndian.PutUint64(b[0:8], uint64(main))
	b[8] = '_'
	binary.BigEndian.PutUint64(b[9:17], uint64(sub))
	return b
}

// copyDir recursively copies src directory to dst.
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)
		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}
		return copyFile(path, dstPath)
	})
}

// copyFile copies src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close() //nolint:errcheck

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close() //nolint:errcheck
		return err
	}
	return out.Close()
}
