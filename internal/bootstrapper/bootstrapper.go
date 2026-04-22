// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package bootstrapper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// Bootstrapper manages the initialization sequence for an etcd member. It
// inspects the data directory, optionally restores from a snapshot, and
// delegates the actual etcd process start to the WrapperClient.
type Bootstrapper struct {
	podName      string
	namespace    string
	dataDir      string
	isSingleNode bool
	isLearner    bool
	memberID     uint64
	peerURLs     []string
	restorer     RestoreProvider
	stateMachine StateMachineProvider
	recorder     Recorder
	wrapper      WrapperClient
	cluster      ClusterOperator
	logger       *zap.Logger
}

// New creates a Bootstrapper with the given configuration and dependencies.
func New(
	podName string,
	namespace string,
	dataDir string,
	isSingleNode bool,
	isLearner bool,
	memberID uint64,
	peerURLs []string,
	restorer RestoreProvider,
	stateMachine StateMachineProvider,
	recorder Recorder,
	wrapper WrapperClient,
	cluster ClusterOperator,
	logger *zap.Logger,
) *Bootstrapper {
	return &Bootstrapper{
		podName:      podName,
		namespace:    namespace,
		dataDir:      dataDir,
		isSingleNode: isSingleNode,
		isLearner:    isLearner,
		memberID:     memberID,
		peerURLs:     peerURLs,
		restorer:     restorer,
		stateMachine: stateMachine,
		recorder:     recorder,
		wrapper:      wrapper,
		cluster:      cluster,
		logger:       logger,
	}
}

// Initialize performs the bootstrap sequence:
//  1. Validate data directory state.
//  2. If corrupt: for multi-node clusters remove self from cluster by ID, then
//     delete data directory.
//  3. If learner joining existing cluster: add as learner, start etcd, then
//     record PendingLearner -> Learner transition.
//  4. If clean and non-empty: start with last known state.
//  5. If empty: attempt restore from snapshot (if restorer != nil), or start
//     as a brand new member.
func (b *Bootstrapper) Initialize(ctx context.Context) error {
	b.logger.Info("starting bootstrap",
		zap.String("pod", b.podName),
		zap.String("dataDir", b.dataDir),
		zap.Bool("singleNode", b.isSingleNode),
		zap.Bool("learner", b.isLearner),
	)

	if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "Initializing"); err != nil {
		b.logger.Error("failed to record bootstrap state", zap.Error(err))
	}

	// Step 1: Check for corruption.
	if b.IsDataDirCorrupt() {
		b.logger.Warn("data directory is corrupt, performing recovery")

		// For multi-node clusters, remove self from cluster membership before
		// cleaning up the data directory so that the remaining members stop
		// trying to replicate to this node.
		if !b.isSingleNode && b.memberID != 0 {
			if err := b.cluster.MemberRemoveByID(ctx, b.memberID); err != nil {
				b.logger.Error("failed to remove member by ID from cluster",
					zap.Uint64("memberID", b.memberID), zap.Error(err))
				// Continue with data dir cleanup regardless.
			}
		} else {
			if err := b.cluster.RemoveMember(ctx, b.podName); err != nil {
				b.logger.Error("failed to remove member from cluster", zap.Error(err))
				// Continue with data dir cleanup regardless.
			}
		}

		if err := b.deleteDataDir(); err != nil {
			return fmt.Errorf("failed to delete corrupt data directory: %w", err)
		}

		b.logger.Info("corrupt data directory cleaned up")
		// Fall through to empty data dir path.
	}

	// Step 2: Learner join flow — isLearner is true, data dir is empty,
	// and we have peer URLs to announce.
	if b.isLearner && b.IsDataDirEmpty() && len(b.peerURLs) > 0 {
		return b.joinAsLearner(ctx)
	}

	// Step 3: Non-empty, non-corrupt data directory — start with existing state.
	if !b.IsDataDirEmpty() {
		b.logger.Info("data directory exists and is not empty, starting with last known state")
		if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "StartingExisting"); err != nil {
			b.logger.Error("failed to record bootstrap state", zap.Error(err))
		}
		if err := b.stateMachine.TriggerStartAsFollower(); err != nil {
			b.logger.Error("failed to trigger state transition for existing data", zap.Error(err))
		}
		return b.startEtcd(ctx)
	}

	// Step 4: Empty data directory — try restore or start new.
	b.logger.Info("data directory is empty")

	// CRITICAL L1: Only attempt restore if restorer is provided.
	if b.restorer != nil {
		snapName, err := b.restorer.FindLatestFullSnapshot(ctx)
		if err != nil {
			b.logger.Error("failed to find latest full snapshot", zap.Error(err))
			// Fall through to start as new.
		} else if snapName != "" {
			b.logger.Info("restoring from snapshot", zap.String("snapshot", snapName))
			if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "Restoring"); err != nil {
				b.logger.Error("failed to record bootstrap state", zap.Error(err))
			}

			if err := b.restorer.Restore(ctx, snapName); err != nil {
				return fmt.Errorf("failed to restore from snapshot %q: %w", snapName, err)
			}

			b.logger.Info("restore completed successfully")
			if err := b.stateMachine.TriggerStartAsFollower(); err != nil {
				b.logger.Error("failed to trigger state transition after restore", zap.Error(err))
			}
			return b.startEtcd(ctx)
		}
	}

	// No restorer or no snapshot available — start as new member.
	b.logger.Info("starting as new member")
	if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "StartingNew"); err != nil {
		b.logger.Error("failed to record bootstrap state", zap.Error(err))
	}
	if err := b.stateMachine.TriggerStartAsNew(); err != nil {
		b.logger.Error("failed to trigger state transition for new member", zap.Error(err))
	}
	return b.startEtcd(ctx)
}

// joinAsLearner adds this member to the cluster as a non-voting learner,
// starts etcd (which uses initial-cluster-state=existing), and records the
// state transitions: Unknown -> PendingLearner -> Learner.
func (b *Bootstrapper) joinAsLearner(ctx context.Context) error {
	b.logger.Info("joining cluster as learner", zap.Strings("peerURLs", b.peerURLs))

	if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "JoiningAsLearner"); err != nil {
		b.logger.Error("failed to record bootstrap state", zap.Error(err))
	}

	// Add self to the existing cluster as a learner.
	memberID, err := b.cluster.MemberAddAsLearner(ctx, b.peerURLs)
	if err != nil {
		return fmt.Errorf("failed to add self as learner to cluster: %w", err)
	}
	b.memberID = memberID
	b.logger.Info("added as learner to cluster", zap.Uint64("memberID", memberID))

	// Record synchronous transition: Unknown -> PendingLearner.
	if err := b.stateMachine.TriggerStartAsPendingLearner(); err != nil {
		b.logger.Error("failed to trigger PendingLearner transition", zap.Error(err))
	}

	// Start etcd — the wrapper configures initial-cluster-state=existing.
	if err := b.startEtcd(ctx); err != nil {
		return err
	}

	// etcd is up and syncing — transition: PendingLearner -> Learner.
	if err := b.stateMachine.TriggerLearnerJoined(); err != nil {
		b.logger.Error("failed to trigger Learner transition", zap.Error(err))
	}

	if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "Learner"); err != nil {
		b.logger.Error("failed to record learner state", zap.Error(err))
	}

	b.logger.Info("learner join completed", zap.Uint64("memberID", memberID))
	return nil
}

// PromoteLearner promotes the learner member with the given ID to a full
// voting member and records the Learner -> Follower transition.
func (b *Bootstrapper) PromoteLearner(ctx context.Context, memberID uint64) error {
	b.logger.Info("promoting learner to voter", zap.Uint64("memberID", memberID))

	if err := b.cluster.MemberPromote(ctx, memberID); err != nil {
		return fmt.Errorf("failed to promote learner member %d: %w", memberID, err)
	}

	if err := b.stateMachine.TriggerPromoted(); err != nil {
		b.logger.Error("failed to trigger Promoted transition", zap.Error(err))
	}

	if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "Follower"); err != nil {
		b.logger.Error("failed to record follower state", zap.Error(err))
	}

	b.logger.Info("learner promoted to voter", zap.Uint64("memberID", memberID))
	return nil
}

// IsDataDirEmpty returns true if the data directory does not exist or contains
// no entries.
func (b *Bootstrapper) IsDataDirEmpty() bool {
	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		// If the directory doesn't exist, treat as empty.
		return true
	}
	return len(entries) == 0
}

// IsDataDirCorrupt returns true if the data directory contains a marker file
// indicating corruption. The marker file is "CORRUPT" placed in the data dir.
func (b *Bootstrapper) IsDataDirCorrupt() bool {
	markerPath := filepath.Join(b.dataDir, "CORRUPT")
	_, err := os.Stat(markerPath)
	return err == nil
}

// deleteDataDir removes the data directory and all its contents.
func (b *Bootstrapper) deleteDataDir() error {
	if err := os.RemoveAll(b.dataDir); err != nil {
		return fmt.Errorf("failed to remove data directory %q: %w", b.dataDir, err)
	}
	// Re-create the empty directory.
	if err := os.MkdirAll(b.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to re-create data directory %q: %w", b.dataDir, err)
	}
	return nil
}

// startEtcd delegates to the wrapper client to start the embedded etcd process
// and waits for it to become ready.
func (b *Bootstrapper) startEtcd(ctx context.Context) error {
	if err := b.wrapper.StartEmbeddedEtcd(ctx); err != nil {
		return fmt.Errorf("failed to start embedded etcd: %w", err)
	}
	if err := b.wrapper.CheckReady(ctx); err != nil {
		return fmt.Errorf("etcd did not become ready: %w", err)
	}
	if err := b.recorder.RecordBootstrapState(ctx, b.podName, b.namespace, "Running"); err != nil {
		b.logger.Error("failed to record running state", zap.Error(err))
	}
	b.logger.Info("etcd started successfully")
	return nil
}
