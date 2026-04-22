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
//  2. If corrupt: remove member from cluster, delete data directory.
//  3. If clean and non-empty: start with last known state.
//  4. If empty: attempt restore from snapshot (if restorer != nil), or start
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

		if err := b.cluster.RemoveMember(ctx, b.podName); err != nil {
			b.logger.Error("failed to remove member from cluster", zap.Error(err))
			// Continue with data dir cleanup regardless.
		}

		if err := b.deleteDataDir(); err != nil {
			return fmt.Errorf("failed to delete corrupt data directory: %w", err)
		}

		b.logger.Info("corrupt data directory cleaned up")
		// Fall through to empty data dir path.
	}

	// Step 2: Non-empty, non-corrupt data directory — start with existing state.
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

	// Step 3: Empty data directory — try restore or start new.
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
