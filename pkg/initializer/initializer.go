// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package initializer orchestrates the DEP-04 etcd member lifecycle initialization.
package initializer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/etcdclient"
	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/metrics"
	"github.com/gardener/etcd-steward/pkg/restoration"
	"github.com/gardener/etcd-steward/pkg/snapstore"
	"github.com/gardener/etcd-steward/pkg/statemachine"
	"github.com/gardener/etcd-steward/pkg/validator"
)

// InitializationStatus represents the current state of the initialization process.
type InitializationStatus string

const (
	// InitializationStatusNew indicates initialization has not started.
	InitializationStatusNew InitializationStatus = "New"
	// InitializationStatusInProgress indicates initialization is underway.
	InitializationStatusInProgress InitializationStatus = "InProgress"
	// InitializationStatusSuccessful indicates initialization completed successfully.
	InitializationStatusSuccessful InitializationStatus = "Successful"
)

// EtcdStatusAPI is the subset of the etcd client needed to query member and cluster ID.
type EtcdStatusAPI interface {
	Status(ctx context.Context, endpoint string) (*EtcdStatusResponse, error)
}

// EtcdStatusResponse holds the fields needed from an etcd status query.
type EtcdStatusResponse struct {
	MemberID  uint64
	ClusterID uint64
}

// Initializer orchestrates the full DEP-04 etcd member lifecycle initialization.
type Initializer struct {
	memberName                   string
	memberNamespace              string
	peerURL                      string
	dataDir                      string
	restorationTempDir           string
	initialCluster               string
	isSingleNode                 bool
	hasCreateAsLearnerAnnotation bool

	recorder      statemachine.Recorder
	memberClient  member.Client
	clusterClient etcdclient.ClusterClient
	etcdStatusAPI EtcdStatusAPI
	etcdEndpoint  string
	logger        *zap.Logger
	store         snapstore.Snapstore    // nil if no backup configured
	compressor    compression.Compressor // nil if no backup configured

	// tcpDialFn is called to check if the etcd endpoint is TCP-reachable.
	// Defaults to net.Dialer.DialContext; overridable in tests.
	tcpDialFn func(ctx context.Context, network, addr string) (net.Conn, error)

	status    atomic.Value // stores InitializationStatus
	startOnce sync.Once

	isLearnerMember    atomic.Bool
	inDataLossRecovery atomic.Bool
}

// New creates a new Initializer with the given configuration.
func New(
	memberName, memberNamespace, peerURL string,
	dataDir, restorationTempDir, initialCluster string,
	isSingleNode bool,
	hasCreateAsLearnerAnnotation bool,
	recorder statemachine.Recorder,
	memberClient member.Client,
	clusterClient etcdclient.ClusterClient,
	etcdStatusAPI EtcdStatusAPI,
	etcdEndpoint string,
	logger *zap.Logger,
	store snapstore.Snapstore,
	compressor compression.Compressor,
) *Initializer {
	i := &Initializer{
		memberName:                   memberName,
		memberNamespace:              memberNamespace,
		peerURL:                      peerURL,
		dataDir:                      dataDir,
		restorationTempDir:           restorationTempDir,
		initialCluster:               initialCluster,
		isSingleNode:                 isSingleNode,
		hasCreateAsLearnerAnnotation: hasCreateAsLearnerAnnotation,
		recorder:                     recorder,
		memberClient:                 memberClient,
		clusterClient:                clusterClient,
		etcdStatusAPI:                etcdStatusAPI,
		etcdEndpoint:                 etcdEndpoint,
		logger:                       logger,
		store:                        store,
		compressor:                   compressor,
		tcpDialFn:                    (&net.Dialer{}).DialContext,
	}
	i.status.Store(InitializationStatusNew)
	return i
}

// GetStatus returns the current initialization status.
func (i *Initializer) GetStatus() InitializationStatus {
	return i.status.Load().(InitializationStatus)
}

// IsLearner returns true while this member is still a non-voting learner.
func (i *Initializer) IsLearner() bool {
	return i.isLearnerMember.Load()
}

// NeedsExistingClusterState returns true if etcd must be started with
// initial-cluster-state=existing.
func (i *Initializer) NeedsExistingClusterState() bool {
	return i.hasCreateAsLearnerAnnotation || i.inDataLossRecovery.Load()
}

// GetMemberAndClusterID queries the local etcd to retrieve the member and cluster IDs.
// Returns empty strings if initialization has not completed or etcd is unreachable.
func (i *Initializer) GetMemberAndClusterID(ctx context.Context) (memberID, clusterID string) {
	if i.GetStatus() != InitializationStatusSuccessful {
		return "", ""
	}
	if i.etcdStatusAPI == nil {
		return "", ""
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := i.etcdStatusAPI.Status(queryCtx, i.etcdEndpoint)
	if err != nil {
		return "", ""
	}
	return fmt.Sprintf("%x", resp.MemberID), fmt.Sprintf("%x", resp.ClusterID)
}

// Start begins the initialization process for the given mode.
// mode is "sanity" or "full" as requested by etcd-wrapper.
// This is non-blocking -- it launches a goroutine and returns immediately.
// Subsequent calls are no-ops.
func (i *Initializer) Start(ctx context.Context, mode string) error {
	i.startOnce.Do(func() {
		i.status.Store(InitializationStatusInProgress)
		go i.run(ctx, mode)
	})
	return nil
}

// run executes the DEP-04 initialization flow in a goroutine.
func (i *Initializer) run(ctx context.Context, mode string) {
	start := time.Now()
	path := "unknown"

	// Record the initial New state per DEP-04. This is the very first transition for this member
	// and allows etcd-druid to distinguish a never-initialised member from one in progress.
	newReason := statemachine.ReasonNewSingleNodeClusterCreated
	if !i.isSingleNode {
		newReason = statemachine.ReasonClusterScaledUp
	}
	if err := i.record(ctx, statemachine.Transition{
		State:  statemachine.StateNew,
		Reason: newReason,
	}); err != nil {
		// Non-fatal: if EtcdMember doesn't exist yet (race with etcd-druid), log and continue.
		i.logger.Warn("failed to record initial New transition, continuing", zap.Error(err))
	}

	err := i.initialize(ctx, mode)
	duration := time.Since(start).Seconds()
	metrics.InitializationDurationSeconds.WithLabelValues(i.memberNamespace, i.memberName, path).Observe(duration)
	if err != nil {
		i.logger.Error("initialization failed", zap.Error(err))
		// Do NOT set Successful -- leave status as InProgress so etcd-wrapper retries.
		return
	}
	i.status.Store(InitializationStatusSuccessful)
}

// record writes a state transition and increments the StateTransitionsTotal counter.
func (i *Initializer) record(ctx context.Context, t statemachine.Transition) error {
	subStateStr := ""
	if t.SubState != nil {
		subStateStr = string(*t.SubState)
	}
	if err := i.recorder.Record(ctx, i.memberName, i.memberNamespace, t); err != nil {
		return err
	}
	metrics.StateTransitionsTotal.WithLabelValues(
		i.memberNamespace, i.memberName,
		string(t.State), subStateStr, string(t.Reason),
	).Inc()
	return nil
}

// initialize dispatches to the appropriate initialization path based on member state.
func (i *Initializer) initialize(ctx context.Context, mode string) error {
	// Path B: scale-up -- new member joining existing cluster as learner.
	if i.hasCreateAsLearnerAnnotation {
		return i.initializeAsLearner(ctx)
	}
	// Path A/C: existing or fresh member -- use DB validation.
	return i.initializeFromDB(ctx, mode)
}

// initializeFromDB handles Path A (restart with DB validation) and Path C (fresh single-node).
func (i *Initializer) initializeFromDB(ctx context.Context, mode string) error {
	var valMode validator.ValidationMode
	var subState statemachine.SubState
	var reason statemachine.Reason

	if mode == "sanity" {
		subState = statemachine.SubStateDBValidationSanity
		reason = statemachine.ReasonDetectedPreviousCleanExit
		valMode = validator.ValidationModeSanity
	} else {
		subState = statemachine.SubStateDBValidationFull
		reason = statemachine.ReasonDetectedPreviousUncleanExit
		valMode = validator.ValidationModeFull
	}

	if err := i.record(ctx, statemachine.Transition{
		State:    statemachine.StateInitializing,
		SubState: &subState,
		Reason:   reason,
	}); err != nil {
		return fmt.Errorf("failed to record DB validation transition: %w", err)
	}

	result := validator.Validate(i.dataDir, valMode)
	i.logger.Info("DB validation result",
		zap.String("mode", mode),
		zap.Bool("valid", result.Valid),
		zap.Bool("isSingleNode", i.isSingleNode),
		zap.Bool("isDataDirEmpty", i.isDataDirEmpty()),
		zap.Bool("storeConfigured", i.store != nil),
	)
	if result.Valid {
		// Single-node: if the data directory is empty and a snapstore is configured,
		// attempt restoration from the latest snapshot before proceeding. This handles
		// the PVC-deletion scenario where etcd would otherwise bootstrap fresh and lose
		// all data. tryRestore is a no-op when no snapshots are found (ErrNoSnapshotFound),
		// so a genuinely fresh cluster is handled identically to the no-store case.
		if i.isSingleNode && i.isDataDirEmpty() && i.store != nil {
			restorationSubState := statemachine.SubStateRestoration
			if err := i.record(ctx, statemachine.Transition{
				State:    statemachine.StateInitializing,
				SubState: &restorationSubState,
				Reason:   statemachine.ReasonDBValidationFailed,
			}); err != nil {
				return fmt.Errorf("failed to record restoration transition: %w", err)
			}

			restoreStart := metav1.Now()
			if err := i.tryRestore(ctx); err != nil {
				restoreEnd := metav1.Now()
				msg := err.Error()
				_ = i.memberClient.UpdateStatus(ctx, i.memberName, i.memberNamespace, member.UpdateStatusOpts{
					LastRestoration: &member.LastRestorationStatus{
						Type:      "FromSnapshot",
						Status:    "Failed",
						StartTime: restoreStart,
						EndTime:   &restoreEnd,
						Message:   &msg,
					},
				})
				return fmt.Errorf("restoration failed for member %s: %w", i.memberName, err)
			}
			restoreEnd := metav1.Now()
			// Only update LastRestoration status if there were actual snapshots to restore from.
			// tryRestore returns nil for both "no snapshots" (fresh cluster) and "restore succeeded".
			// We check the data dir: if restoration populated it, snapshots were found.
			if !i.isDataDirEmpty() {
				if err := i.memberClient.UpdateStatus(ctx, i.memberName, i.memberNamespace, member.UpdateStatusOpts{
					LastRestoration: &member.LastRestorationStatus{
						Type:      "FromSnapshot",
						Status:    "Succeeded",
						StartTime: restoreStart,
						EndTime:   &restoreEnd,
					},
				}); err != nil {
					i.logger.Warn("failed to update LastRestoration status after successful restore", zap.Error(err))
				}

				leaderSubState := statemachine.SubStateLeader
				if err := i.record(ctx, statemachine.Transition{
					State:    statemachine.StateStarted,
					SubState: &leaderSubState,
					Reason:   statemachine.ReasonRestorationSucceeded,
				}); err != nil {
					return fmt.Errorf("failed to record restoration success transition: %w", err)
				}
				return nil
			}
			// No snapshots found — fall through to fresh start (same as no-store case).
		}

		// Multi-node: check for data-loss recovery.
		if !i.isSingleNode {
			needsRecovery, err := i.needsDataLossRecovery(ctx)
			if err != nil {
				i.logger.Warn("failed to check cluster membership for data-loss detection, falling back to empty-dir check",
					zap.Error(err))
				needsRecovery = i.isDataDirEmpty()
			}
			if needsRecovery {
				i.logger.Info("detected data loss in multi-node cluster, triggering recovery",
					zap.String("member", i.memberName),
					zap.String("dataDir", i.dataDir),
				)
				return i.initializeDataLossRecovery(ctx)
			}
		}

		subStateStarted := statemachine.SubStateLeader
		if !i.isSingleNode {
			subStateStarted = statemachine.SubStateFollower
		}

		reasonStarted := statemachine.ReasonDBValidationSucceeded
		if i.isSingleNode && mode == "full" {
			reasonStarted = statemachine.ReasonNewSingleNodeClusterCreated
		}

		if err := i.record(ctx, statemachine.Transition{
			State:    statemachine.StateStarted,
			SubState: &subStateStarted,
			Reason:   reasonStarted,
		}); err != nil {
			return fmt.Errorf("failed to record started transition: %w", err)
		}
		return nil
	}

	// DB validation failed.
	if i.isSingleNode {
		restorationSubState := statemachine.SubStateRestoration
		if err := i.record(ctx, statemachine.Transition{
			State:    statemachine.StateInitializing,
			SubState: &restorationSubState,
			Reason:   statemachine.ReasonDBValidationFailed,
		}); err != nil {
			return fmt.Errorf("failed to record restoration transition: %w", err)
		}

		restoreStart := metav1.Now()
		if err := i.tryRestore(ctx); err != nil {
			restoreEnd := metav1.Now()
			msg := err.Error()
			_ = i.memberClient.UpdateStatus(ctx, i.memberName, i.memberNamespace, member.UpdateStatusOpts{
				LastRestoration: &member.LastRestorationStatus{
					Type:      "FromSnapshot",
					Status:    "Failed",
					StartTime: restoreStart,
					EndTime:   &restoreEnd,
					Message:   &msg,
				},
			})
			return fmt.Errorf("restoration failed for member %s: %w", i.memberName, err)
		}
		restoreEnd := metav1.Now()
		if err := i.memberClient.UpdateStatus(ctx, i.memberName, i.memberNamespace, member.UpdateStatusOpts{
			LastRestoration: &member.LastRestorationStatus{
				Type:      "FromSnapshot",
				Status:    "Succeeded",
				StartTime: restoreStart,
				EndTime:   &restoreEnd,
			},
		}); err != nil {
			i.logger.Warn("failed to update LastRestoration status after successful restore", zap.Error(err))
		}

		leaderSubState := statemachine.SubStateLeader
		if err := i.record(ctx, statemachine.Transition{
			State:    statemachine.StateStarted,
			SubState: &leaderSubState,
			Reason:   statemachine.ReasonRestorationSucceeded,
		}); err != nil {
			return fmt.Errorf("failed to record restoration success transition: %w", err)
		}
		return nil
	}

	// Multi-node: DB validation failed.
	// Per DEP-04: record New state, remove data directory, remove this member from the cluster,
	// then re-join as a learner. This avoids an infinite restart loop where the member repeatedly
	// fails validation with the same corrupted DB.
	i.logger.Warn("DB validation failed for multi-node member, re-joining cluster as learner",
		zap.String("member", i.memberName),
		zap.Error(result.Err),
	)
	if err := i.record(ctx, statemachine.Transition{
		State:   statemachine.StateNew,
		Reason:  statemachine.ReasonDBValidationFailed,
		Message: fmt.Sprintf("DB validation failed, re-joining as learner: %v", result.Err),
	}); err != nil {
		return fmt.Errorf("failed to record New transition after DB validation failure: %w", err)
	}
	if err := os.RemoveAll(i.dataDir); err != nil {
		return fmt.Errorf("failed to remove data directory after DB validation failure for %s: %w", i.memberName, err)
	}
	if err := i.clusterClient.RemoveStaleMember(ctx, i.peerURL); err != nil {
		// Log but do not fatal — the member may not be registered yet (e.g. first startup).
		i.logger.Warn("failed to remove stale member entry before re-joining, proceeding anyway",
			zap.String("peerURL", i.peerURL),
			zap.Error(err),
		)
	}
	return i.joinAsLearner(ctx)
}

// isDataDirEmpty returns true if the etcd data directory contains no DB or WAL files.
func (i *Initializer) isDataDirEmpty() bool {
	dbPath := filepath.Join(i.dataDir, "member", "snap", "db")
	if _, err := os.Stat(dbPath); err == nil {
		return false
	}
	walDir := filepath.Join(i.dataDir, "member", "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return true
	}
	return len(entries) == 0
}

// needsDataLossRecovery returns true if this member needs to rejoin the cluster as a learner.
// Returns false when the cluster is unreachable (fresh bootstrap — no cluster exists yet).
func (i *Initializer) needsDataLossRecovery(ctx context.Context) (bool, error) {
	// Fast TCP check first — avoids the gRPC connection-establishment hang when no etcd is running.
	if !i.isEtcdReachable(ctx) {
		i.logger.Info("cluster not reachable via TCP, skipping data-loss check",
			zap.String("member", i.memberName),
		)
		return false, nil
	}

	if !i.isDataDirEmpty() {
		// Non-empty data dir: check if THIS member is still registered in the cluster.
		// If not, it was removed and needs to rejoin.
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		wasInCluster, err := i.clusterClient.WasMemberInCluster(checkCtx, i.peerURL)
		if err != nil {
			return false, fmt.Errorf("failed to query cluster membership: %w", err)
		}
		return !wasInCluster, nil
	}
	// Data dir is empty — do a full membership query to check for data-loss recovery.
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	members, err := i.clusterClient.ListMembers(checkCtx)
	if err != nil {
		// Cluster not reachable → fresh bootstrap, not data-loss recovery.
		i.logger.Info("cluster not reachable, treating empty data dir as fresh bootstrap",
			zap.String("member", i.memberName),
			zap.Error(err),
		)
		return false, nil
	}
	// Cluster is reachable and data dir is empty → data-loss recovery.
	for _, m := range members {
		for _, u := range m.PeerURLs {
			if u == i.peerURL {
				return true, nil
			}
		}
	}
	// Data dir empty and not in cluster → joining as new member (scale-up handled elsewhere).
	return false, nil
}

// isEtcdReachable checks whether any etcd peer in the cluster is reachable via TCP.
// For multi-node clusters it tries peers from initial-cluster (excluding self) first,
// so that a corrupted or restarting member can still detect the existing cluster.
// Falls back to the local endpoint for single-node or when no peers are found.
// This avoids blocking on gRPC connection establishment when no etcd cluster exists yet.
func (i *Initializer) isEtcdReachable(ctx context.Context) bool {
	addrs := i.clusterTCPAddrs()
	for _, addr := range addrs {
		if addr == "" {
			continue
		}
		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := i.tcpDialFn(dialCtx, "tcp", addr)
		cancel()
		if err == nil {
			conn.Close() //nolint:errcheck
			return true
		}
	}
	return false
}

// clusterTCPAddrs returns TCP addresses to probe for cluster reachability.
// Peer client addresses (derived from initial-cluster peer URLs, converting port 2380→2379)
// are tried first so that a corrupted member can detect its live peers.
// The local etcd endpoint is appended as a final fallback.
func (i *Initializer) clusterTCPAddrs() []string {
	var addrs []string

	// Parse initial-cluster: "name1=peerURL1,name2=peerURL2,..."
	// For each peer that is NOT this member, derive its client address (2380→2379).
	for _, part := range splitInitialCluster(i.initialCluster) {
		eqIdx := indexOf(part, '=')
		if eqIdx < 0 {
			continue
		}
		name := part[:eqIdx]
		peerURL := part[eqIdx+1:]
		if name == i.memberName {
			continue // skip self
		}
		// Convert peer port (2380) to client port (2379) for TCP reachability check.
		addr := urlToTCPAddr(peerURL)
		addr = strings.Replace(addr, ":2380", ":2379", 1)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}

	// Append local endpoint as fallback (works for single-node and when self is up).
	if local := i.etcdTCPAddr(); local != "" {
		addrs = append(addrs, local)
	}
	return addrs
}

// splitInitialCluster splits an initial-cluster string on commas.
func splitInitialCluster(s string) []string {
	if s == "" {
		return nil
	}
	var parts []string
	start := 0
	for idx := 0; idx < len(s); idx++ {
		if s[idx] == ',' {
			parts = append(parts, s[start:idx])
			start = idx + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// etcdTCPAddr extracts the host:port from the etcd endpoint URL.
func (i *Initializer) etcdTCPAddr() string {
	return urlToTCPAddr(i.etcdEndpoint)
}

// urlToTCPAddr strips scheme and path from a URL and returns host:port.
func urlToTCPAddr(ep string) string {
	// Strip scheme prefix.
	for _, prefix := range []string{"https://", "http://"} {
		if len(ep) > len(prefix) && ep[:len(prefix)] == prefix {
			ep = ep[len(prefix):]
			break
		}
	}
	// Strip trailing path.
	if idx := indexOf(ep, '/'); idx >= 0 {
		ep = ep[:idx]
	}
	return ep
}

// indexOf returns the index of the first occurrence of b in s, or -1.
func indexOf(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// initializeDataLossRecovery handles the case where a multi-node member's PVC was deleted.
func (i *Initializer) initializeDataLossRecovery(ctx context.Context) error {
	i.inDataLossRecovery.Store(true)

	// Record New state with DataLossRecoveryStarted reason so etcd-druid can distinguish
	// a data-loss re-join from a normal scale-up in the transition history.
	if err := i.record(ctx, statemachine.Transition{
		State:   statemachine.StateNew,
		Reason:  statemachine.ReasonDataLossRecoveryStarted,
		Message: "data-loss detected: removing stale member entry and re-joining as learner",
	}); err != nil {
		return fmt.Errorf("failed to record data-loss recovery transition: %w", err)
	}

	i.logger.Info("removing stale member entry before rejoining as learner",
		zap.String("peerURL", i.peerURL),
	)
	if err := i.clusterClient.RemoveStaleMember(ctx, i.peerURL); err != nil {
		return fmt.Errorf("failed to remove stale member for %s: %w", i.memberName, err)
	}

	return i.joinAsLearner(ctx)
}

// initializeAsLearner handles Path B: new member joining as learner (scale-up).
func (i *Initializer) initializeAsLearner(ctx context.Context) error {
	// DEP-04: record Initializing state before any cluster interaction so etcd-druid
	// can track the full transition sequence for scale-up members.
	initSubState := statemachine.SubStateDBValidationSanity
	if err := i.record(ctx, statemachine.Transition{
		State:    statemachine.StateInitializing,
		SubState: &initSubState,
		Reason:   statemachine.ReasonClusterScaledUp,
		Message:  "new member added via scale-up, proceeding to join as learner",
	}); err != nil {
		return fmt.Errorf("failed to record Initializing transition for scale-up: %w", err)
	}

	// Check if we're already a learner.
	alreadyJoined, existingMemberID, err := i.findExistingLearnerMember(ctx)
	if err != nil {
		i.logger.Warn("failed to check existing cluster membership, proceeding with fresh join", zap.Error(err))
	}

	if alreadyJoined {
		i.logger.Info("member already registered as learner in cluster, skipping AddLearner",
			zap.Uint64("memberID", existingMemberID))
		i.isLearnerMember.Store(true)
		learnerSubState := statemachine.SubStateLearner
		if err := i.record(ctx, statemachine.Transition{
			State:    statemachine.StateStarting,
			SubState: &learnerSubState,
			Reason:   statemachine.ReasonJoinedAsLearner,
		}); err != nil {
			return fmt.Errorf("failed to record rejoined learner transition: %w", err)
		}
		go i.promoteAfterSync(ctx, existingMemberID)
		return nil
	}

	return i.joinAsLearner(ctx)
}

// joinAsLearner is the shared learner-join logic.
func (i *Initializer) joinAsLearner(ctx context.Context) error {
	pendingSubState := statemachine.SubStatePendingLearner
	if err := i.record(ctx, statemachine.Transition{
		State:    statemachine.StateStarting,
		SubState: &pendingSubState,
		Reason:   statemachine.ReasonWaitingToJoinAsLearner,
	}); err != nil {
		return fmt.Errorf("failed to record pending learner transition: %w", err)
	}

	// Clear data dir so etcd starts fresh.
	if err := os.RemoveAll(i.dataDir); err != nil {
		return fmt.Errorf("failed to clear data directory before learner join: %w", err)
	}

	memberID, err := i.clusterClient.AddLearner(ctx, i.peerURL)
	if err != nil {
		return fmt.Errorf("failed to add member %s as learner: %w", i.memberName, err)
	}
	i.isLearnerMember.Store(true)

	learnerSubState := statemachine.SubStateLearner
	if err := i.record(ctx, statemachine.Transition{
		State:    statemachine.StateStarting,
		SubState: &learnerSubState,
		Reason:   statemachine.ReasonJoinedAsLearner,
	}); err != nil {
		return fmt.Errorf("failed to record joined learner transition: %w", err)
	}

	go i.promoteAfterSync(ctx, memberID)
	return nil
}

// findExistingLearnerMember checks if this member is already registered as a learner.
func (i *Initializer) findExistingLearnerMember(ctx context.Context) (bool, uint64, error) {
	members, err := i.clusterClient.ListMembers(ctx)
	if err != nil {
		return false, 0, err
	}
	for _, m := range members {
		if m.IsLearner {
			for _, url := range m.PeerURLs {
				if url == i.peerURL {
					return true, m.ID, nil
				}
			}
		}
	}
	return false, 0, nil
}

// promoteAfterSync waits for the learner to sync then promotes it.
func (i *Initializer) promoteAfterSync(ctx context.Context, memberID uint64) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	if err := i.promoteLearner(ctx, memberID); err != nil {
		i.logger.Error("failed to promote learner to voting member", zap.Error(err))
		return
	}
	i.isLearnerMember.Store(false)

	followerSubState := statemachine.SubStateFollower
	if err := i.record(ctx, statemachine.Transition{
		State:    statemachine.StateStarted,
		SubState: &followerSubState,
		Reason:   statemachine.ReasonPromotedAsVotingMember,
	}); err != nil {
		i.logger.Error("failed to record promoted transition", zap.Error(err))
		return
	}

	if err := i.memberClient.RemoveCreateAsLearnerAnnotation(ctx, i.memberName, i.memberNamespace); err != nil {
		i.logger.Warn("failed to remove create-as-learner annotation", zap.Error(err))
	}
}

// promoteLearner attempts to promote the learner with exponential backoff.
func (i *Initializer) promoteLearner(ctx context.Context, memberID uint64) error {
	backoff := 5 * time.Second
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
		}
		err := i.clusterClient.PromoteMember(ctx, memberID)
		if err == nil {
			return nil
		}
		i.logger.Warn("failed to promote learner, retrying", zap.Error(err), zap.Int("attempt", attempt+1))
	}
	return fmt.Errorf("failed to promote member %x after 10 attempts", memberID)
}

// tryRestore performs restoration from the latest snapshot in the configured snapstore.
func (i *Initializer) tryRestore(ctx context.Context) error {
	if i.store == nil {
		i.logger.Info("snapstore not configured, skipping restoration -- etcd will start fresh")
		return nil
	}

	if err := os.RemoveAll(i.dataDir); err != nil {
		return fmt.Errorf("failed to clear data dir before restore: %w", err)
	}

	tempDir := i.restorationTempDir
	if tempDir == "" {
		tempDir = i.dataDir + ".restoration.tmp"
	}

	// Remove any stale temp dir from a previous interrupted restoration attempt.
	// A crashed restoration can leave a partial tempDir on the PVC, which would
	// cause the next restoration to fail or pick up incorrect data.
	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("failed to clear stale restoration temp dir %s: %w", tempDir, err)
	}

	err := restoration.Restore(
		ctx,
		i.store,
		i.compressor,
		i.dataDir,
		tempDir,
		i.memberName,
		i.peerURL,
		i.initialCluster,
		i.logger,
	)
	// Always clean up the temp dir — whether restoration succeeded, failed, or found no snapshots.
	// This ensures stale temp state doesn't accumulate across restarts.
	_ = os.RemoveAll(tempDir)
	if errors.Is(err, restoration.ErrNoSnapshotFound) {
		// No snapshots exist yet — this is a genuinely fresh cluster.
		// Treat identically to "no store configured": etcd will bootstrap from scratch.
		i.logger.Info("no snapshots found in store, skipping restoration -- etcd will start fresh")
		return nil
	}
	return err
}
