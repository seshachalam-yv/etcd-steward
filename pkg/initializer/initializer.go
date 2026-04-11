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

	recorder             statemachine.Recorder
	memberClient         member.Client
	clusterClient        etcdclient.ClusterClient
	serviceClusterClient etcdclient.ClusterClient // optional; uses ClusterIP service endpoint for scale-out detection
	etcdStatusAPI        EtcdStatusAPI
	etcdEndpoint         string
	serviceEndpoint      string // ClusterIP service endpoint for TCP reachability check during scale-out
	logger               *zap.Logger
	store                snapstore.Snapstore    // nil if no backup configured
	compressor           compression.Compressor // nil if no backup configured

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
	return i.hasCreateAsLearnerAnnotation || i.inDataLossRecovery.Load() || i.isLearnerMember.Load()
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

// SetEtcdStatusAPI injects the etcd status client after etcd has started.
// Must be called before the first lease renewal tick to populate memberID/clusterID.
func (i *Initializer) SetEtcdStatusAPI(api EtcdStatusAPI) {
	i.etcdStatusAPI = api
}

// SetServiceClusterClient injects a cluster client that connects via the ClusterIP service
// endpoint (e.g. test-client:2379). Used during scale-out to detect existing cluster membership
// without requiring the local etcd to be running.
func (i *Initializer) SetServiceClusterClient(client etcdclient.ClusterClient, serviceEndpoint string) {
	i.serviceClusterClient = client
	i.serviceEndpoint = serviceEndpoint
}

// activeClusterClient returns serviceClusterClient if configured, otherwise clusterClient.
// The service cluster client connects to the ClusterIP service and is usable even when
// the local etcd has not started yet (scale-out scenario).
func (i *Initializer) activeClusterClient() etcdclient.ClusterClient {
	if i.serviceClusterClient != nil {
		return i.serviceClusterClient
	}
	return i.clusterClient
}

// GetCurrentMembersInitialCluster fetches the current cluster membership and returns
// an initial-cluster string ("name=peerURL,...") containing only the members that are
// actually in the cluster right now. Used when joining as a learner so that the
// initial-cluster field in the etcd config matches the real cluster size rather than
// the full planned replica count from the ConfigMap.
//
// configMapEntries is a name→peerURL map from the ConfigMap's initial-cluster field.
// It is used to:
//  1. Resolve names for unnamed learner members (newly added, no name yet in etcd).
//     Matching is done by normalising away the URL scheme so http://host and https://host
//     are considered the same peer. This handles TLS-transition scenarios.
//  2. Use the ConfigMap's peerURL for unnamed learners (they don't have a live URL yet).
//
// For existing named members, the live cluster peerURL is used as-is to avoid
// mismatches: if TLS migration is in progress, the live cluster URL reflects the
// member's current state, which etcd validates on startup.
//
// Returns empty string if the cluster is unreachable (caller falls back to ConfigMap value).
func (i *Initializer) GetCurrentMembersInitialCluster(ctx context.Context, configMapEntries map[string]string) string {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	members, err := i.activeClusterClient().ListMembers(queryCtx)
	if err != nil {
		i.logger.Warn("GetCurrentMembersInitialCluster: failed to list members, falling back to ConfigMap value",
			zap.Error(err),
		)
		return ""
	}

	// Build a reverse map: normalised peerURL host+path → ConfigMap name, for unnamed learner lookup.
	// "normalised" means stripping the scheme prefix so http://host and https://host match.
	normalise := func(u string) string {
		if idx := strings.Index(u, "://"); idx >= 0 {
			return u[idx+3:]
		}
		return u
	}
	normToName := make(map[string]string, len(configMapEntries))
	nameToConfigURL := make(map[string]string, len(configMapEntries))
	for name, url := range configMapEntries {
		normToName[normalise(url)] = name
		nameToConfigURL[name] = url
	}

	parts := make([]string, 0, len(members))
	for _, m := range members {
		if len(m.PeerURLs) == 0 {
			continue
		}
		name := m.Name

		if name == "" {
			// Unnamed learner: resolve name using normalised URL matching against ConfigMap.
			for _, u := range m.PeerURLs {
				if n, ok := normToName[normalise(u)]; ok {
					name = n
					break
				}
			}
			if name == "" {
				continue // cannot resolve — skip
			}
		}

		// Always use the ConfigMap URL for all members (named or unnamed).
		// The ConfigMap represents the desired state and always has the correct scheme
		// (e.g. https:// after TLS is enabled). The live cluster peerURL may be stale
		// (still http://) because etcd does not automatically update member peerURLs on restart.
		peerURL, ok := nameToConfigURL[name]
		if !ok {
			// Member not in ConfigMap (e.g. a concurrent scale-up peer not yet in the ConfigMap).
			// Fall back to live URL.
			peerURL = m.PeerURLs[0]
		}

		// If the member's registered peer URL differs from the ConfigMap URL (scheme mismatch
		// during TLS migration), proactively update it via MemberUpdate. This ensures that
		// when the next learner joins, etcd's peer URL validation passes.
		if len(m.PeerURLs) > 0 && m.PeerURLs[0] != peerURL && name != "" {
			updateCtx, updateCancel := context.WithTimeout(ctx, 10*time.Second)
			updateErr := i.activeClusterClient().UpdateMemberPeerURL(updateCtx, m.ID, peerURL)
			updateCancel()
			if updateErr != nil {
				i.logger.Warn("GetCurrentMembersInitialCluster: failed to update member peerURL",
					zap.String("member", name),
					zap.String("from", m.PeerURLs[0]),
					zap.String("to", peerURL),
					zap.Error(updateErr),
				)
			} else {
				i.logger.Info("GetCurrentMembersInitialCluster: updated stale peer URL in cluster membership",
					zap.String("member", name),
					zap.String("from", m.PeerURLs[0]),
					zap.String("to", peerURL),
				)
			}
		}

		parts = append(parts, name+"="+peerURL)
	}
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += "," + p
	}
	return result
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

	resolvedPath, err := i.initialize(ctx, mode)
	if resolvedPath != "" {
		path = resolvedPath
	}
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
// It returns the DEP-04 path label ("A", "B", "C", "D") for metrics and an error.
func (i *Initializer) initialize(ctx context.Context, mode string) (string, error) {
	// Path B: scale-up -- new member joining existing cluster as learner.
	if i.hasCreateAsLearnerAnnotation {
		return "B", i.initializeAsLearner(ctx)
	}
	// Path A/C/D: existing or fresh member -- use DB validation.
	return i.initializeFromDB(ctx, mode)
}

// initializeFromDB handles Path A (restart with DB validation), Path C (fresh single-node),
// and Path D (snapshot restore). Returns the DEP-04 path label for metrics.
func (i *Initializer) initializeFromDB(ctx context.Context, mode string) (string, error) {
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
		return "unknown", fmt.Errorf("failed to record DB validation transition: %w", err)
	}

	result := validator.Validate(i.dataDir, valMode)
	validationResult := "success"
	if !result.Valid {
		validationResult = "failure"
	}
	metrics.ValidationTotal.WithLabelValues(i.memberNamespace, i.memberName, mode, validationResult).Inc()

	// Detect read-only data volume and surface it as a condition on the EtcdMember.
	if result.IsReadOnly {
		cond := member.Condition{
			Type:    member.ConditionDataVolumeReadOnly,
			Status:  "True",
			Reason:  "PVCReadOnly",
			Message: result.Err.Error(),
		}
		if condErr := i.memberClient.SetCondition(ctx, i.memberName, i.memberNamespace, cond); condErr != nil {
			i.logger.Error("failed to set DataVolumeReadOnly condition", zap.Error(condErr))
		}
	}

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
				return "D", fmt.Errorf("failed to record restoration transition: %w", err)
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
				return "D", fmt.Errorf("restoration failed for member %s: %w", i.memberName, err)
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
					return "D", fmt.Errorf("failed to record restoration success transition: %w", err)
				}
				return "D", nil
			}
			// No snapshots found — fall through to fresh start (Path C).
		}

		// Multi-node: check for data-loss recovery or scale-up join.
		if !i.isSingleNode {
			needsRecovery, isNewJoin, err := i.needsDataLossRecovery(ctx)
			if err != nil {
				i.logger.Warn("failed to check cluster membership for data-loss detection, falling back to empty-dir check",
					zap.Error(err))
				needsRecovery = i.isDataDirEmpty()
				isNewJoin = false
			}
			if needsRecovery {
				i.logger.Info("detected data loss in multi-node cluster, triggering recovery",
					zap.String("member", i.memberName),
					zap.String("dataDir", i.dataDir),
				)
				return "D", i.initializeDataLossRecovery(ctx)
			}
			if isNewJoin {
				i.logger.Info("cluster reachable and member not registered, joining as learner (scale-up)",
					zap.String("member", i.memberName),
				)
				return "B", i.initializeAsLearner(ctx)
			}
		}

		subStateStarted := statemachine.SubStateLeader
		if !i.isSingleNode {
			subStateStarted = statemachine.SubStateFollower
		}

		reasonStarted := statemachine.ReasonDBValidationSucceeded
		// Path C: fresh single-node cluster with empty data dir.
		pathLabel := "A"
		if i.isSingleNode && mode == "full" {
			reasonStarted = statemachine.ReasonNewSingleNodeClusterCreated
			pathLabel = "C"
		}

		if err := i.record(ctx, statemachine.Transition{
			State:    statemachine.StateStarted,
			SubState: &subStateStarted,
			Reason:   reasonStarted,
		}); err != nil {
			return pathLabel, fmt.Errorf("failed to record started transition: %w", err)
		}
		return pathLabel, nil
	}

	// DB validation failed — Path D (snapshot restore).
	if i.isSingleNode {
		restorationSubState := statemachine.SubStateRestoration
		if err := i.record(ctx, statemachine.Transition{
			State:    statemachine.StateInitializing,
			SubState: &restorationSubState,
			Reason:   statemachine.ReasonDBValidationFailed,
		}); err != nil {
			return "D", fmt.Errorf("failed to record restoration transition: %w", err)
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
			return "D", fmt.Errorf("restoration failed for member %s: %w", i.memberName, err)
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
			return "D", fmt.Errorf("failed to record restoration success transition: %w", err)
		}
		return "D", nil
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
		return "D", fmt.Errorf("failed to record New transition after DB validation failure: %w", err)
	}
	if err := os.RemoveAll(i.dataDir); err != nil {
		return "D", fmt.Errorf("failed to remove data directory after DB validation failure for %s: %w", i.memberName, err)
	}
	if err := i.activeClusterClient().RemoveStaleMember(ctx, i.peerURL); err != nil {
		// Log but do not fatal — the member may not be registered yet (e.g. first startup).
		i.logger.Warn("failed to remove stale member entry before re-joining, proceeding anyway",
			zap.String("peerURL", i.peerURL),
			zap.Error(err),
		)
	}
	return "D", i.joinAsLearner(ctx)
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

// needsDataLossRecovery returns (needsRecovery, isNewJoin, error).
// needsRecovery is true if this member was previously in the cluster but its data dir is empty.
// isNewJoin is true if the cluster is reachable but this member has never been registered (scale-up).
// Returns (false, false, nil) when the cluster is unreachable (fresh bootstrap).
func (i *Initializer) needsDataLossRecovery(ctx context.Context) (bool, bool, error) {
	// Fast TCP check first — avoids the gRPC connection-establishment hang when no etcd is running.
	if !i.isEtcdReachable(ctx) {
		i.logger.Info("cluster not reachable via TCP, skipping data-loss check",
			zap.String("member", i.memberName),
		)
		return false, false, nil
	}

	// Use the service cluster client (ClusterIP service) when available — it can reach the
	// existing cluster even when the local etcd endpoint is not yet running (scale-out).
	// Fall back to the local cluster client for single-node or when not configured.
	cc := i.clusterClient
	if i.serviceClusterClient != nil {
		cc = i.serviceClusterClient
	}

	if !i.isDataDirEmpty() {
		// Non-empty data dir: check if THIS member is still registered in the cluster.
		// If not, it was removed and needs to rejoin.
		// Use scheme-agnostic matching: during TLS migration the cluster may still have the
		// old http:// URL for this member while we now advertise https://. Same host:port → same member.
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		members, err := cc.ListMembers(checkCtx)
		if err != nil {
			return false, false, fmt.Errorf("failed to query cluster membership: %w", err)
		}
		registered := isMemberRegisteredByHostPort(members, i.peerURL)
		return !registered, false, nil
	}
	// Data dir is empty — do a full membership query to determine state.
	// Retry with backoff: TCP probe succeeded so a cluster likely exists; gRPC may need a
	// moment for the kube-proxy routing to stabilise after a scale-out.
	var members []etcdclient.Member
	{
		const maxAttempts = 5
		backoff := 1 * time.Second
		var lastErr error
		for attempt := range maxAttempts {
			checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			members, lastErr = cc.ListMembers(checkCtx)
			cancel()
			if lastErr == nil {
				break
			}
			i.logger.Info("ListMembers failed, retrying",
				zap.String("member", i.memberName),
				zap.Int("attempt", attempt+1),
				zap.Error(lastErr),
			)
			select {
			case <-ctx.Done():
				return false, false, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
		if lastErr != nil {
			// Cluster still not reachable via gRPC → fresh bootstrap, not data-loss recovery.
			i.logger.Info("cluster not reachable after retries, treating empty data dir as fresh bootstrap",
				zap.String("member", i.memberName),
				zap.Error(lastErr),
			)
			return false, false, nil
		}
	}
	// Cluster is reachable. Check if this member is registered (scheme-agnostic match).
	if isMemberRegisteredByHostPort(members, i.peerURL) {
		// Member is registered but data dir is empty → data-loss recovery needed.
		return true, false, nil
	}
	// Cluster reachable but member not registered → new member joining via scale-up.
	return false, true, nil
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
// The ClusterIP service endpoint (if configured) is tried first — it is reachable whenever
// any healthy etcd member is running, even when the local etcd has not started yet.
// Peer client addresses (derived from initial-cluster peer URLs, converting port 2380→2379)
// are tried next so that a corrupted member can detect its live peers.
// The local etcd endpoint is appended as a final fallback.
func (i *Initializer) clusterTCPAddrs() []string {
	var addrs []string

	// ClusterIP service endpoint is the most reliable probe for scale-out: it routes to any
	// healthy pod in the cluster, even when the joining member's local etcd hasn't started.
	if i.serviceEndpoint != "" {
		if addr := urlToTCPAddr(i.serviceEndpoint); addr != "" {
			addrs = append(addrs, addr)
		}
	}

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

// isMemberRegisteredByHostPort returns true if any member in the cluster has a peer URL
// whose host:port matches the host:port of peerURL (scheme-agnostic).
// This handles TLS migration: after enabling peer TLS, the member's configured URL changes
// from http:// to https://, but it's still the same member in the cluster.
func isMemberRegisteredByHostPort(members []etcdclient.Member, peerURL string) bool {
	targetHostPort := urlToTCPAddr(peerURL)
	for _, m := range members {
		for _, u := range m.PeerURLs {
			if urlToTCPAddr(u) == targetHostPort {
				return true
			}
		}
	}
	return false
}

// findStaleURLForMember returns the peer URL currently registered in the cluster for the
// member whose host:port matches peerURL (scheme-agnostic). Returns empty string if not found.
func findStaleURLForMember(members []etcdclient.Member, peerURL string) string {
	targetHostPort := urlToTCPAddr(peerURL)
	for _, m := range members {
		for _, u := range m.PeerURLs {
			if urlToTCPAddr(u) == targetHostPort {
				return u
			}
		}
	}
	return ""
}
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
	// Look up the actual URL registered in the cluster (may differ in scheme due to TLS migration).
	removeCtx, removeCancel := context.WithTimeout(ctx, 10*time.Second)
	members, listErr := i.activeClusterClient().ListMembers(removeCtx)
	removeCancel()
	staleURL := i.peerURL
	if listErr == nil {
		if found := findStaleURLForMember(members, i.peerURL); found != "" {
			staleURL = found
		}
	}
	if err := i.activeClusterClient().RemoveStaleMember(ctx, staleURL); err != nil {
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

	memberID, err := i.activeClusterClient().AddLearner(ctx, i.peerURL)
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
	members, err := i.activeClusterClient().ListMembers(ctx)
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
		err := i.activeClusterClient().PromoteMember(ctx, memberID)
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
