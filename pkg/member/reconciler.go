// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"

	"github.com/gardener/etcd-steward/pkg/metrics"
)

// StatusReconciler batches EtcdMember status updates from multiple InfoProviders.
// It runs a background loop that ticks every interval, collects info from all registered
// providers, and patches EtcdMember.status in a single merge-patch call.
//
// This avoids concurrent patch races that would occur if every component patched directly.
type StatusReconciler struct {
	client          Client
	memberName      string
	memberNamespace string
	interval        time.Duration
	logger          *zap.Logger

	mu        sync.Mutex
	providers map[string]InfoProvider
}

// NewStatusReconciler creates a StatusReconciler.
func NewStatusReconciler(
	client Client,
	memberName, namespace string,
	interval time.Duration,
	logger *zap.Logger,
) *StatusReconciler {
	return &StatusReconciler{
		client:          client,
		memberName:      memberName,
		memberNamespace: namespace,
		interval:        interval,
		logger:          logger,
		providers:       make(map[string]InfoProvider),
	}
}

// RegisterProvider registers an InfoProvider under a unique ID.
// Safe to call before or after Run starts.
func (r *StatusReconciler) RegisterProvider(id string, p InfoProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[id] = p
}

// Run starts the periodic reconcile loop until ctx is cancelled.
// Returns nil on clean shutdown.
func (r *StatusReconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// reconcile collects info from all providers and patches EtcdMember.status.
func (r *StatusReconciler) reconcile(ctx context.Context) {
	merged := r.collectInfo()

	if err := r.patch(ctx, merged); err != nil {
		r.logger.Error("failed to patch EtcdMember status", zap.Error(err),
			zap.String("member", r.memberName),
			zap.String("namespace", r.memberNamespace),
		)
		metrics.ComponentHealth.WithLabelValues(r.memberNamespace, r.memberName, "member-status-reconciler").Set(0)
		return
	}
	metrics.ComponentHealth.WithLabelValues(r.memberNamespace, r.memberName, "member-status-reconciler").Set(1)
}

// collectInfo reads all registered providers and merges their outputs.
func (r *StatusReconciler) collectInfo() StatusInfo {
	r.mu.Lock()
	snapshot := make(map[string]InfoProvider, len(r.providers))
	for id, p := range r.providers {
		snapshot[id] = p
	}
	r.mu.Unlock()

	var merged StatusInfo
	for _, p := range snapshot {
		merged = mergeStatusInfo(merged, p.ProvideInfo())
	}
	return merged
}

// patch builds and applies a merge-patch for the collected StatusInfo.
// Only non-nil fields in info are included in the patch.
func (r *StatusReconciler) patch(ctx context.Context, info StatusInfo) error {
	statusFields := make(map[string]interface{})

	if info.DBSize != nil {
		statusFields["dbSize"] = info.DBSize.String()
	}
	if info.DBSizeInUse != nil {
		statusFields["dbSizeInUse"] = info.DBSizeInUse.String()
	}
	if info.PeerTLSEnabled != nil {
		statusFields["peerTLSEnabled"] = *info.PeerTLSEnabled
	}
	if info.Snapshots != nil {
		statusFields["snapshots"] = buildSnapshotsMap(info.Snapshots)
	}
	if info.LastDefragmentation != nil {
		statusFields["lastDefragmentation"] = buildDefragMap(info.LastDefragmentation)
	}

	if len(statusFields) == 0 {
		// Nothing to patch.
		return nil
	}

	payload := map[string]interface{}{"status": statusFields}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal status patch: %w", err)
	}

	return r.client.PatchStatus(ctx, r.memberName, r.memberNamespace, types.MergePatchType, data)
}

// buildSnapshotsMap converts SnapshotInfo to a JSON-serialisable map.
func buildSnapshotsMap(s *SnapshotInfo) map[string]interface{} {
	m := make(map[string]interface{})
	if s.LastFull != nil {
		m["lastFull"] = buildSnapshotEntryMap(s.LastFull)
	}
	if s.LastDelta != nil {
		m["lastDelta"] = buildSnapshotEntryMap(s.LastDelta)
	}
	if s.AccumulatedDeltaSize != nil {
		m["accumulatedDeltaSize"] = s.AccumulatedDeltaSize.String()
	}
	return m
}

// buildSnapshotEntryMap converts a SnapshotEntry to a JSON-serialisable map.
func buildSnapshotEntryMap(e *SnapshotEntry) map[string]interface{} {
	m := map[string]interface{}{
		"name":          e.Name,
		"timestamp":     e.Timestamp.UTC().Format(time.RFC3339),
		"startRevision": e.StartRevision,
		"endRevision":   e.EndRevision,
	}
	if e.Size != nil {
		m["size"] = e.Size.String()
	}
	return m
}

// buildDefragMap converts DefragInfo to a JSON-serialisable map.
func buildDefragMap(d *DefragInfo) map[string]interface{} {
	m := map[string]interface{}{
		"startTime": d.StartTime.UTC().Format(time.RFC3339),
	}
	if d.EndTime != nil {
		m["endTime"] = d.EndTime.UTC().Format(time.RFC3339)
	}
	if d.InitialDBSize != nil {
		m["initialDBSize"] = d.InitialDBSize.String()
	}
	if d.FinalDBSize != nil {
		m["finalDBSize"] = d.FinalDBSize.String()
	}
	if d.Reason != nil {
		m["reason"] = *d.Reason
	}
	if d.Message != nil {
		m["message"] = *d.Message
	}
	return m
}
