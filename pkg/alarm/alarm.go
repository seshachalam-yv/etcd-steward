// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package alarm monitors etcd alarms and performs remediation for NOSPACE conditions.
package alarm

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/metrics"
)

// MaintenanceAPI is the subset of the etcd Maintenance interface needed by Handler.
// It uses clientv3 types to match the etcd client library interface.
type MaintenanceAPI interface {
	AlarmList(ctx context.Context) (*clientv3.AlarmResponse, error)
	AlarmDisarm(ctx context.Context, m *clientv3.AlarmMember) (*clientv3.AlarmResponse, error)
	Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error)
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
}

// KVCompactAPI is the subset of the etcd KV interface for compaction.
type KVCompactAPI interface {
	Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error)
}

// Handler monitors etcd alarms and remediates NOSPACE conditions.
type Handler struct {
	maintenance        MaintenanceAPI
	kv                 KVCompactAPI
	endpoint           string
	compactRevisionLag int64
	interval           time.Duration
	namespace          string
	name               string
	logger             *zap.Logger

	mu         sync.Mutex
	lastDefrag *member.DefragInfo // set after every defragmentation
}

// New creates a Handler that polls for etcd alarms and remediates NOSPACE.
func New(
	maintenance MaintenanceAPI,
	kv KVCompactAPI,
	endpoint string,
	compactRevisionLag int64,
	interval time.Duration,
	namespace, name string,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		maintenance:        maintenance,
		kv:                 kv,
		endpoint:           endpoint,
		compactRevisionLag: compactRevisionLag,
		interval:           interval,
		namespace:          namespace,
		name:               name,
		logger:             logger,
	}
}

// Run polls AlarmList at the configured interval until ctx is cancelled.
func (h *Handler) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.check(ctx); err != nil {
				h.logger.Error("alarm check failed", zap.Error(err))
			}
		}
	}
}

// check performs a single alarm check and remediation cycle.
func (h *Handler) check(ctx context.Context) error {
	resp, err := h.maintenance.AlarmList(ctx)
	if err != nil {
		return fmt.Errorf("failed to list alarms: %w", err)
	}

	if len(resp.Alarms) == 0 {
		metrics.ComponentHealth.WithLabelValues(h.namespace, h.name, "alarm_handler").Set(1)
		return nil
	}

	for _, am := range resp.Alarms {
		switch am.Alarm {
		case pb.AlarmType_NOSPACE:
			h.logger.Warn("NOSPACE alarm detected, attempting remediation",
				zap.Uint64("memberID", am.MemberID),
			)
			camPtr := (*clientv3.AlarmMember)(am)
			if err := h.remediateNOSPACE(ctx, camPtr); err != nil {
				h.logger.Error("NOSPACE remediation failed", zap.Error(err))
				metrics.ComponentHealth.WithLabelValues(h.namespace, h.name, "alarm_handler").Set(0)
				return err
			}
			metrics.ComponentHealth.WithLabelValues(h.namespace, h.name, "alarm_handler").Set(1)

		case pb.AlarmType_CORRUPT:
			h.logger.Error("CORRUPT alarm detected, manual intervention required",
				zap.Uint64("memberID", am.MemberID),
			)
			metrics.ComponentHealth.WithLabelValues(h.namespace, h.name, "alarm_handler").Set(0)
			// Do NOT disarm CORRUPT alarms.

		default:
			h.logger.Warn("unknown alarm type", zap.Int32("type", int32(am.Alarm)))
		}
	}

	return nil
}

// remediateNOSPACE handles the NOSPACE alarm by compacting, defragmenting, and disarming.
func (h *Handler) remediateNOSPACE(ctx context.Context, alarm *clientv3.AlarmMember) error {
	// Get current revision and initial DB size.
	statusResp, err := h.maintenance.Status(ctx, h.endpoint)
	if err != nil {
		return fmt.Errorf("failed to get status: %w", err)
	}

	currentRev := statusResp.Header.Revision
	initialDBBytes := statusResp.DbSize
	compactRev := currentRev - h.compactRevisionLag
	if compactRev < 1 {
		compactRev = 1
	}

	h.logger.Info("compacting to revision",
		zap.Int64("compactRevision", compactRev),
		zap.Int64("currentRevision", currentRev),
	)

	if _, err := h.kv.Compact(ctx, compactRev, clientv3.WithCompactPhysical()); err != nil {
		return fmt.Errorf("compact failed at revision %d: %w", compactRev, err)
	}

	reason := "NSPACEAlarm"
	startTime := metav1.Now()
	h.logger.Info("defragmenting", zap.String("endpoint", h.endpoint))
	defragStart := time.Now()
	_, defragErr := h.maintenance.Defragment(ctx, h.endpoint)
	defragDuration := time.Since(defragStart).Seconds()

	statusCode := "success"
	if defragErr != nil {
		statusCode = "failure"
	}
	metrics.DefragmentationDurationSeconds.WithLabelValues(h.namespace, h.name, statusCode, reason).Observe(defragDuration)

	endTime := metav1.Now()

	// Get final DB size (best-effort; use 0 if unavailable).
	var finalDBBytes int64
	if postStatus, err := h.maintenance.Status(ctx, h.endpoint); err == nil {
		finalDBBytes = postStatus.DbSize
	}

	initialQty := resource.NewMilliQuantity(initialDBBytes*1000, resource.BinarySI)
	finalQty := resource.NewMilliQuantity(finalDBBytes*1000, resource.BinarySI)

	defragInfo := &member.DefragInfo{
		StartTime:     startTime,
		EndTime:       &endTime,
		InitialDBSize: initialQty,
		FinalDBSize:   finalQty,
		Reason:        &reason,
	}
	if defragErr != nil {
		msg := defragErr.Error()
		defragInfo.Message = &msg
	}

	h.mu.Lock()
	h.lastDefrag = defragInfo
	h.mu.Unlock()

	if defragErr != nil {
		return fmt.Errorf("defragment failed: %w", defragErr)
	}

	h.logger.Info("disarming NOSPACE alarm", zap.Uint64("memberID", alarm.MemberID))
	if _, err := h.maintenance.AlarmDisarm(ctx, alarm); err != nil {
		return fmt.Errorf("alarm disarm failed: %w", err)
	}

	h.logger.Info("NOSPACE alarm remediated successfully")
	return nil
}

// ProvideInfo implements member.InfoProvider.
// Returns the most recent defragmentation info collected by this Handler.
// Safe to call concurrently with Run.
func (h *Handler) ProvideInfo() member.StatusInfo {
	h.mu.Lock()
	d := h.lastDefrag
	h.mu.Unlock()
	if d == nil {
		return member.StatusInfo{}
	}
	snapshot := *d
	return member.StatusInfo{LastDefragmentation: &snapshot}
}
