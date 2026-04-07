// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package alarm monitors etcd alarms and performs remediation for NOSPACE conditions.
package alarm

import (
	"context"
	"fmt"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

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
	// Get current revision.
	statusResp, err := h.maintenance.Status(ctx, h.endpoint)
	if err != nil {
		return fmt.Errorf("failed to get status: %w", err)
	}

	currentRev := statusResp.Header.Revision
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

	h.logger.Info("defragmenting", zap.String("endpoint", h.endpoint))
	if _, err := h.maintenance.Defragment(ctx, h.endpoint); err != nil {
		return fmt.Errorf("defragment failed: %w", err)
	}

	h.logger.Info("disarming NOSPACE alarm", zap.Uint64("memberID", alarm.MemberID))
	if _, err := h.maintenance.AlarmDisarm(ctx, alarm); err != nil {
		return fmt.Errorf("alarm disarm failed: %w", err)
	}

	h.logger.Info("NOSPACE alarm remediated successfully")
	return nil
}
