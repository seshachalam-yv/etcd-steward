// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package alarm

import (
	"context"
	"time"

	"github.com/gardener/etcd-steward/internal/etcdclient"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Handler polls etcd alarms and takes corrective action.
// For NOSPACE alarms it compacts, defrags all endpoints, and disarms.
// For CORRUPT alarms it logs a warning.
type Handler struct {
	podName       string
	pollInterval  time.Duration
	compactRevLag int64
	endpoints     []string
	kv            etcdclient.KV
	maintenance   etcdclient.Maintenance
	logger        *zap.Logger
}

// New creates a new alarm Handler.
func New(
	podName string,
	pollInterval time.Duration,
	compactRevLag int64,
	endpoints []string,
	kv etcdclient.KV,
	maintenance etcdclient.Maintenance,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		podName:       podName,
		pollInterval:  pollInterval,
		compactRevLag: compactRevLag,
		endpoints:     endpoints,
		kv:            kv,
		maintenance:   maintenance,
		logger:        logger,
	}
}

// Run polls AlarmList at the configured interval until the context is cancelled.
func (h *Handler) Run(ctx context.Context) {
	ticker := time.NewTicker(h.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("alarm handler stopped")
			return
		case <-ticker.C:
			h.check(ctx)
		}
	}
}

func (h *Handler) check(ctx context.Context) {
	resp, err := h.maintenance.AlarmList(ctx)
	if err != nil {
		h.logger.Error("failed to list alarms", zap.Error(err))
		return
	}

	if len(resp.Alarms) == 0 {
		return
	}

	for _, a := range resp.Alarms {
		switch a.Alarm {
		case pb.AlarmType_NOSPACE:
			h.logger.Warn("NOSPACE alarm detected", zap.Uint64("memberID", a.MemberID))
			h.handleNospace(ctx, (*clientv3.AlarmMember)(a))
		case pb.AlarmType_CORRUPT:
			h.logger.Error("CORRUPT alarm detected, manual intervention required",
				zap.Uint64("memberID", a.MemberID))
		default:
			h.logger.Warn("unknown alarm type", zap.Int32("type", int32(a.Alarm)),
				zap.Uint64("memberID", a.MemberID))
		}
	}
}

func (h *Handler) handleNospace(ctx context.Context, alarm *clientv3.AlarmMember) {
	// Step 1: Get current revision.
	getResp, err := h.kv.Get(ctx, "", clientv3.WithLimit(1))
	if err != nil {
		h.logger.Error("failed to get current revision for compaction", zap.Error(err))
		return
	}
	rev := getResp.Header.Revision

	// Step 2: Compact at revision - compactRevLag.
	compactRev := rev - h.compactRevLag
	if compactRev < 1 {
		compactRev = 1
	}

	h.logger.Info("compacting", zap.Int64("revision", compactRev))
	if _, err := h.kv.Compact(ctx, compactRev); err != nil {
		h.logger.Error("compact failed", zap.Int64("revision", compactRev), zap.Error(err))
		return
	}

	// Step 3: Defragment all endpoints.
	for _, ep := range h.endpoints {
		h.logger.Info("defragmenting endpoint", zap.String("endpoint", ep))
		if _, err := h.maintenance.Defragment(ctx, ep); err != nil {
			h.logger.Error("defragment failed", zap.String("endpoint", ep), zap.Error(err))
			// Continue with remaining endpoints.
		}
	}

	// Step 4: Disarm the alarm.
	h.logger.Info("disarming NOSPACE alarm", zap.Uint64("memberID", alarm.MemberID))
	if _, err := h.maintenance.AlarmDisarm(ctx, alarm); err != nil {
		h.logger.Error("failed to disarm alarm", zap.Uint64("memberID", alarm.MemberID), zap.Error(err))
	}
}
