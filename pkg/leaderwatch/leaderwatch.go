// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package leaderwatch monitors etcd leader elections and records state transitions.
package leaderwatch

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/metrics"
	"github.com/gardener/etcd-steward/pkg/statemachine"
)

// Role represents the role of an etcd member in the cluster.
type Role string

const (
	// RoleLeader indicates the member is the cluster leader.
	RoleLeader Role = "Leader"
	// RoleMember indicates the member is a follower (non-leader voting member).
	RoleMember Role = "Member"
)

// StatusAPI is the subset of the etcd Maintenance interface used by LeaderWatcher.
type StatusAPI interface {
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
}

// LeaderWatcher polls etcd status to detect leadership changes and records transitions.
type LeaderWatcher struct {
	memberName      string
	memberNamespace string
	endpoint        string
	pollInterval    time.Duration
	statusAPI       StatusAPI
	recorder        statemachine.Recorder
	logger          *zap.Logger

	mu          sync.RWMutex
	currentRole Role
	wasLeader   *bool
	dbSize      int64 // last observed DbSize in bytes (0 = not yet polled)
	dbSizeInUse int64 // last observed DbSizeInUse in bytes
}

// New creates a LeaderWatcher with the provided dependencies.
func New(
	pollInterval time.Duration,
	recorder statemachine.Recorder,
	memberName, namespace string,
	api StatusAPI,
	logger *zap.Logger,
) *LeaderWatcher {
	return &LeaderWatcher{
		memberName:      memberName,
		memberNamespace: namespace,
		pollInterval:    pollInterval,
		statusAPI:       api,
		recorder:        recorder,
		logger:          logger,
	}
}

// GetCurrentRole returns the last observed role for this member.
// Returns RoleMember if no poll has completed yet.
func (w *LeaderWatcher) GetCurrentRole() Role {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.currentRole == "" {
		return RoleMember
	}
	return w.currentRole
}

// Run polls etcd status at pollInterval and records leadership changes until ctx is cancelled.
func (w *LeaderWatcher) Run(ctx context.Context, endpoint string) {
	w.endpoint = endpoint
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := w.checkLeadership(ctx)
			if err != nil {
				w.logger.Warn("failed to check leadership status", zap.Error(err))
				metrics.ComponentHealth.WithLabelValues(w.memberNamespace, w.memberName, "leaderwatch").Set(0)
				continue
			}
			isLeader := resp.Header.MemberId == resp.Leader

			metrics.ComponentHealth.WithLabelValues(w.memberNamespace, w.memberName, "leaderwatch").Set(1)

			w.mu.Lock()
			if isLeader {
				w.currentRole = RoleLeader
			} else {
				w.currentRole = RoleMember
			}
			w.dbSize = resp.DbSize
			w.dbSizeInUse = resp.DbSizeInUse
			w.mu.Unlock()

			if w.wasLeader == nil {
				w.wasLeader = &isLeader
				continue
			}

			if isLeader && !*w.wasLeader {
				*w.wasLeader = true
				leaderSubState := statemachine.SubStateLeader
				if err := w.recorder.Record(ctx, w.memberName, w.memberNamespace, statemachine.Transition{
					State:    statemachine.StateStarted,
					SubState: &leaderSubState,
					Reason:   statemachine.ReasonGainedClusterLeadership,
				}); err != nil {
					w.logger.Error("failed to record leadership gain", zap.Error(err))
				}
			} else if !isLeader && *w.wasLeader {
				*w.wasLeader = false
				followerSubState := statemachine.SubStateFollower
				if err := w.recorder.Record(ctx, w.memberName, w.memberNamespace, statemachine.Transition{
					State:    statemachine.StateStarted,
					SubState: &followerSubState,
					Reason:   statemachine.ReasonLostClusterLeadership,
				}); err != nil {
					w.logger.Error("failed to record leadership loss", zap.Error(err))
				}
			}
		}
	}
}

// checkLeadership queries the etcd status endpoint and returns the full status response.
func (w *LeaderWatcher) checkLeadership(ctx context.Context) (*clientv3.StatusResponse, error) {
	resp, err := w.statusAPI.Status(ctx, w.endpoint)
	if err != nil {
		return nil, fmt.Errorf("status call failed: %w", err)
	}
	return resp, nil
}

// ProvideInfo implements member.InfoProvider.
// Returns the latest DB size values observed from etcd status polling.
// Safe to call concurrently with Run.
func (w *LeaderWatcher) ProvideInfo() member.StatusInfo {
	w.mu.RLock()
	dbSize := w.dbSize
	dbSizeInUse := w.dbSizeInUse
	w.mu.RUnlock()

	if dbSize == 0 && dbSizeInUse == 0 {
		return member.StatusInfo{}
	}
	dbSizeQty := resource.NewMilliQuantity(dbSize*1000, resource.BinarySI)
	dbSizeInUseQty := resource.NewMilliQuantity(dbSizeInUse*1000, resource.BinarySI)
	return member.StatusInfo{
		DBSize:      dbSizeQty,
		DBSizeInUse: dbSizeInUseQty,
	}
}
