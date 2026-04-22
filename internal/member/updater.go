// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"sync"
	"time"

	"github.com/gardener/etcd-steward/internal/errors"
	"go.uber.org/zap"
)

// MemberInfoProvider supplies the current member information for the updater.
// Implementations typically query etcd maintenance.Status to obtain details.
type MemberInfoProvider interface {
	// MemberInfo returns the current member information.
	MemberInfo(ctx context.Context) (MemberInfo, error)
}

// MemberInfo holds the current state of a member obtained from etcd maintenance
// status.
type MemberInfo struct {
	// ID is the unique member ID within the etcd cluster.
	ID uint64
	// Name is the human-readable name of the member (typically the pod name).
	Name string
	// Role is the current role (Leader, Follower, Learner).
	Role string
	// DBSize is the size of the etcd database in bytes.
	DBSize int64
	// DBSizeInUse is the in-use size of the etcd database in bytes.
	DBSizeInUse int64
	// IsHealthy indicates whether the member is considered healthy.
	IsHealthy bool
	// SupplementaryInfo holds additional data collected from supplementary info
	// providers, keyed by provider ID.
	SupplementaryInfo map[string]map[string]interface{}
}

// SupplementaryInfoProvider supplies additional data to be included in the
// EtcdMember status patch alongside the core member info.
type SupplementaryInfoProvider interface {
	// ID returns a unique identifier for this provider.
	ID() string
	// GetInfo returns additional data to include in the member status.
	GetInfo(ctx context.Context) (map[string]interface{}, error)
}

// StateRecorder records member state transitions to an external system (e.g.,
// the EtcdMember custom resource).
type StateRecorder interface {
	// RecordMemberState records the given member info for the named member.
	RecordMemberState(ctx context.Context, memberName, namespace string, info MemberInfo) error
}

// Updater synchronously records state transitions and asynchronously collects
// member info via registered providers and publishes updates.
type Updater struct {
	podName   string
	namespace string
	interval  time.Duration
	recorder  StateRecorder
	logger    *zap.Logger

	mu                      sync.RWMutex
	providers               []MemberInfoProvider
	supplementaryProviders  []SupplementaryInfoProvider
	lastInfo                *MemberInfo
}

// NewUpdater creates an Updater.
func NewUpdater(
	podName string,
	namespace string,
	interval time.Duration,
	recorder StateRecorder,
	logger *zap.Logger,
) *Updater {
	return &Updater{
		podName:   podName,
		namespace: namespace,
		interval:  interval,
		recorder:  recorder,
		logger:    logger,
	}
}

// RegisterInfoProvider adds a MemberInfoProvider to the updater. Providers are
// queried in the order they are registered; the first successful result is used.
func (u *Updater) RegisterInfoProvider(p MemberInfoProvider) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.providers = append(u.providers, p)
}

// RegisterSupplementaryProvider adds a SupplementaryInfoProvider to the updater.
// Supplementary providers are queried after the core member info is collected
// and their results are merged into MemberInfo.SupplementaryInfo.
func (u *Updater) RegisterSupplementaryProvider(p SupplementaryInfoProvider) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.supplementaryProviders = append(u.supplementaryProviders, p)
}

// RecordStateTransition synchronously records a state transition by collecting
// info from registered providers and writing it to the recorder.
func (u *Updater) RecordStateTransition(ctx context.Context) error {
	info, err := u.collectInfo(ctx)
	if err != nil {
		return errors.Wrap(errors.ErrCodeInternal, "failed to collect member info", err)
	}

	u.mu.Lock()
	u.lastInfo = &info
	u.mu.Unlock()

	if err := u.recorder.RecordMemberState(ctx, u.podName, u.namespace, info); err != nil {
		return errors.Wrap(errors.ErrCodeNetwork, "failed to record member state", err)
	}

	u.logger.Info("member state recorded",
		zap.Uint64("id", info.ID),
		zap.String("role", info.Role),
		zap.Int64("dbSize", info.DBSize),
	)
	return nil
}

// Run starts the async update loop that periodically collects member info and
// records it. It blocks until the context is cancelled.
func (u *Updater) Run(ctx context.Context) {
	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := u.RecordStateTransition(ctx); err != nil {
				u.logger.Error("periodic member update failed", zap.Error(err))
			}
		}
	}
}

// LastInfo returns the last collected member info, or nil if no info has been
// collected yet.
func (u *Updater) LastInfo() *MemberInfo {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.lastInfo == nil {
		return nil
	}
	cp := *u.lastInfo
	return &cp
}

// collectInfo queries registered providers for member info. The first provider
// that returns successfully is used. Supplementary providers are then queried
// and their results are merged into MemberInfo.SupplementaryInfo.
func (u *Updater) collectInfo(ctx context.Context) (MemberInfo, error) {
	u.mu.RLock()
	providers := make([]MemberInfoProvider, len(u.providers))
	copy(providers, u.providers)
	suppProviders := make([]SupplementaryInfoProvider, len(u.supplementaryProviders))
	copy(suppProviders, u.supplementaryProviders)
	u.mu.RUnlock()

	if len(providers) == 0 {
		return MemberInfo{}, errors.New(errors.ErrCodeInternal, "no info providers registered")
	}

	var lastErr error
	var info MemberInfo
	found := false
	for _, p := range providers {
		var err error
		info, err = p.MemberInfo(ctx)
		if err != nil {
			lastErr = err
			u.logger.Error("info provider failed", zap.Error(err))
			continue
		}
		found = true
		break
	}

	if !found {
		return MemberInfo{}, errors.Wrap(errors.ErrCodeInternal, "all info providers failed", lastErr)
	}

	// Collect supplementary info from all registered supplementary providers.
	if len(suppProviders) > 0 {
		info.SupplementaryInfo = make(map[string]map[string]interface{}, len(suppProviders))
		for _, sp := range suppProviders {
			data, err := sp.GetInfo(ctx)
			if err != nil {
				u.logger.Error("supplementary info provider failed",
					zap.String("providerID", sp.ID()),
					zap.Error(err),
				)
				continue
			}
			info.SupplementaryInfo[sp.ID()] = data
		}
	}

	return info, nil
}
