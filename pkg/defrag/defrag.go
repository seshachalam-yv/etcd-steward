// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package defrag implements the Option-3 defragmentation strategy from the etcd-steward design notes.
//
// # Design (Option-3 — preferred)
//
// The leader writes an etcd key /druid/defrag/<namespace>/<name>/<member> with a short TTL
// for each member in the cluster. Each follower watches for its own key and, upon seeing it,
// acquires an etcd lease, defrags itself, and then deletes its key to signal completion.
// The leader waits for all followers to finish (by watching key deletion) before defragging
// itself last. This ensures:
//
//   - No two members defrag simultaneously (serialised via individual keys).
//   - The leader always defrags last (so it can remain the data source while followers rebuild).
//   - The operation is tolerant to pod restarts (keys expire via TTL if a member crashes mid-defrag).
//
// # Usage
//
//	orchestrator := defrag.NewOrchestrator(etcdClient, maintenance, namespace, name, endpoint, logger)
//	participant := defrag.NewParticipant(etcdClient, maintenance, namespace, name, memberName, endpoint, logger)
//
//	// On leader:
//	go orchestrator.Run(ctx, memberNames, period)
//
//	// On every member (including leader):
//	go participant.Run(ctx)
package defrag

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/metrics"
)

const (
	// defragKeyPrefix is the etcd key namespace for defrag coordination.
	defragKeyPrefix = "/druid/defrag/"
	// defragKeyTTL is the TTL (seconds) for defrag coordination keys.
	// A key that outlives its TTL means the assigned member crashed mid-defrag.
	defragKeyTTL = 120
)

// MaintenanceAPI is the subset of etcd Maintenance needed for defragmentation.
type MaintenanceAPI interface {
	Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error)
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
}

// memberKey returns the etcd key for a specific member's defrag token.
func memberKey(namespace, clusterName, memberName string) string {
	return defragKeyPrefix + namespace + "/" + clusterName + "/" + memberName
}

// clusterPrefix returns the watch prefix for all members of a cluster.
func clusterPrefix(namespace, clusterName string) string {
	return defragKeyPrefix + namespace + "/" + clusterName + "/"
}

// --------------------------------------------------------------------------
// Orchestrator — runs on the leader
// --------------------------------------------------------------------------

// Orchestrator writes defrag tokens for all followers, waits for acknowledgement,
// then defrags the leader itself.
type Orchestrator struct {
	client      *clientv3.Client
	maintenance MaintenanceAPI
	namespace   string
	clusterName string
	endpoint    string
	logger      *zap.Logger

	lastDefrag *member.DefragInfo
}

// NewOrchestrator creates an Orchestrator for the cluster leader.
func NewOrchestrator(
	client *clientv3.Client,
	maintenance MaintenanceAPI,
	namespace, clusterName, endpoint string,
	logger *zap.Logger,
) *Orchestrator {
	return &Orchestrator{
		client:      client,
		maintenance: maintenance,
		namespace:   namespace,
		clusterName: clusterName,
		endpoint:    endpoint,
		logger:      logger,
	}
}

// Run executes the leader-side defrag orchestration loop on the given period.
// getMemberNames is called each tick to discover the current cluster member names.
// The leader name should match leaderName so the leader defrags last.
func (o *Orchestrator) Run(ctx context.Context, leaderName string, getMemberNames func(ctx context.Context) ([]string, error), period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			members, err := getMemberNames(ctx)
			if err != nil {
				o.logger.Error("failed to list cluster members for defrag", zap.Error(err))
				continue
			}
			if err := o.orchestrate(ctx, leaderName, members); err != nil {
				o.logger.Error("defrag orchestration failed", zap.Error(err))
			}
		}
	}
}

// orchestrate triggers defragmentation for all members, waiting for followers to finish
// before defragmenting the leader itself.
func (o *Orchestrator) orchestrate(ctx context.Context, leaderName string, memberNames []string) error {
	o.logger.Info("starting coordinated defragmentation",
		zap.Strings("members", memberNames),
		zap.String("leader", leaderName),
	)

	// Write a short-TTL token key for each non-leader member.
	followers := make([]string, 0, len(memberNames)-1)
	for _, m := range memberNames {
		if m == leaderName {
			continue
		}
		followers = append(followers, m)

		lease, err := o.client.Grant(ctx, defragKeyTTL)
		if err != nil {
			return fmt.Errorf("failed to grant lease for defrag token %s: %w", m, err)
		}

		key := memberKey(o.namespace, o.clusterName, m)
		_, err = o.client.Put(ctx, key, "defrag", clientv3.WithLease(lease.ID))
		if err != nil {
			return fmt.Errorf("failed to write defrag token for %s: %w", m, err)
		}
	}

	// Wait for all follower keys to be deleted (acknowledgement of completion).
	if err := o.waitForFollowers(ctx, followers); err != nil {
		return fmt.Errorf("timed out waiting for followers to complete defrag: %w", err)
	}

	// All followers done — defrag the leader last.
	return o.defragSelf(ctx, leaderName, "Scheduled")
}

// waitForFollowers blocks until all follower defrag keys are deleted or ctx expires.
func (o *Orchestrator) waitForFollowers(ctx context.Context, followers []string) error {
	if len(followers) == 0 {
		return nil
	}

	prefix := clusterPrefix(o.namespace, o.clusterName)
	watchCtx, watchCancel := context.WithTimeout(ctx, time.Duration(defragKeyTTL)*time.Second*2)
	defer watchCancel()

	wch := o.client.Watch(watchCtx, prefix, clientv3.WithPrefix())

	remaining := make(map[string]bool)
	for _, f := range followers {
		remaining[memberKey(o.namespace, o.clusterName, f)] = true
	}

	for watchResp := range wch {
		for _, ev := range watchResp.Events {
			if ev.Type == clientv3.EventTypeDelete {
				delete(remaining, string(ev.Kv.Key))
			}
		}
		if len(remaining) == 0 {
			return nil
		}
	}

	if len(remaining) > 0 {
		return fmt.Errorf("%d follower(s) did not complete defrag in time", len(remaining))
	}
	return nil
}

// defragSelf defragments the local endpoint and records metrics.
func (o *Orchestrator) defragSelf(ctx context.Context, memberName, reason string) error {
	statusBefore, err := o.maintenance.Status(ctx, o.endpoint)
	if err != nil {
		return fmt.Errorf("failed to get status before defrag for leader %s: %w", memberName, err)
	}
	initialDBBytes := statusBefore.DbSize

	defragStart := metav1.Now()
	start := time.Now()
	o.logger.Info("defragmenting leader", zap.String("endpoint", o.endpoint))
	_, defragErr := o.maintenance.Defragment(ctx, o.endpoint)
	duration := time.Since(start).Seconds()

	statusCode := "success"
	if defragErr != nil {
		statusCode = "failure"
	}
	metrics.DefragmentationDurationSeconds.WithLabelValues(o.namespace, o.clusterName, statusCode, reason).Observe(duration)

	defragEnd := metav1.Now()
	var finalDBBytes int64
	if post, err := o.maintenance.Status(ctx, o.endpoint); err == nil {
		finalDBBytes = post.DbSize
	}

	initialQty := resource.NewMilliQuantity(initialDBBytes*1000, resource.BinarySI)
	finalQty := resource.NewMilliQuantity(finalDBBytes*1000, resource.BinarySI)
	reasonCopy := reason
	defragInfo := &member.DefragInfo{
		StartTime:     defragStart,
		EndTime:       &defragEnd,
		InitialDBSize: initialQty,
		FinalDBSize:   finalQty,
		Reason:        &reasonCopy,
	}
	if defragErr != nil {
		msg := defragErr.Error()
		defragInfo.Message = &msg
	}
	o.lastDefrag = defragInfo

	return defragErr
}

// ProvideInfo implements member.InfoProvider for the leader's defrag status.
func (o *Orchestrator) ProvideInfo() member.StatusInfo {
	if o.lastDefrag == nil {
		return member.StatusInfo{}
	}
	snapshot := *o.lastDefrag
	return member.StatusInfo{LastDefragmentation: &snapshot}
}

// --------------------------------------------------------------------------
// Participant — runs on every member (follower)
// --------------------------------------------------------------------------

// Participant watches for a defrag token key and performs defragmentation when signalled.
type Participant struct {
	client      *clientv3.Client
	maintenance MaintenanceAPI
	namespace   string
	clusterName string
	memberName  string
	endpoint    string
	logger      *zap.Logger

	lastDefrag *member.DefragInfo
}

// NewParticipant creates a Participant for the given member.
func NewParticipant(
	client *clientv3.Client,
	maintenance MaintenanceAPI,
	namespace, clusterName, memberName, endpoint string,
	logger *zap.Logger,
) *Participant {
	return &Participant{
		client:      client,
		maintenance: maintenance,
		namespace:   namespace,
		clusterName: clusterName,
		memberName:  memberName,
		endpoint:    endpoint,
		logger:      logger,
	}
}

// Run watches for a defrag token key and defrags when signalled.
// Blocks until ctx is cancelled.
func (p *Participant) Run(ctx context.Context) {
	key := memberKey(p.namespace, p.clusterName, p.memberName)
	p.logger.Info("participant defrag watcher started",
		zap.String("key", key),
	)

	wch := p.client.Watch(ctx, key)
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-wch:
			if !ok {
				// Channel closed — re-establish watch.
				wch = p.client.Watch(ctx, key)
				continue
			}
			for _, ev := range resp.Events {
				if ev.Type == clientv3.EventTypePut {
					p.logger.Info("received defrag signal", zap.String("member", p.memberName))
					if err := p.defragAndAck(ctx, key); err != nil {
						p.logger.Error("defrag failed", zap.String("member", p.memberName), zap.Error(err))
					}
				}
			}
		}
	}
}

// defragAndAck performs defragmentation and deletes the token key to signal completion.
func (p *Participant) defragAndAck(ctx context.Context, key string) error {
	statusBefore, err := p.maintenance.Status(ctx, p.endpoint)
	if err != nil {
		return fmt.Errorf("failed to get status before defrag for %s: %w", p.memberName, err)
	}
	initialDBBytes := statusBefore.DbSize

	defragStart := metav1.Now()
	start := time.Now()
	p.logger.Info("defragmenting follower", zap.String("endpoint", p.endpoint))
	_, defragErr := p.maintenance.Defragment(ctx, p.endpoint)
	duration := time.Since(start).Seconds()

	statusCode := "success"
	if defragErr != nil {
		statusCode = "failure"
	}
	metrics.DefragmentationDurationSeconds.WithLabelValues(p.namespace, p.clusterName, statusCode, "Scheduled").Observe(duration)

	defragEnd := metav1.Now()
	var finalDBBytes int64
	if post, err := p.maintenance.Status(ctx, p.endpoint); err == nil {
		finalDBBytes = post.DbSize
	}

	initialQty := resource.NewMilliQuantity(initialDBBytes*1000, resource.BinarySI)
	finalQty := resource.NewMilliQuantity(finalDBBytes*1000, resource.BinarySI)
	reason := "Scheduled"
	defragInfo := &member.DefragInfo{
		StartTime:     defragStart,
		EndTime:       &defragEnd,
		InitialDBSize: initialQty,
		FinalDBSize:   finalQty,
		Reason:        &reason,
	}
	if defragErr != nil {
		msg := defragErr.Error()
		defragInfo.Message = &msg
	}
	p.lastDefrag = defragInfo

	// Delete the token key regardless of defrag success, so the leader unblocks.
	delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer delCancel()
	if _, delErr := p.client.Delete(delCtx, key); delErr != nil {
		p.logger.Error("failed to delete defrag token key",
			zap.String("key", key),
			zap.Error(delErr),
		)
	}

	return defragErr
}

// ProvideInfo implements member.InfoProvider for the follower's defrag status.
func (p *Participant) ProvideInfo() member.StatusInfo {
	if p.lastDefrag == nil {
		return member.StatusInfo{}
	}
	snapshot := *p.lastDefrag
	return member.StatusInfo{LastDefragmentation: &snapshot}
}
