// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// InfoFunc returns the current member identity information used to build the
// lease HolderIdentity.  The returned memberID is the etcd member's numeric ID,
// and role is one of "Leader", "Member" (follower) or "Learner".
type InfoFunc func(ctx context.Context) (memberID uint64, role string, err error)

// Renewer periodically renews a Kubernetes coordination lease to indicate that
// the etcd member is alive. If the lease does not exist it is created on the
// first renewal attempt.
type Renewer struct {
	podName   string
	namespace string
	leaseName string
	heartbeat time.Duration
	client    kubernetes.Interface
	logger    *zap.Logger
	infoFunc  InfoFunc
}

// NewRenewer creates a Renewer that will maintain the given coordination lease.
// An optional InfoFunc can be supplied via SetInfoFunc to populate the lease's
// HolderIdentity with the etcd member ID and role.
func NewRenewer(
	podName string,
	namespace string,
	leaseName string,
	heartbeat time.Duration,
	client kubernetes.Interface,
	logger *zap.Logger,
) *Renewer {
	return &Renewer{
		podName:   podName,
		namespace: namespace,
		leaseName: leaseName,
		heartbeat: heartbeat,
		client:    client,
		logger:    logger,
	}
}

// SetInfoFunc sets the InfoFunc used to obtain the etcd member ID and role for
// building the lease HolderIdentity in the format expected by etcd-druid
// (<memberID>:<role>).
func (r *Renewer) SetInfoFunc(fn InfoFunc) {
	r.infoFunc = fn
}

// Run starts the renewal loop that renews the lease at the configured heartbeat
// interval. It blocks until the context is cancelled.
func (r *Renewer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.heartbeat)
	defer ticker.Stop()

	// Perform an initial renewal immediately.
	r.renew(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.renew(ctx)
		}
	}
}

// renew performs a single lease renewal. If the lease does not exist it is created.
func (r *Renewer) renew(ctx context.Context) {
	leaseClient := r.client.CoordinationV1().Leases(r.namespace)
	now := metav1.NewMicroTime(time.Now())

	holderIdentity := r.holderIdentity(ctx)

	existing, err := leaseClient.Get(ctx, r.leaseName, metav1.GetOptions{})
	if err != nil {
		// Lease does not exist — create it.
		r.logger.Info("lease not found, creating", zap.String("lease", r.leaseName))
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.leaseName,
				Namespace: r.namespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       strPtr(holderIdentity),
				LeaseDurationSeconds: int32Ptr(leaseDurationSeconds(r.heartbeat)),
				RenewTime:            &now,
			},
		}
		if _, createErr := leaseClient.Create(ctx, lease, metav1.CreateOptions{}); createErr != nil {
			r.logger.Error("failed to create lease", zap.String("lease", r.leaseName), zap.Error(createErr))
			return
		}
		r.logger.Info("lease created", zap.String("lease", r.leaseName))
		return
	}

	// Update the existing lease.
	existing.Spec.HolderIdentity = strPtr(holderIdentity)
	existing.Spec.RenewTime = &now

	if _, err := leaseClient.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		r.logger.Error("failed to renew lease", zap.String("lease", r.leaseName), zap.Error(err))
		return
	}

	r.logger.Info("lease renewed", zap.String("lease", r.leaseName),
		zap.String("holder", holderIdentity),
		zap.String("renewTime", fmt.Sprintf("%v", now.Time)),
	)
}

// holderIdentity returns the lease HolderIdentity string. When an InfoFunc is
// configured and succeeds it returns "<memberID>:<role>", matching the format
// expected by etcd-druid's readyCheck. Otherwise it falls back to the pod name.
func (r *Renewer) holderIdentity(ctx context.Context) string {
	if r.infoFunc == nil {
		return r.podName
	}
	memberID, role, err := r.infoFunc(ctx)
	if err != nil {
		r.logger.Warn("InfoFunc failed, falling back to pod name for holder identity",
			zap.Error(err))
		return r.podName
	}
	return strconv.FormatUint(memberID, 10) + ":" + druidRole(role)
}

// druidRole maps the etcd-steward role string to the value expected by
// etcd-druid (EtcdRoleLeader = "Leader", EtcdRoleMember = "Member").
func druidRole(role string) string {
	switch role {
	case "Leader":
		return "Leader"
	case "Follower":
		return "Member"
	default:
		// "Learner" or any unknown role — pass through so the druid can decide.
		return role
	}
}

func strPtr(s string) *string {
	return &s
}

func int32Ptr(i int32) *int32 {
	return &i
}

// leaseDurationSeconds calculates the lease duration (3x heartbeat) safely,
// clamping to avoid int32 overflow.
func leaseDurationSeconds(heartbeat time.Duration) int32 {
	secs := int64(heartbeat.Seconds()) * 3
	if secs > int64(^int32(0)) {
		secs = int64(^int32(0))
	}
	if secs < 1 {
		secs = 1
	}
	return int32(secs)
}
