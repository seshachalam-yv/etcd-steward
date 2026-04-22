// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

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
}

// NewRenewer creates a Renewer that will maintain the given coordination lease.
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
				HolderIdentity:       strPtr(r.podName),
				LeaseDurationSeconds: int32Ptr(int32(r.heartbeat.Seconds()) * 3),
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
	existing.Spec.HolderIdentity = strPtr(r.podName)
	existing.Spec.RenewTime = &now

	if _, err := leaseClient.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		r.logger.Error("failed to renew lease", zap.String("lease", r.leaseName), zap.Error(err))
		return
	}

	r.logger.Info("lease renewed", zap.String("lease", r.leaseName),
		zap.String("holder", r.podName),
		zap.String("renewTime", fmt.Sprintf("%v", now.Time)),
	)
}

func strPtr(s string) *string {
	return &s
}

func int32Ptr(i int32) *int32 {
	return &i
}
