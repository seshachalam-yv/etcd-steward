// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package lease provides periodic renewal of Kubernetes Lease resources for etcd members.
package lease

import (
	"context"
	"fmt"
	"strconv"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"go.uber.org/zap"
)

// LeaseAnnotationKeyPeerURLTLSEnabled is the annotation key etcd-druid reads from member leases
// to gate StatefulSet updates during TLS enablement/disablement.
const LeaseAnnotationKeyPeerURLTLSEnabled = "member.etcd.gardener.cloud/tls-enabled"

// StateFunc returns the current member ID, cluster ID, and role for the lease holder identity.
type StateFunc func() (memberID, clusterID, role string)

// Renewer periodically updates the K8s Lease for this etcd member.
type Renewer struct {
	leaseName         string
	namespace         string
	holderIdentity    string
	heartbeatInterval time.Duration
	leaseDuration     int32
	client            coordinationv1client.LeasesGetter
	stateFunc         StateFunc
	peerTLSEnabled    bool
	logger            *zap.Logger

	// cachedMemberID and cachedClusterID hold the last successfully obtained
	// member and cluster IDs. They are used to avoid overwriting a valid lease
	// identity with empty values when etcd is temporarily unreachable.
	cachedMemberID  string
	cachedClusterID string
}

// New creates a new Renewer.
func New(
	leaseName, namespace string,
	holderIdentity string,
	heartbeatInterval time.Duration,
	client coordinationv1client.LeasesGetter,
	stateFunc StateFunc,
	peerTLSEnabled bool,
	logger *zap.Logger,
) *Renewer {
	return &Renewer{
		leaseName:         leaseName,
		namespace:         namespace,
		holderIdentity:    holderIdentity,
		heartbeatInterval: heartbeatInterval,
		leaseDuration:     int32(heartbeatInterval.Seconds()) * 3,
		client:            client,
		stateFunc:         stateFunc,
		peerTLSEnabled:    peerTLSEnabled,
		logger:            logger,
	}
}

// Run calls renew() immediately, then on tick until ctx is done.
func (r *Renewer) Run(ctx context.Context) {
	if err := r.renew(ctx); err != nil {
		r.logger.Error("Failed initial lease renewal", zap.String("lease", r.leaseName), zap.Error(err))
	}

	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.renew(ctx); err != nil {
				r.logger.Error("Failed to renew lease", zap.String("lease", r.leaseName), zap.Error(err))
			}
		}
	}
}

// renew performs a single lease renewal, creating the lease if it does not exist.
func (r *Renewer) renew(ctx context.Context) error {
	memberID, clusterID, role := r.stateFunc()

	// Cache the member and cluster IDs once we obtain valid (non-empty) values.
	// This prevents overwriting a previously valid lease identity with empty strings
	// when etcd is temporarily unreachable between heartbeat ticks.
	if memberID != "" {
		r.cachedMemberID = memberID
	}
	if clusterID != "" {
		r.cachedClusterID = clusterID
	}
	effectiveMemberID := r.cachedMemberID
	effectiveClusterID := r.cachedClusterID

	holderIdentity := fmt.Sprintf("%s:%s:%s", effectiveMemberID, effectiveClusterID, role)

	now := metav1.NewMicroTime(time.Now())
	leaseClient := r.client.Leases(r.namespace)

	existing, err := leaseClient.Get(ctx, r.leaseName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get lease %s/%s: %w", r.namespace, r.leaseName, err)
		}
		return r.createLease(ctx, leaseClient, holderIdentity, now)
	}

	updated := existing.DeepCopy()
	updated.Spec.HolderIdentity = &holderIdentity
	updated.Spec.RenewTime = &now
	leaseDuration := r.leaseDuration
	updated.Spec.LeaseDurationSeconds = &leaseDuration
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[LeaseAnnotationKeyPeerURLTLSEnabled] = strconv.FormatBool(r.peerTLSEnabled)

	if _, err := leaseClient.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update lease %s/%s: %w", r.namespace, r.leaseName, err)
	}
	return nil
}

// createLease creates a new K8s Lease resource.
func (r *Renewer) createLease(ctx context.Context, leaseClient coordinationv1client.LeaseInterface, holderIdentity string, now metav1.MicroTime) error {
	leaseDuration := r.leaseDuration
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.leaseName,
			Namespace: r.namespace,
			Annotations: map[string]string{
				LeaseAnnotationKeyPeerURLTLSEnabled: strconv.FormatBool(r.peerTLSEnabled),
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holderIdentity,
			LeaseDurationSeconds: &leaseDuration,
			RenewTime:            &now,
		},
	}
	if _, err := leaseClient.Create(ctx, lease, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create lease %s/%s: %w", r.namespace, r.leaseName, err)
	}
	return nil
}
