// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package snapshotlease polls the snapstore and updates Kubernetes Leases with the latest snapshot revisions.
package snapshotlease

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

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// Renewer polls the snapstore and upserts Kubernetes Leases with the latest
// full and delta snapshot revisions.
type Renewer struct {
	etcdName     string
	namespace    string
	store        snapstore.Snapstore
	client       coordinationv1client.LeasesGetter
	pollInterval time.Duration
	logger       *zap.Logger
}

// New creates a Renewer that polls the given snapstore and updates snapshot revision leases.
func New(
	etcdName, namespace string,
	store snapstore.Snapstore,
	client coordinationv1client.LeasesGetter,
	pollInterval time.Duration,
	logger *zap.Logger,
) *Renewer {
	return &Renewer{
		etcdName:     etcdName,
		namespace:    namespace,
		store:        store,
		client:       client,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

// Run polls the snapstore at pollInterval and updates the full-snapshot and delta-snapshot
// leases until ctx is cancelled.
func (r *Renewer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.update(ctx); err != nil {
				r.logger.Error("failed to update snapshot leases", zap.Error(err))
			}
		}
	}
}

// update performs a single poll of the snapstore and upserts the leases.
func (r *Renewer) update(ctx context.Context) error {
	snaps, err := r.store.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(snaps) == 0 {
		return nil
	}

	var latestFull, latestDelta *snapstore.Snapshot
	for i := len(snaps) - 1; i >= 0; i-- {
		if snaps[i].Kind == "Full" && latestFull == nil {
			s := snaps[i]
			latestFull = &s
		}
		if snaps[i].Kind == "Incremental" && latestDelta == nil {
			s := snaps[i]
			latestDelta = &s
		}
		if latestFull != nil && latestDelta != nil {
			break
		}
	}

	if latestFull != nil {
		if err := r.upsertLease(ctx, r.etcdName+"-full-snapshot", latestFull.LastRevision); err != nil {
			r.logger.Error("failed to upsert full-snapshot lease", zap.Error(err))
		}
	}

	if latestDelta != nil {
		if err := r.upsertLease(ctx, r.etcdName+"-delta-snapshot", latestDelta.LastRevision); err != nil {
			r.logger.Error("failed to upsert delta-snapshot lease", zap.Error(err))
		}
	}

	return nil
}

// upsertLease creates or updates a Kubernetes Lease with the given holder identity.
func (r *Renewer) upsertLease(ctx context.Context, leaseName string, revision int64) error {
	holderIdentity := strconv.FormatInt(revision, 10)
	now := metav1.NewMicroTime(time.Now())

	leaseClient := r.client.Leases(r.namespace)

	existing, err := leaseClient.Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get lease %s/%s: %w", r.namespace, leaseName, err)
		}
		// Create the lease.
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      leaseName,
				Namespace: r.namespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &holderIdentity,
				RenewTime:      &now,
			},
		}
		if _, err := leaseClient.Create(ctx, lease, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create lease %s/%s: %w", r.namespace, leaseName, err)
		}
		return nil
	}

	updated := existing.DeepCopy()
	updated.Spec.HolderIdentity = &holderIdentity
	updated.Spec.RenewTime = &now
	if _, err := leaseClient.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update lease %s/%s: %w", r.namespace, leaseName, err)
	}
	return nil
}
