// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package snapshotlease updates the Kubernetes snapshot leases that etcd-druid uses
// to track snapshot revision progress and drive compaction decisions.
//
// etcd-druid reads two Leases per Etcd cluster:
//   - <etcd-name>-full-snap  — HolderIdentity = last full snapshot revision (decimal string)
//   - <etcd-name>-delta-snap — HolderIdentity = last delta snapshot revision (decimal string)
//
// The compaction controller checks whether HolderIdentity is set and compares the revision
// delta to decide when to run compaction jobs.
package snapshotlease

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

// Updater updates the full and delta snapshot leases after each snapshot is taken.
type Updater struct {
	etcdName  string
	namespace string
	client    coordinationv1client.LeasesGetter
	logger    *zap.Logger
}

// New creates an Updater.
func New(etcdName, namespace string, client coordinationv1client.LeasesGetter, logger *zap.Logger) *Updater {
	return &Updater{
		etcdName:  etcdName,
		namespace: namespace,
		client:    client,
		logger:    logger,
	}
}

// UpdateFullSnapshotLease sets the full snapshot lease HolderIdentity to the given revision (decimal).
// This is called after each full snapshot is saved.
func (u *Updater) UpdateFullSnapshotLease(ctx context.Context, revision int64) error {
	leaseName := u.etcdName + "-full-snap"
	return u.updateLease(ctx, leaseName, strconv.FormatInt(revision, 10))
}

// UpdateDeltaSnapshotLease sets the delta snapshot lease HolderIdentity to the given revision (decimal).
// This is called after each delta snapshot is saved.
func (u *Updater) UpdateDeltaSnapshotLease(ctx context.Context, revision int64) error {
	leaseName := u.etcdName + "-delta-snap"
	return u.updateLease(ctx, leaseName, strconv.FormatInt(revision, 10))
}

// updateLease creates or updates a Lease, setting HolderIdentity to holderIdentity and
// RenewTime to now, only if the new holderIdentity value is greater than the existing one.
func (u *Updater) updateLease(ctx context.Context, leaseName, holderIdentity string) error {
	leaseClient := u.client.Leases(u.namespace)

	existing, err := leaseClient.Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get snapshot lease %s/%s: %w", u.namespace, leaseName, err)
		}
		return u.createLease(ctx, leaseClient, leaseName, holderIdentity)
	}

	// Only advance the holder identity — never regress.
	if existing.Spec.HolderIdentity != nil {
		existingRev, err := strconv.ParseInt(*existing.Spec.HolderIdentity, 10, 64)
		newRev, newErr := strconv.ParseInt(holderIdentity, 10, 64)
		if err == nil && newErr == nil && newRev <= existingRev {
			return nil // already at or ahead of this revision
		}
	}

	updated := existing.DeepCopy()
	updated.Spec.HolderIdentity = &holderIdentity
	now := metav1.NewMicroTime(time.Now())
	updated.Spec.RenewTime = &now

	if _, err := leaseClient.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update snapshot lease %s/%s: %w", u.namespace, leaseName, err)
	}
	u.logger.Info("updated snapshot lease",
		zap.String("lease", leaseName),
		zap.String("holderIdentity", holderIdentity),
	)
	return nil
}

// createLease creates the snapshot lease for the first time.
func (u *Updater) createLease(ctx context.Context, leaseClient coordinationv1client.LeaseInterface, leaseName, holderIdentity string) error {
	now := metav1.NewMicroTime(time.Now())
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: u.namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holderIdentity,
			RenewTime:      &now,
		},
	}
	if _, err := leaseClient.Create(ctx, lease, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create snapshot lease %s/%s: %w", u.namespace, leaseName, err)
	}
	u.logger.Info("created snapshot lease",
		zap.String("lease", leaseName),
		zap.String("holderIdentity", holderIdentity),
	)
	return nil
}
