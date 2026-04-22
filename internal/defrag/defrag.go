// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package defrag

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"
)

// DistributedLock provides a distributed locking mechanism to ensure that only
// one member defrags at a time.
type DistributedLock interface {
	// Acquire blocks until the lock is held or the context is cancelled.
	Acquire(ctx context.Context) error
	// Release releases the lock.
	Release(ctx context.Context) error
}

// RoleProvider exposes the current leadership role of this member.
type RoleProvider interface {
	// IsLeader returns true if the local member is the current leader.
	IsLeader() bool
}

// MaintenanceClient defines the etcd maintenance operations needed for defrag.
type MaintenanceClient interface {
	// Defragment defragments the storage of the member at the given endpoint.
	Defragment(ctx context.Context, endpoint string) error
	// Status returns the DB size for the member at the given endpoint.
	Status(ctx context.Context, endpoint string) (dbSize int64, err error)
}

// KVClient defines the etcd KV operations needed for defrag status tracking.
type KVClient interface {
	// Put sets the value for the given key.
	Put(ctx context.Context, key, val string) error
	// Get retrieves the value for the given key. Returns "" if not found.
	Get(ctx context.Context, key string) (string, error)
}

// ClusterClient defines the operations needed to discover cluster members.
type ClusterClient interface {
	// MemberEndpoints returns the client endpoints for all members in the cluster.
	MemberEndpoints(ctx context.Context) ([]string, error)
}

// Defragmenter coordinates distributed defragmentation across etcd cluster
// members. The leader initiates defrag, followers defrag first, leader last.
type Defragmenter struct {
	podName     string
	interval    time.Duration
	lock        DistributedLock
	role        RoleProvider
	maintenance MaintenanceClient
	kv          KVClient
	cluster     ClusterClient
	logger      *zap.Logger
}

// New creates a Defragmenter with the given dependencies.
func New(
	podName string,
	interval time.Duration,
	lock DistributedLock,
	role RoleProvider,
	maintenance MaintenanceClient,
	kv KVClient,
	cluster ClusterClient,
	logger *zap.Logger,
) *Defragmenter {
	return &Defragmenter{
		podName:     podName,
		interval:    interval,
		lock:        lock,
		role:        role,
		maintenance: maintenance,
		kv:          kv,
		cluster:     cluster,
		logger:      logger,
	}
}

// statusKeyPrefix is the etcd key prefix used to track per-member defrag status.
const statusKeyPrefix = "/steward/defrag/status/"

// Run starts the defragmentation loop that triggers at the configured interval.
// It blocks until the context is cancelled.
func (d *Defragmenter) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !d.role.IsLeader() {
				d.logger.Info("skipping defrag cycle, not leader")
				continue
			}
			if err := d.Defragment(ctx); err != nil {
				d.logger.Error("defrag cycle failed", zap.Error(err))
			}
		}
	}
}

// Defragment performs a coordinated defrag across all cluster members. The
// leader writes status keys, each member acquires the distributed lock,
// followers are defragged first, and the leader is defragged last.
func (d *Defragmenter) Defragment(ctx context.Context) error {
	endpoints, err := d.cluster.MemberEndpoints(ctx)
	if err != nil {
		return fmt.Errorf("failed to list member endpoints: %w", err)
	}

	if len(endpoints) == 0 {
		return fmt.Errorf("no member endpoints found")
	}

	// Write initial status for all members.
	for _, ep := range endpoints {
		key := statusKeyPrefix + ep
		if err := d.kv.Put(ctx, key, "pending"); err != nil {
			return fmt.Errorf("failed to write defrag status for %s: %w", ep, err)
		}
	}

	// Sort endpoints so that the local endpoint (leader) is last.
	sorted := sortEndpoints(endpoints, d.podName)

	for _, ep := range sorted {
		if err := d.defragMember(ctx, ep); err != nil {
			d.logger.Error("defrag failed for member", zap.String("endpoint", ep), zap.Error(err))
			// Mark as failed but continue with remaining members.
			_ = d.kv.Put(ctx, statusKeyPrefix+ep, "failed")
			continue
		}
		if err := d.kv.Put(ctx, statusKeyPrefix+ep, "completed"); err != nil {
			d.logger.Error("failed to update defrag status", zap.String("endpoint", ep), zap.Error(err))
		}
	}

	d.logger.Info("defrag cycle completed", zap.Int("members", len(sorted)))
	return nil
}

// DefragDirect defragments a single endpoint directly without acquiring the
// distributed lock. This is intended for NOSPACE emergency bypass scenarios.
func (d *Defragmenter) DefragDirect(ctx context.Context, endpoint string) error {
	d.logger.Info("direct defrag (NOSPACE bypass)", zap.String("endpoint", endpoint))

	if err := d.maintenance.Defragment(ctx, endpoint); err != nil {
		return fmt.Errorf("direct defrag failed for %s: %w", endpoint, err)
	}

	d.logger.Info("direct defrag completed", zap.String("endpoint", endpoint))
	return nil
}

// defragMember acquires the distributed lock, performs defrag on the given
// endpoint, and releases the lock.
func (d *Defragmenter) defragMember(ctx context.Context, endpoint string) error {
	if err := d.lock.Acquire(ctx); err != nil {
		return fmt.Errorf("failed to acquire lock for defrag of %s: %w", endpoint, err)
	}
	defer func() {
		if err := d.lock.Release(ctx); err != nil {
			d.logger.Error("failed to release defrag lock", zap.String("endpoint", endpoint), zap.Error(err))
		}
	}()

	d.logger.Info("defragmenting member", zap.String("endpoint", endpoint))

	if err := d.maintenance.Defragment(ctx, endpoint); err != nil {
		return fmt.Errorf("defragment failed for %s: %w", endpoint, err)
	}

	d.logger.Info("defrag completed for member", zap.String("endpoint", endpoint))
	return nil
}

// sortEndpoints returns a copy of endpoints sorted so that any endpoint
// containing localPodName appears last. This ensures followers are defragged
// before the leader.
func sortEndpoints(endpoints []string, localPodName string) []string {
	sorted := make([]string, len(endpoints))
	copy(sorted, endpoints)

	sort.SliceStable(sorted, func(i, j int) bool {
		iLocal := isLocalEndpoint(sorted[i], localPodName)
		jLocal := isLocalEndpoint(sorted[j], localPodName)
		if iLocal != jLocal {
			return !iLocal // local goes to the end
		}
		return sorted[i] < sorted[j]
	})

	return sorted
}

// isLocalEndpoint checks whether the endpoint string contains the local pod name.
func isLocalEndpoint(endpoint, localPodName string) bool {
	return len(localPodName) > 0 && contains(endpoint, localPodName)
}

// contains checks if s contains substr (simple string containment).
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

// searchString returns true if substr is found within s.
func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
