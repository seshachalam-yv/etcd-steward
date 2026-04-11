// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package etcdclient provides a wrapper around the etcd client for cluster membership operations.
package etcdclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Member holds basic information about an etcd cluster member.
type Member struct {
	// ID is the etcd member ID.
	ID uint64
	// Name is the member name.
	Name string
	// PeerURLs are the peer communication URLs.
	PeerURLs []string
	// IsLearner is true if this member is a non-voting learner.
	IsLearner bool
}

// ClusterClient provides membership control operations on an etcd cluster.
type ClusterClient interface {
	// AddLearner adds a member with the given peer URL as a non-voting learner.
	// Retries up to 6 times with exponential backoff starting at 500ms.
	// Returns the assigned member ID on success.
	AddLearner(ctx context.Context, peerURL string) (uint64, error)
	// PromoteMember promotes the learner with the given member ID to a full voting member.
	PromoteMember(ctx context.Context, memberID uint64) error
	// RemoveMember removes the member with the given ID from the cluster.
	RemoveMember(ctx context.Context, memberID uint64) error
	// ListMembers returns all current members of the cluster.
	ListMembers(ctx context.Context) ([]Member, error)
	// WasMemberInCluster returns true if a member with the given peerURL exists in the cluster.
	WasMemberInCluster(ctx context.Context, peerURL string) (bool, error)
	// RemoveStaleMember removes any member whose PeerURLs contain peerURL.
	// Returns nil if no matching member is found (idempotent).
	RemoveStaleMember(ctx context.Context, peerURL string) error
	// UpdateMemberPeerURL updates the peer URL of the member with the given ID.
	UpdateMemberPeerURL(ctx context.Context, memberID uint64, peerURL string) error
}

// EtcdClusterClient implements ClusterClient using the etcd client v3 Cluster interface.
type EtcdClusterClient struct {
	cluster           clientv3.Cluster
	initialBackoff    time.Duration
	maxRetries        int
	perAttemptTimeout time.Duration
}

// NewClusterClient creates a ClusterClient backed by the provided etcd client.
func NewClusterClient(client *clientv3.Client) ClusterClient {
	return &EtcdClusterClient{
		cluster:           client.Cluster,
		initialBackoff:    500 * time.Millisecond,
		maxRetries:        20,
		perAttemptTimeout: 30 * time.Second,
	}
}

// AddLearner adds a member with the given peer URL as a non-voting learner.
// It retries up to maxRetries times with exponential backoff starting at initialBackoff.
// For transient "too many learner members" errors (another member is being promoted)
// a fixed short backoff is used instead of exponential to recover quickly.
// If the peer URL already exists, returns the existing member's ID (idempotent).
func (c *EtcdClusterClient) AddLearner(ctx context.Context, peerURL string) (uint64, error) {
	var lastErr error
	backoff := c.initialBackoff
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		if attempt > 0 {
			wait := backoff
			// "too many learner members" is transient: another member is being promoted.
			// Use a fixed short wait instead of exponential so we recover quickly.
			if lastErr != nil && strings.Contains(lastErr.Error(), "too many learner members") {
				wait = 5 * time.Second
			} else {
				backoff *= 2
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(wait):
			}
		}

		attemptCtx, cancel := context.WithTimeout(ctx, c.perAttemptTimeout)
		resp, err := c.cluster.MemberAddAsLearner(attemptCtx, []string{peerURL})
		cancel()

		if err == nil {
			return resp.Member.ID, nil
		}

		// Treat "peer URLs already exists" as success (idempotent).
		if strings.Contains(err.Error(), "Peer URLs already exists") || strings.Contains(err.Error(), "peerURL exists") {
			if memberID, findErr := c.findMemberByPeerURL(ctx, peerURL); findErr == nil && memberID != 0 {
				return memberID, nil
			}
		}
		lastErr = err
	}
	return 0, fmt.Errorf("failed to add learner after %d attempts: %w", c.maxRetries, lastErr)
}

// findMemberByPeerURL returns the member ID of the member with the given peer URL, or 0 if not found.
func (c *EtcdClusterClient) findMemberByPeerURL(ctx context.Context, peerURL string) (uint64, error) {
	resp, err := c.cluster.MemberList(ctx)
	if err != nil {
		return 0, err
	}
	for _, m := range resp.Members {
		for _, u := range m.PeerURLs {
			if u == peerURL {
				return m.ID, nil
			}
		}
	}
	return 0, nil
}

// PromoteMember promotes the learner with the given member ID to a full voting member.
func (c *EtcdClusterClient) PromoteMember(ctx context.Context, memberID uint64) error {
	_, err := c.cluster.MemberPromote(ctx, memberID)
	if err != nil {
		return fmt.Errorf("failed to promote member %x: %w", memberID, err)
	}
	return nil
}

// RemoveMember removes the member with the given ID from the cluster.
func (c *EtcdClusterClient) RemoveMember(ctx context.Context, memberID uint64) error {
	_, err := c.cluster.MemberRemove(ctx, memberID)
	if err != nil {
		return fmt.Errorf("failed to remove member %x: %w", memberID, err)
	}
	return nil
}

// ListMembers returns all current members of the cluster.
func (c *EtcdClusterClient) ListMembers(ctx context.Context) ([]Member, error) {
	resp, err := c.cluster.MemberList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list members: %w", err)
	}
	members := make([]Member, 0, len(resp.Members))
	for _, m := range resp.Members {
		members = append(members, Member{
			ID:        m.ID,
			Name:      m.Name,
			PeerURLs:  m.PeerURLs,
			IsLearner: m.IsLearner,
		})
	}
	return members, nil
}

// WasMemberInCluster returns true if a member with the given peerURL exists in the cluster.
func (c *EtcdClusterClient) WasMemberInCluster(ctx context.Context, peerURL string) (bool, error) {
	members, err := c.ListMembers(ctx)
	if err != nil {
		return false, err
	}
	for _, m := range members {
		for _, u := range m.PeerURLs {
			if u == peerURL {
				return true, nil
			}
		}
	}
	return false, nil
}

// RemoveStaleMember removes any member whose PeerURLs contain peerURL.
// Retries on "unhealthy cluster" errors that can occur during rolling StatefulSet updates
// when the cluster briefly reports unhealthy due to the recently-terminated member.
func (c *EtcdClusterClient) RemoveStaleMember(ctx context.Context, peerURL string) error {
	const maxAttempts = 10
	backoff := 3 * time.Second
	var lastErr error
	for attempt := range maxAttempts {
		resp, err := c.cluster.MemberList(ctx)
		if err != nil {
			return fmt.Errorf("failed to list members to find stale entry: %w", err)
		}
		var found bool
		for _, m := range resp.Members {
			for _, u := range m.PeerURLs {
				if u == peerURL {
					found = true
					_, removeErr := c.cluster.MemberRemove(ctx, m.ID)
					if removeErr == nil {
						return nil
					}
					if !strings.Contains(removeErr.Error(), "unhealthy cluster") {
						return fmt.Errorf("failed to remove stale member %x (peerURL %s): %w", m.ID, peerURL, removeErr)
					}
					lastErr = removeErr
				}
			}
		}
		if !found {
			return nil
		}
		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}
	}
	return fmt.Errorf("failed to remove stale member (peerURL %s) after %d attempts: %w", peerURL, maxAttempts, lastErr)
}

// UpdateMemberPeerURL updates the peer URLs of the member with the given ID.
// This is used when a member's advertised peer URL changes (e.g. after enabling peer TLS).
func (c *EtcdClusterClient) UpdateMemberPeerURL(ctx context.Context, memberID uint64, peerURL string) error {
	_, err := c.cluster.MemberUpdate(ctx, memberID, []string{peerURL})
	if err != nil {
		return fmt.Errorf("failed to update peer URL for member %x: %w", memberID, err)
	}
	return nil
}
