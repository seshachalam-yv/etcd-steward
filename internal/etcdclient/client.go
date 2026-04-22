// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdclient

import (
	"context"
	"io"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// KV defines the key-value operations on an etcd cluster.
type KV interface {
	// Get retrieves the value for the given key.
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	// Put sets the value for the given key.
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	// Delete removes the given key.
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	// Compact compacts the etcd key-value store up to the given revision.
	Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error)
}

// Maintenance defines cluster maintenance operations.
type Maintenance interface {
	// Defragment defragments the storage of the etcd member at the given endpoint.
	Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error)
	// AlarmList lists all active alarms.
	AlarmList(ctx context.Context) (*clientv3.AlarmResponse, error)
	// AlarmDisarm disarms the given alarm member.
	AlarmDisarm(ctx context.Context, m *clientv3.AlarmMember) (*clientv3.AlarmResponse, error)
	// Status returns the status of the etcd member at the given endpoint.
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
	// Snapshot streams a point-in-time snapshot of the etcd member.
	Snapshot(ctx context.Context) (io.ReadCloser, error)
}

// Cluster defines etcd cluster membership operations.
type Cluster interface {
	// MemberList lists all members of the etcd cluster.
	MemberList(ctx context.Context) (*clientv3.MemberListResponse, error)
	// MemberAdd adds a new member with the given peer URLs.
	MemberAdd(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error)
	// MemberAddAsLearner adds a new member as a learner (non-voting) with the given peer URLs.
	MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error)
	// MemberRemove removes a member by its ID.
	MemberRemove(ctx context.Context, id uint64) (*clientv3.MemberRemoveResponse, error)
	// MemberPromote promotes a learner member to a full voting member.
	MemberPromote(ctx context.Context, id uint64) (*clientv3.MemberPromoteResponse, error)
}

// Client wraps an etcd clientv3.Client and implements the KV, Maintenance,
// and Cluster interfaces.
type Client struct {
	inner *clientv3.Client
}

// NewClient creates a new Client wrapping the given etcd clientv3 client.
func NewClient(c *clientv3.Client) *Client {
	return &Client{inner: c}
}

// Close closes the underlying etcd client connection.
func (c *Client) Close() error {
	return c.inner.Close()
}

// --- KV ---

// Get retrieves the value for the given key.
func (c *Client) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return c.inner.Get(ctx, key, opts...)
}

// Put sets the value for the given key.
func (c *Client) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return c.inner.Put(ctx, key, val, opts...)
}

// Delete removes the given key.
func (c *Client) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return c.inner.Delete(ctx, key, opts...)
}

// Compact compacts the etcd key-value store up to the given revision.
func (c *Client) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return c.inner.Compact(ctx, rev, opts...)
}

// --- Maintenance ---

// Defragment defragments the storage of the etcd member at the given endpoint.
func (c *Client) Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error) {
	return c.inner.Defragment(ctx, endpoint)
}

// AlarmList lists all active alarms.
func (c *Client) AlarmList(ctx context.Context) (*clientv3.AlarmResponse, error) {
	return c.inner.AlarmList(ctx)
}

// AlarmDisarm disarms the given alarm member.
func (c *Client) AlarmDisarm(ctx context.Context, m *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	return c.inner.AlarmDisarm(ctx, m)
}

// Status returns the status of the etcd member at the given endpoint.
func (c *Client) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	return c.inner.Status(ctx, endpoint)
}

// Snapshot streams a point-in-time snapshot of the etcd member.
func (c *Client) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return c.inner.Snapshot(ctx)
}

// --- Cluster ---

// MemberList lists all members of the etcd cluster.
func (c *Client) MemberList(ctx context.Context) (*clientv3.MemberListResponse, error) {
	return c.inner.MemberList(ctx)
}

// MemberAdd adds a new member with the given peer URLs.
func (c *Client) MemberAdd(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	return c.inner.MemberAdd(ctx, peerAddrs)
}

// MemberAddAsLearner adds a new member as a learner (non-voting) with the given peer URLs.
func (c *Client) MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	return c.inner.MemberAddAsLearner(ctx, peerAddrs)
}

// MemberRemove removes a member by its ID.
func (c *Client) MemberRemove(ctx context.Context, id uint64) (*clientv3.MemberRemoveResponse, error) {
	return c.inner.MemberRemove(ctx, id)
}

// MemberPromote promotes a learner member to a full voting member.
func (c *Client) MemberPromote(ctx context.Context, id uint64) (*clientv3.MemberPromoteResponse, error) {
	return c.inner.MemberPromote(ctx, id)
}
