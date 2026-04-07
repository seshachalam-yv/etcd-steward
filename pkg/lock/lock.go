// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package lock provides a distributed lock backed by an etcd lease and transaction.
package lock

import (
	"context"
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// lockTTLSeconds is the TTL for the etcd lease backing the lock.
	lockTTLSeconds = 15
	// lockKeyPrefix is the prefix for all lock keys.
	lockKeyPrefix = "/druid/lock/"
)

// EtcdAPI is the subset of the etcd client used by Lock.
type EtcdAPI interface {
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	Txn(ctx context.Context) clientv3.Txn
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

// etcdAPIAdapter adapts a clientv3.Client to the EtcdAPI interface.
type etcdAPIAdapter struct {
	client *clientv3.Client
}

func (a *etcdAPIAdapter) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return a.client.Grant(ctx, ttl)
}

func (a *etcdAPIAdapter) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return a.client.Revoke(ctx, id)
}

func (a *etcdAPIAdapter) Txn(ctx context.Context) clientv3.Txn {
	return a.client.Txn(ctx)
}

func (a *etcdAPIAdapter) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return a.client.Watch(ctx, key, opts...)
}

// Lock provides a distributed lock backed by an etcd lease and atomic transaction.
type Lock struct {
	api     EtcdAPI
	lockKey string
	leaseID clientv3.LeaseID
	mu      sync.Mutex
}

// New creates a Lock that uses the given etcd client.
// The lock key is /druid/lock/<namespace>/<name>.
func New(client *clientv3.Client, namespace, name string) *Lock {
	return &Lock{
		api:     &etcdAPIAdapter{client: client},
		lockKey: lockKeyPrefix + namespace + "/" + name,
	}
}

// NewWithAPI creates a Lock with a custom EtcdAPI implementation (for testing).
func NewWithAPI(api EtcdAPI, namespace, name string) *Lock {
	return &Lock{
		api:     api,
		lockKey: lockKeyPrefix + namespace + "/" + name,
	}
}

// Acquire obtains the distributed lock. It grants a lease with a 15-second TTL
// and uses an atomic transaction (CreateRevision==0) to claim the key. If the key
// is already held, it watches for deletion and retries.
func (l *Lock) Acquire(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for {
		// Check if context is already cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Grant a lease.
		resp, err := l.api.Grant(ctx, lockTTLSeconds)
		if err != nil {
			return fmt.Errorf("failed to grant lease for lock %s: %w", l.lockKey, err)
		}

		// Try to acquire: key must not exist (CreateRevision == 0).
		txnResp, err := l.api.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(l.lockKey), "=", 0)).
			Then(clientv3.OpPut(l.lockKey, "", clientv3.WithLease(resp.ID))).
			Commit()
		if err != nil {
			// Revoke the lease we just created since the txn failed.
			_, _ = l.api.Revoke(ctx, resp.ID)
			return fmt.Errorf("failed to execute lock transaction for %s: %w", l.lockKey, err)
		}

		if txnResp.Succeeded {
			l.leaseID = resp.ID
			return nil
		}

		// Lock is held by someone else. Revoke our unused lease and watch for deletion.
		_, _ = l.api.Revoke(ctx, resp.ID)

		// Watch for deletion of the lock key.
		watchCtx, watchCancel := context.WithCancel(ctx)
		wch := l.api.Watch(watchCtx, l.lockKey)

		deleted := false
		for watchResp := range wch {
			for _, ev := range watchResp.Events {
				if ev.Type == clientv3.EventTypeDelete {
					deleted = true
					break
				}
			}
			if deleted {
				break
			}
		}
		watchCancel()

		// Check if the parent context was cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Retry acquiring.
	}
}

// Release releases the distributed lock by revoking the lease, which automatically
// deletes the lock key.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.leaseID == 0 {
		return nil
	}

	_, err := l.api.Revoke(ctx, l.leaseID)
	l.leaseID = 0
	if err != nil {
		return fmt.Errorf("failed to revoke lease for lock %s: %w", l.lockKey, err)
	}
	return nil
}
