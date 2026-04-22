// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdclient

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Lock provides a distributed lock backed by an etcd lease and concurrency mutex.
type Lock struct {
	client  *clientv3.Client
	prefix  string
	ttl     int
	session *concurrency.Session
	mutex   *concurrency.Mutex
}

// NewLock creates a new Lock that will use the given etcd client, key prefix,
// and session TTL (in seconds).
func NewLock(client *clientv3.Client, prefix string, ttl int) *Lock {
	return &Lock{
		client: client,
		prefix: prefix,
		ttl:    ttl,
	}
}

// Acquire creates a new session and acquires the distributed lock.
// It blocks until the lock is obtained or the context is cancelled.
func (l *Lock) Acquire(ctx context.Context) error {
	session, err := concurrency.NewSession(l.client, concurrency.WithTTL(l.ttl))
	if err != nil {
		return fmt.Errorf("failed to create etcd session for lock %q: %w", l.prefix, err)
	}
	l.session = session

	l.mutex = concurrency.NewMutex(session, l.prefix)
	if err := l.mutex.Lock(ctx); err != nil {
		// Best-effort close the session on lock failure.
		_ = session.Close()
		l.session = nil
		l.mutex = nil
		return fmt.Errorf("failed to acquire lock %q: %w", l.prefix, err)
	}
	return nil
}

// Release releases the distributed lock and closes the underlying session.
func (l *Lock) Release(ctx context.Context) error {
	if l.mutex == nil {
		return nil
	}

	var unlockErr error
	if err := l.mutex.Unlock(ctx); err != nil {
		unlockErr = fmt.Errorf("failed to unlock %q: %w", l.prefix, err)
	}
	l.mutex = nil

	if l.session != nil {
		if err := l.session.Close(); err != nil {
			if unlockErr != nil {
				return fmt.Errorf("%w; additionally failed to close session: %v", unlockErr, err)
			}
			return fmt.Errorf("failed to close session for lock %q: %w", l.prefix, err)
		}
		l.session = nil
	}

	return unlockErr
}
