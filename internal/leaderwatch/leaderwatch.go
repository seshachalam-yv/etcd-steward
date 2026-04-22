// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package leaderwatch

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Role represents the leadership role of this pod.
type Role string

const (
	// Leader indicates this pod holds the leader key.
	Leader Role = "Leader"
	// Follower indicates another pod holds the leader key.
	Follower Role = "Follower"
	// Unknown indicates the leader key is empty or could not be read.
	Unknown Role = "Unknown"
)

// LeaderKey is the etcd key used to determine the current leader.
const LeaderKey = "/steward/leader"

// Watcher periodically polls an etcd key to determine the leadership role of
// the current pod and notifies subscribers when the role changes.
type Watcher struct {
	podName    string
	pollPeriod time.Duration
	etcdClient clientv3.KV
	logger     *zap.Logger

	mu          sync.RWMutex
	currentRole Role
	subscribers []chan<- Role
}

// New creates a new Watcher that polls the leader key every pollPeriod.
func New(podName string, pollPeriod time.Duration, etcdClient clientv3.KV, logger *zap.Logger) *Watcher {
	return &Watcher{
		podName:     podName,
		pollPeriod:  pollPeriod,
		etcdClient:  etcdClient,
		logger:      logger,
		currentRole: Unknown,
	}
}

// GetCurrentRole returns the last observed leadership role.
func (w *Watcher) GetCurrentRole() Role {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.currentRole
}

// Subscribe returns a channel that receives role change notifications. The
// channel is buffered with capacity 1 so a slow consumer does not block the
// watcher.
func (w *Watcher) Subscribe() <-chan Role {
	ch := make(chan Role, 1)
	w.mu.Lock()
	w.subscribers = append(w.subscribers, ch)
	w.mu.Unlock()
	return ch
}

// Run polls the leader key periodically until ctx is cancelled. It updates the
// current role and notifies subscribers on every role change.
func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.pollPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

func (w *Watcher) poll(ctx context.Context) {
	resp, err := w.etcdClient.Get(ctx, LeaderKey)
	if err != nil {
		w.logger.Error("failed to get leader key", zap.Error(err))
		return
	}

	var newRole Role
	if len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) == "" {
		newRole = Unknown
	} else if string(resp.Kvs[0].Value) == w.podName {
		newRole = Leader
	} else {
		newRole = Follower
	}

	w.mu.Lock()
	oldRole := w.currentRole
	w.currentRole = newRole
	var subs []chan<- Role
	if oldRole != newRole {
		subs = make([]chan<- Role, len(w.subscribers))
		copy(subs, w.subscribers)
	}
	w.mu.Unlock()

	if oldRole != newRole {
		w.logger.Info("role changed", zap.String("from", string(oldRole)), zap.String("to", string(newRole)))
		for _, ch := range subs {
			select {
			case ch <- newRole:
			default:
				// subscriber channel full, skip to avoid blocking
			}
		}
	}
}
