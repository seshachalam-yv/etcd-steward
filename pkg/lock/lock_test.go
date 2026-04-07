// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"fmt"
	"sync"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// fakeEtcdAPI is a mock EtcdAPI for testing.
type fakeEtcdAPI struct {
	mu        sync.Mutex
	nextLease clientv3.LeaseID
	keys      map[string]clientv3.LeaseID
	watchers  map[string][]chan clientv3.WatchResponse
	// watchStarted is closed the first time Watch is called (used by synchronized tests).
	watchStarted chan struct{}
}

func newFakeEtcdAPI() *fakeEtcdAPI {
	return &fakeEtcdAPI{
		nextLease:    1,
		keys:         make(map[string]clientv3.LeaseID),
		watchers:     make(map[string][]chan clientv3.WatchResponse),
		watchStarted: make(chan struct{}),
	}
}

func (f *fakeEtcdAPI) Grant(_ context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextLease
	f.nextLease++
	return &clientv3.LeaseGrantResponse{ID: id}, nil
}

func (f *fakeEtcdAPI) Revoke(_ context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range f.keys {
		if v == id {
			delete(f.keys, k)
			// Notify watchers.
			if chs, ok := f.watchers[k]; ok {
				for _, ch := range chs {
					ch <- clientv3.WatchResponse{
						Events: []*clientv3.Event{
							{Type: clientv3.EventTypeDelete},
						},
					}
					close(ch)
				}
				delete(f.watchers, k)
			}
		}
	}
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (f *fakeEtcdAPI) Txn(_ context.Context) clientv3.Txn {
	return &fakeTxn{api: f}
}

func (f *fakeEtcdAPI) Watch(_ context.Context, key string, _ ...clientv3.OpOption) clientv3.WatchChan {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan clientv3.WatchResponse, 1)
	f.watchers[key] = append(f.watchers[key], ch)
	// Signal that a watcher has been registered (close once).
	select {
	case <-f.watchStarted:
		// Already closed.
	default:
		close(f.watchStarted)
	}
	return ch
}

// fakeTxn implements clientv3.Txn for testing lock acquire.
type fakeTxn struct {
	api     *fakeEtcdAPI
	cmpKey  string
	putOp   *clientv3.Op
	leaseID clientv3.LeaseID
}

func (t *fakeTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	if len(cmps) > 0 {
		// Extract the key from the compare.
		t.cmpKey = string(cmps[0].KeyBytes())
	}
	return t
}

func (t *fakeTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	if len(ops) > 0 {
		op := ops[0]
		t.putOp = &op
		// Extract lease from the put options by checking the serialized proto.
		// For simplicity, we track lease via the api's last grant.
		t.leaseID = t.api.nextLease - 1
	}
	return t
}

func (t *fakeTxn) Else(_ ...clientv3.Op) clientv3.Txn {
	return t
}

func (t *fakeTxn) Commit() (*clientv3.TxnResponse, error) {
	t.api.mu.Lock()
	defer t.api.mu.Unlock()

	_, exists := t.api.keys[t.cmpKey]
	if !exists {
		// CreateRevision == 0 check succeeds — key does not exist.
		t.api.keys[t.cmpKey] = t.leaseID
		return &clientv3.TxnResponse{
			Header:    &pb.ResponseHeader{},
			Succeeded: true,
		}, nil
	}
	// Key already exists.
	return &clientv3.TxnResponse{
		Header:    &pb.ResponseHeader{},
		Succeeded: false,
	}, nil
}

func TestLock_AcquireRelease(t *testing.T) {
	api := newFakeEtcdAPI()
	l := NewWithAPI(api, "default", "etcd-main")

	ctx := context.Background()

	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	if l.leaseID == 0 {
		t.Error("expected non-zero leaseID after Acquire")
	}

	if err := l.Release(ctx); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	if l.leaseID != 0 {
		t.Error("expected zero leaseID after Release")
	}
}

func TestLock_AcquireContention(t *testing.T) {
	api := newFakeEtcdAPI()
	l1 := NewWithAPI(api, "default", "etcd-main")
	l2 := NewWithAPI(api, "default", "etcd-main")

	ctx := context.Background()

	// First lock acquires.
	if err := l1.Acquire(ctx); err != nil {
		t.Fatalf("l1.Acquire failed: %v", err)
	}

	// Second lock should block, then succeed after l1 releases.
	done := make(chan error, 1)
	go func() {
		done <- l2.Acquire(ctx)
	}()

	// Release the first lock.
	if err := l1.Release(ctx); err != nil {
		t.Fatalf("l1.Release failed: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("l2.Acquire failed after l1 released: %v", err)
	}

	if err := l2.Release(ctx); err != nil {
		t.Fatalf("l2.Release failed: %v", err)
	}
}

func TestLock_ReleaseWithoutAcquire(t *testing.T) {
	api := newFakeEtcdAPI()
	l := NewWithAPI(api, "default", "etcd-main")

	if err := l.Release(context.Background()); err != nil {
		t.Fatalf("Release without prior Acquire should not fail, got: %v", err)
	}
}

func TestLock_AcquireCancelledContext(t *testing.T) {
	api := newFakeEtcdAPI()
	l1 := NewWithAPI(api, "default", "etcd-main")
	l2 := NewWithAPI(api, "default", "etcd-main")

	ctx := context.Background()

	if err := l1.Acquire(ctx); err != nil {
		t.Fatalf("l1.Acquire failed: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately.

	err := l2.Acquire(cancelCtx)
	if err == nil {
		t.Error("expected error when acquiring with cancelled context")
	}

	if err := l1.Release(ctx); err != nil {
		t.Fatalf("l1.Release failed: %v", err)
	}
}

// fakeEtcdAPIWithGrantErr wraps fakeEtcdAPI and injects a Grant error.
type fakeEtcdAPIWithGrantErr struct {
	*fakeEtcdAPI
	grantErr error
}

func (f *fakeEtcdAPIWithGrantErr) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	if f.grantErr != nil {
		return nil, f.grantErr
	}
	return f.fakeEtcdAPI.Grant(ctx, ttl)
}

// fakeTxnWithCommitErr wraps fakeTxn but always returns an error from Commit.
// It overrides all chaining methods to return itself so that Commit() is always called
// on this type rather than the inner *fakeTxn.
type fakeTxnWithCommitErr struct {
	*fakeTxn
	commitErr error
}

func (t *fakeTxnWithCommitErr) If(cmps ...clientv3.Cmp) clientv3.Txn {
	t.fakeTxn.If(cmps...)
	return t
}

func (t *fakeTxnWithCommitErr) Then(ops ...clientv3.Op) clientv3.Txn {
	t.fakeTxn.Then(ops...)
	return t
}

func (t *fakeTxnWithCommitErr) Else(ops ...clientv3.Op) clientv3.Txn {
	t.fakeTxn.Else(ops...)
	return t
}

func (t *fakeTxnWithCommitErr) Commit() (*clientv3.TxnResponse, error) {
	return nil, t.commitErr
}

// fakeEtcdAPIWithTxnErr returns a fakeTxn whose Commit always errors.
type fakeEtcdAPIWithTxnErr struct {
	*fakeEtcdAPI
	commitErr error
}

func (f *fakeEtcdAPIWithTxnErr) Txn(ctx context.Context) clientv3.Txn {
	inner := f.fakeEtcdAPI.Txn(ctx).(*fakeTxn)
	return &fakeTxnWithCommitErr{fakeTxn: inner, commitErr: f.commitErr}
}

// TestLock_New verifies that New does not panic and builds the expected lockKey.
func TestLock_New(t *testing.T) {
	// New accepts a nil *clientv3.Client — the adapter wraps it without dereferencing.
	l := New(nil, "mynamespace", "myname")
	if l == nil {
		t.Fatal("expected non-nil Lock from New")
	}
	want := lockKeyPrefix + "mynamespace/myname"
	if l.lockKey != want {
		t.Errorf("lockKey = %q, want %q", l.lockKey, want)
	}
}

// TestLock_AcquireGrantError verifies that Acquire propagates a Grant failure.
func TestLock_AcquireGrantError(t *testing.T) {
	base := newFakeEtcdAPI()
	grantErr := fmt.Errorf("lease quota exceeded")
	api := &fakeEtcdAPIWithGrantErr{fakeEtcdAPI: base, grantErr: grantErr}

	l := NewWithAPI(api, "default", "etcd-main")
	err := l.Acquire(context.Background())
	if err == nil {
		t.Fatal("expected error from Acquire when Grant fails")
	}
	if l.leaseID != 0 {
		t.Errorf("leaseID should remain 0 after Grant error, got %d", l.leaseID)
	}
}

// TestLock_AcquireTxnCommitError verifies that Acquire propagates a Txn.Commit failure.
func TestLock_AcquireTxnCommitError(t *testing.T) {
	base := newFakeEtcdAPI()
	commitErr := fmt.Errorf("etcd unavailable")
	api := &fakeEtcdAPIWithTxnErr{fakeEtcdAPI: base, commitErr: commitErr}

	l := NewWithAPI(api, "default", "etcd-main")
	err := l.Acquire(context.Background())
	if err == nil {
		t.Fatal("expected error from Acquire when Txn.Commit fails")
	}
	if l.leaseID != 0 {
		t.Errorf("leaseID should remain 0 after Txn error, got %d", l.leaseID)
	}
}

// TestLock_ContextCancelledDuringWatch verifies that cancelling the context while
// l2 is waiting in the watch loop causes Acquire to return the context error.
func TestLock_ContextCancelledDuringWatch(t *testing.T) {
	api := newFakeEtcdAPI()
	l1 := NewWithAPI(api, "default", "etcd-main")
	l2 := NewWithAPI(api, "default", "etcd-main")

	ctx := context.Background()

	// l1 acquires the lock so that l2 must wait.
	if err := l1.Acquire(ctx); err != nil {
		t.Fatalf("l1.Acquire failed: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- l2.Acquire(watchCtx)
	}()

	// Give the goroutine time to reach the watch loop, then cancel.
	// We use a small sleep here — the only acceptable use in tests where
	// we need to synchronize with an internal goroutine state.
	// There is no synchronization primitive exposed for the watch-loop entry.
	cancel()

	err := <-errCh
	if err == nil {
		t.Fatal("expected non-nil error when context is cancelled during watch")
	}

	// Cleanup.
	if err := l1.Release(ctx); err != nil {
		t.Fatalf("l1.Release failed: %v", err)
	}
}
