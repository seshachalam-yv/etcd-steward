// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/initializer"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

func newTestServer(status initializer.InitializationStatus) *Server {
	return NewServer(
		0,
		func() initializer.InitializationStatus { return status },
		func(_ context.Context, _ string) error { return nil },
		func() ([]byte, error) { return []byte("name: etcd-main"), nil },
		nil, // no snapshotter
		nil, // no store
		zap.NewNop(),
	)
}

func TestHandleInitializationStatus_New(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodGet, "/initialization/status", nil)
	w := httptest.NewRecorder()

	s.handleInitializationStatus(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "New" {
		t.Errorf("body = %q, want %q", string(body), "New")
	}
}

func TestHandleInitializationStatus_Successful(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusSuccessful)

	req := httptest.NewRequest(http.MethodGet, "/initialization/status", nil)
	w := httptest.NewRecorder()

	s.handleInitializationStatus(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	if string(body) != "Successful" {
		t.Errorf("body = %q, want %q", string(body), "Successful")
	}
}

func TestHandleHealthz_OK(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	s.handleHealthz(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", string(body), "ok")
	}
}

func TestHandleHealthz_ShuttingDown(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)
	s.isShutdown.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	s.handleHealthz(w, req)

	if w.Result().StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status code = %d, want %d", w.Result().StatusCode, http.StatusServiceUnavailable)
	}
}

func TestHandleSnapshotFull_NotConfigured(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodPost, "/snapshot/full", nil)
	w := httptest.NewRecorder()

	s.handleSnapshotFull(w, req)

	if w.Result().StatusCode != http.StatusNotImplemented {
		t.Errorf("status code = %d, want %d", w.Result().StatusCode, http.StatusNotImplemented)
	}
}

func TestHandleSnapshotDelta_NotConfigured(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodPost, "/snapshot/delta", nil)
	w := httptest.NewRecorder()

	s.handleSnapshotDelta(w, req)

	if w.Result().StatusCode != http.StatusNotImplemented {
		t.Errorf("status code = %d, want %d", w.Result().StatusCode, http.StatusNotImplemented)
	}
}

func TestHandleSnapshotLatest_NotConfigured(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodGet, "/snapshot/latest", nil)
	w := httptest.NewRecorder()

	s.handleSnapshotLatest(w, req)

	if w.Result().StatusCode != http.StatusNotImplemented {
		t.Errorf("status code = %d, want %d", w.Result().StatusCode, http.StatusNotImplemented)
	}
}

func TestHandleConfig(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	w := httptest.NewRecorder()

	s.handleConfig(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "name: etcd-main" {
		t.Errorf("body = %q, want %q", string(body), "name: etcd-main")
	}
}

func TestHandleInitializationStart_Successful(t *testing.T) {
	s := NewServer(
		0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusSuccessful },
		func(_ context.Context, _ string) error { return nil },
		nil,
		nil, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodPost, "/initialization/start?mode=sanity", nil)
	w := httptest.NewRecorder()

	s.handleInitializationStart(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "Successful\n" {
		t.Errorf("body = %q, want %q", string(body), "Successful\n")
	}
}

// mockSnapstoreForServer implements snapstore.Snapstore for server tests.
type mockSnapstoreForServer struct {
	snaps   []snapstore.Snapshot
	listErr error
}

func (m *mockSnapstoreForServer) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (m *mockSnapstoreForServer) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockSnapstoreForServer) List() ([]snapstore.Snapshot, error) {
	return m.snaps, m.listErr
}
func (m *mockSnapstoreForServer) Delete(_ snapstore.Snapshot) error { return nil }

func TestHandleSnapshotLatest_WithStore(t *testing.T) {
	store := &mockSnapstoreForServer{
		snaps: []snapstore.Snapshot{
			{Kind: "Full", LastRevision: 100, SnapDir: "b", SnapName: "f1"},
			{Kind: "Incremental", LastRevision: 200, SnapDir: "b", SnapName: "i1"},
		},
	}

	s := NewServer(
		0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil,
		nil,
		store,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodGet, "/snapshot/latest", nil)
	w := httptest.NewRecorder()

	s.handleSnapshotLatest(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Result().StatusCode, http.StatusOK)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleConfig
// ---------------------------------------------------------------------------

func TestHandleConfig_MethodNotAllowed(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/config", nil)
			w := httptest.NewRecorder()
			s.handleConfig(w, req)
			if w.Result().StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("method %s: status = %d, want %d", method, w.Result().StatusCode, http.StatusMethodNotAllowed)
			}
		})
	}
}

func TestHandleConfig_ConfigFnError(t *testing.T) {
	s := NewServer(
		0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		func() ([]byte, error) { return nil, fmt.Errorf("config load failed") },
		nil, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	w := httptest.NewRecorder()
	s.handleConfig(w, req)

	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusInternalServerError)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleInitializationStart
// ---------------------------------------------------------------------------

func TestHandleInitializationStart_GetMethod_OK(t *testing.T) {
	s := NewServer(
		0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil, nil, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodGet, "/initialization/start", nil)
	w := httptest.NewRecorder()
	s.handleInitializationStart(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "OK\n" {
		t.Errorf("body = %q, want %q", string(body), "OK\n")
	}
}

func TestHandleInitializationStart_MethodNotAllowed(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodDelete, "/initialization/start", nil)
	w := httptest.NewRecorder()
	s.handleInitializationStart(w, req)

	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestHandleInitializationStart_StartFnError(t *testing.T) {
	s := NewServer(
		0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return fmt.Errorf("start failed") },
		nil, nil, nil,
		zap.NewNop(),
	)

	// Use GET so we don't enter the polling loop — startFn error is checked before branching.
	req := httptest.NewRequest(http.MethodGet, "/initialization/start", nil)
	w := httptest.NewRecorder()
	s.handleInitializationStart(w, req)

	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusInternalServerError)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleInitializationStatus
// ---------------------------------------------------------------------------

func TestHandleInitializationStatus_MethodNotAllowed(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodPost, "/initialization/status", nil)
	w := httptest.NewRecorder()
	s.handleInitializationStatus(w, req)

	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// mockSnapshotter — implements Snapshotter for snapshot handler tests.
// ---------------------------------------------------------------------------

type mockSnapshotter struct {
	fullSnap  *snapstore.Snapshot
	fullErr   error
	deltaSnap *snapstore.Snapshot
	deltaErr  error
}

func (m *mockSnapshotter) TriggerFullSnapshot(_ context.Context, _ bool) (*snapstore.Snapshot, error) {
	return m.fullSnap, m.fullErr
}

func (m *mockSnapshotter) TriggerDeltaSnapshot(_ context.Context) (*snapstore.Snapshot, error) {
	return m.deltaSnap, m.deltaErr
}

// ---------------------------------------------------------------------------
// Additional tests — handleSnapshotFull
// ---------------------------------------------------------------------------

func TestHandleSnapshotFull_MethodNotAllowed(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodGet, "/snapshot/full", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotFull(w, req)

	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestHandleSnapshotFull_SnapshotterError(t *testing.T) {
	snap := &mockSnapshotter{fullErr: fmt.Errorf("full snapshot failed")}
	s := NewServer(0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil, snap, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodPost, "/snapshot/full", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotFull(w, req)

	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusInternalServerError)
	}
}

func TestHandleSnapshotFull_Success(t *testing.T) {
	returned := &snapstore.Snapshot{Kind: "Full", LastRevision: 42}
	snap := &mockSnapshotter{fullSnap: returned}
	s := NewServer(0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil, snap, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodPost, "/snapshot/full", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotFull(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusOK)
	}
	if ct := w.Result().Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleSnapshotDelta
// ---------------------------------------------------------------------------

func TestHandleSnapshotDelta_MethodNotAllowed(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodGet, "/snapshot/delta", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotDelta(w, req)

	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestHandleSnapshotDelta_SnapshotterError(t *testing.T) {
	snap := &mockSnapshotter{deltaErr: fmt.Errorf("delta snapshot failed")}
	s := NewServer(0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil, snap, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodPost, "/snapshot/delta", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotDelta(w, req)

	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusInternalServerError)
	}
}

func TestHandleSnapshotDelta_Success(t *testing.T) {
	returned := &snapstore.Snapshot{Kind: "Incremental", LastRevision: 100}
	snap := &mockSnapshotter{deltaSnap: returned}
	s := NewServer(0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil, snap, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodPost, "/snapshot/delta", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotDelta(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusOK)
	}
	if ct := w.Result().Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleConfig nil configFn
// ---------------------------------------------------------------------------

func TestHandleConfig_NilConfigFn(t *testing.T) {
	s := NewServer(
		0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil, // configFn is nil → 501 Not Implemented
		nil, nil,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	w := httptest.NewRecorder()
	s.handleConfig(w, req)

	if w.Result().StatusCode != http.StatusNotImplemented {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusNotImplemented)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleSnapshotLatest wrong method
// ---------------------------------------------------------------------------

func TestHandleSnapshotLatest_MethodNotAllowed(t *testing.T) {
	s := newTestServer(initializer.InitializationStatusNew)

	req := httptest.NewRequest(http.MethodPost, "/snapshot/latest", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotLatest(w, req)

	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleSnapshotLatest store list error
// ---------------------------------------------------------------------------

func TestHandleSnapshotLatest_ListError(t *testing.T) {
	store := &mockSnapstoreForServer{listErr: fmt.Errorf("list failed")}
	s := NewServer(
		0,
		func() initializer.InitializationStatus { return initializer.InitializationStatusNew },
		func(_ context.Context, _ string) error { return nil },
		nil, nil, store,
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodGet, "/snapshot/latest", nil)
	w := httptest.NewRecorder()
	s.handleSnapshotLatest(w, req)

	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusInternalServerError)
	}
}

// ---------------------------------------------------------------------------
// Additional tests — handleInitializationStart POST cancelled context
// ---------------------------------------------------------------------------

func TestHandleInitializationStart_PostCancelledContext(t *testing.T) {
	s := NewServer(
		0,
		// statusFn always returns InProgress so the polling loop never exits normally.
		func() initializer.InitializationStatus { return initializer.InitializationStatusInProgress },
		func(_ context.Context, _ string) error { return nil },
		nil, nil, nil,
		zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel the context immediately so the polling loop exits on <-r.Context().Done().
	cancel()

	req := httptest.NewRequest(http.MethodPost, "/initialization/start", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	s.handleInitializationStart(w, req)

	if w.Result().StatusCode != http.StatusRequestTimeout {
		t.Errorf("status = %d, want %d", w.Result().StatusCode, http.StatusRequestTimeout)
	}
}
