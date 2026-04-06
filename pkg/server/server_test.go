// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
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
	snaps []snapstore.Snapshot
}

func (m *mockSnapstoreForServer) Save(_ snapstore.Snapshot, _ io.ReadCloser) error { return nil }
func (m *mockSnapstoreForServer) Fetch(_ snapstore.Snapshot) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockSnapstoreForServer) List() ([]snapstore.Snapshot, error) {
	return m.snaps, nil
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
