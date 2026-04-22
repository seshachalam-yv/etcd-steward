// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
)

// freePort starts a server on :0 to find a free port, then returns it.
func startTestServer(t *testing.T, s *Server) (baseURL string, cancel context.CancelFunc) {
	t.Helper()

	ctx, cancelFn := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx)
	}()

	// Give server time to start.
	time.Sleep(100 * time.Millisecond)

	baseURL = fmt.Sprintf("http://127.0.0.1:%d", s.port)
	return baseURL, cancelFn
}

func TestServer_MetricsAlwaysOn(t *testing.T) {
	logger := zap.NewNop()
	s := New(18390, logger)
	base, cancel := startTestServer(t, s)
	defer cancel()

	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("/metrics returned empty body")
	}
}

func TestServer_HealthzAlwaysOn(t *testing.T) {
	logger := zap.NewNop()
	s := New(18391, logger)
	base, cancel := startTestServer(t, s)
	defer cancel()

	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /healthz, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("expected 'ok', got %q", string(body))
	}
}

func TestServer_RegisterHandler(t *testing.T) {
	logger := zap.NewNop()
	s := New(18392, logger)
	s.RegisterHandler("/custom", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "custom-response")
	}))

	base, cancel := startTestServer(t, s)
	defer cancel()

	resp, err := http.Get(base + "/custom")
	if err != nil {
		t.Fatalf("GET /custom failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /custom, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "custom-response" {
		t.Fatalf("expected 'custom-response', got %q", string(body))
	}
}

func TestServer_UnregisteredPath404(t *testing.T) {
	logger := zap.NewNop()
	s := New(18393, logger)
	base, cancel := startTestServer(t, s)
	defer cancel()

	resp, err := http.Get(base + "/nonexistent")
	if err != nil {
		t.Fatalf("GET /nonexistent failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 from /nonexistent, got %d", resp.StatusCode)
	}
}

func TestServer_GracefulShutdown(t *testing.T) {
	logger := zap.NewNop()
	s := New(18394, logger)
	base, cancel := startTestServer(t, s)

	// Verify server is running.
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("server not running: %v", err)
	}
	_ = resp.Body.Close()

	// Cancel context to trigger graceful shutdown.
	cancel()

	// Give the server time to shut down.
	time.Sleep(200 * time.Millisecond)

	// After shutdown, requests should fail.
	_, err = http.Get(base + "/healthz")
	if err == nil {
		t.Fatal("expected error after shutdown, got nil")
	}
}

func TestServer_InitializationStatusEndpoint(t *testing.T) {
	logger := zap.NewNop()
	s := New(18395, logger)

	status := InitStatusNew
	s.RegisterInitializationEndpoints(
		func() InitStatus { return status },
		func(_ context.Context, mode string) error {
			status = InitStatusProgress
			return nil
		},
		func() ([]byte, error) {
			return []byte("config-data"), nil
		},
	)

	base, cancel := startTestServer(t, s)
	defer cancel()

	// GET /initialization/status should return {"status":"New"}.
	resp, err := http.Get(base + "/initialization/status")
	if err != nil {
		t.Fatalf("GET /initialization/status failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["status"] != "New" {
		t.Fatalf("expected status 'New', got %q", result["status"])
	}

	// GET /config should return the config bytes.
	cfgResp, err := http.Get(base + "/config")
	if err != nil {
		t.Fatalf("GET /config failed: %v", err)
	}
	defer func() { _ = cfgResp.Body.Close() }()

	cfgBody, _ := io.ReadAll(cfgResp.Body)
	if string(cfgBody) != "config-data" {
		t.Fatalf("expected 'config-data', got %q", string(cfgBody))
	}
}
