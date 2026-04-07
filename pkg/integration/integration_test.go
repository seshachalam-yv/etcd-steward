// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package integration contains end-to-end integration tests for etcd-steward.
// These tests exercise the full HTTP lifecycle against a real Initializer running
// path C (fresh single-node, no existing data dir, no snapshots) and path A
// (data dir exists with valid bbolt DB). No real etcd instance is required.
package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/compression"
	"github.com/gardener/etcd-steward/pkg/etcdclient"
	"github.com/gardener/etcd-steward/pkg/initializer"
	"github.com/gardener/etcd-steward/pkg/member"
	"github.com/gardener/etcd-steward/pkg/server"
	"github.com/gardener/etcd-steward/pkg/statemachine"
)

// noopEtcdStatus satisfies initializer.EtcdStatusAPI.
type noopEtcdStatus struct{}

func (n *noopEtcdStatus) Status(_ context.Context, _ string) (*initializer.EtcdStatusResponse, error) {
	return &initializer.EtcdStatusResponse{MemberID: 1, ClusterID: 1}, nil
}

// noopClusterClient satisfies etcdclient.ClusterClient — never called in path A/C.
type noopClusterClient struct{}

func (n *noopClusterClient) AddLearner(_ context.Context, _ string) (uint64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (n *noopClusterClient) PromoteMember(_ context.Context, _ uint64) error {
	return fmt.Errorf("not implemented")
}
func (n *noopClusterClient) RemoveMember(_ context.Context, _ uint64) error { return nil }
func (n *noopClusterClient) ListMembers(_ context.Context) ([]etcdclient.Member, error) {
	return nil, nil
}
func (n *noopClusterClient) WasMemberInCluster(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (n *noopClusterClient) RemoveStaleMember(_ context.Context, _ string) error { return nil }

// newTestServer creates a Server listening on a free OS-assigned port.
// Returns the server, its base URL, and a cancel func to shut it down.
func newTestServer(t *testing.T, init *initializer.Initializer, dataDir string) (baseURL string, cancel func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	logger := zap.NewNop()
	srv := server.NewServer(
		port,
		init.GetStatus,
		func(ctx context.Context, mode string) error { return init.Start(ctx, mode) },
		func() ([]byte, error) {
			return []byte("name: etcd-main\ndata-dir: " + dataDir), nil
		},
		nil, // no snapshotter
		nil, // no store
		logger,
	)

	ctx, cancelFn := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- srv.Run(ctx) }()

	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	// Wait for server to be ready.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(10 * time.Millisecond)
	}

	return base, cancelFn
}

func newInitializer(dataDir string) *initializer.Initializer {
	logger := zap.NewNop()
	compressor, _ := compression.NewCompressor("none")
	return initializer.New(
		"etcd-main-0", "default",
		"https://etcd-main-0:2380",
		dataDir, "",
		"etcd-main-0=https://etcd-main-0:2380",
		true,  // singleNode
		false, // no learner annotation
		&statemachine.NoopRecorder{},
		&member.NoopClient{},
		&noopClusterClient{},
		&noopEtcdStatus{},
		"http://localhost:2379",
		logger,
		nil,        // no snapstore
		compressor, // noop compressor
	)
}

// TestHTTPLifecycle_PathC tests the complete HTTP lifecycle for initialization path C:
// fresh single-node pod with no existing data directory and no snapshots.
func TestHTTPLifecycle_PathC(t *testing.T) {
	dataDir := t.TempDir()
	init := newInitializer(dataDir)
	base, cancel := newTestServer(t, init, dataDir)
	defer cancel()

	client := &http.Client{Timeout: 10 * time.Second}

	// 1. /healthz before init → 200 "ok"
	t.Run("healthz before init", func(t *testing.T) {
		resp, err := client.Get(base + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if strings.TrimSpace(string(body)) != "ok" {
			t.Errorf("body = %q, want \"ok\"", string(body))
		}
	})

	// 2. /initialization/status = "New" before start
	t.Run("status before start is New", func(t *testing.T) {
		resp, err := client.Get(base + "/initialization/status")
		if err != nil {
			t.Fatalf("GET /initialization/status: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "New" {
			t.Errorf("status = %q, want \"New\"", string(body))
		}
	})

	// 3. /config → 200 with YAML
	t.Run("config returns yaml", func(t *testing.T) {
		resp, err := client.Get(base + "/config")
		if err != nil {
			t.Fatalf("GET /config: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "name: etcd-main") {
			t.Errorf("config missing expected content: %s", string(body))
		}
	})

	// 4. POST /initialization/start → blocks, returns "Successful"
	t.Run("initialization start path C", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
			base+"/initialization/start?mode=sanity", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("POST /initialization/start: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200; body: %s", resp.StatusCode, body)
		}
		if strings.TrimSpace(string(body)) != "Successful" {
			t.Errorf("body = %q, want \"Successful\"", string(body))
		}
	})

	// 5. /initialization/status = "Successful" after start
	t.Run("status after start is Successful", func(t *testing.T) {
		resp, err := client.Get(base + "/initialization/status")
		if err != nil {
			t.Fatalf("GET /initialization/status: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "Successful" {
			t.Errorf("status = %q, want \"Successful\"", string(body))
		}
	})

	// 6. for path C (no existing DB), initializer returns Successful without
	// creating member/snap — etcd creates those dirs when it starts.
	// Verify status reflects Successful (checked in step 5 already);
	// verify dataDir itself still exists.
	t.Run("data dir base exists", func(t *testing.T) {
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			t.Errorf("expected base dataDir %s to exist", dataDir)
		}
	})

	// 7. idempotency — second POST returns Successful immediately
	t.Run("start is idempotent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
			base+"/initialization/start?mode=sanity", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("second POST /initialization/start: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if strings.TrimSpace(string(body)) != "Successful" {
			t.Errorf("idempotent body = %q, want \"Successful\"", string(body))
		}
	})

	// 8. /metrics reachable
	t.Run("metrics reachable", func(t *testing.T) {
		resp, err := client.Get(base + "/metrics")
		if err != nil {
			t.Fatalf("GET /metrics: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "go_gc_duration_seconds") {
			t.Errorf("metrics missing expected prometheus content")
		}
	})

	// 9. wrong method on /initialization/start → 405 (DELETE is not accepted)
	t.Run("wrong method rejected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, base+"/initialization/start", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("DELETE /initialization/start: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", resp.StatusCode)
		}
	})
}

// TestHTTPLifecycle_PathA tests path A: data dir exists with valid bbolt DB → sanity validates OK.
func TestHTTPLifecycle_PathA(t *testing.T) {
	dataDir := t.TempDir()

	// Pre-create a valid bbolt DB so validator opens it successfully.
	dbDir := filepath.Join(dataDir, "member", "snap")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatal(err)
	}
	db, err := bbolt.Open(filepath.Join(dbDir, "db"), 0600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("create bbolt: %v", err)
	}
	db.Close()

	init := newInitializer(dataDir)
	base, cancel := newTestServer(t, init, dataDir)
	defer cancel()

	ctx, ctxCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ctxCancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/initialization/start?mode=sanity", nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("POST /initialization/start: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(body)) != "Successful" {
		t.Errorf("path A start body = %q, want \"Successful\"", string(body))
	}
}

// TestServerReachableBeforeInit verifies the server responds to /healthz
// immediately — before initialization begins (etcd-wrapper contract).
func TestServerReachableBeforeInit(t *testing.T) {
	dataDir := t.TempDir()

	// startFn that blocks — simulates init in progress.
	blockingStart := func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	status := initializer.InitializationStatusNew
	srv := server.NewServer(
		port,
		func() initializer.InitializationStatus { return status },
		blockingStart,
		func() ([]byte, error) { return []byte("name: etcd-main\ndata-dir: " + dataDir), nil },
		nil, nil, zap.NewNop(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx) //nolint:errcheck

	// Expect server up within 500ms.
	deadline := time.Now().Add(500 * time.Millisecond)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return // PASS — server was reachable before init
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("server was not reachable within 500ms (before init started)")
}
