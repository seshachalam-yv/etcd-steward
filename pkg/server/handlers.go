// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package server provides the HTTP server for etcd-steward.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/initializer"
	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// Snapshotter is the interface for triggering snapshots.
type Snapshotter interface {
	TriggerFullSnapshot(ctx context.Context, isFinal bool) (*snapstore.Snapshot, error)
	TriggerDeltaSnapshot(ctx context.Context) (*snapstore.Snapshot, error)
}

// Server is the HTTP server for etcd-steward.
type Server struct {
	port        int
	certFile    string
	keyFile     string
	mux         *http.ServeMux
	statusFn    func() initializer.InitializationStatus
	startFn     func(ctx context.Context, mode string) error
	configFn    func() ([]byte, error)
	snapshotter Snapshotter
	store       snapstore.Snapstore
	shutdownCh  chan struct{}
	isShutdown  atomic.Bool
	logger      *zap.Logger
}

// NewServer creates a Server with the given configuration.
func NewServer(
	port int,
	statusFn func() initializer.InitializationStatus,
	startFn func(ctx context.Context, mode string) error,
	configFn func() ([]byte, error),
	snapshotter Snapshotter,
	store snapstore.Snapstore,
	logger *zap.Logger,
) *Server {
	return NewServerWithTLS(port, "", "", statusFn, startFn, configFn, snapshotter, store, logger)
}

// NewServerWithTLS creates a Server that serves HTTPS when certFile and keyFile are non-empty.
func NewServerWithTLS(
	port int,
	certFile, keyFile string,
	statusFn func() initializer.InitializationStatus,
	startFn func(ctx context.Context, mode string) error,
	configFn func() ([]byte, error),
	snapshotter Snapshotter,
	store snapstore.Snapstore,
	logger *zap.Logger,
) *Server {
	s := &Server{
		port:        port,
		certFile:    certFile,
		keyFile:     keyFile,
		mux:         http.NewServeMux(),
		statusFn:    statusFn,
		startFn:     startFn,
		configFn:    configFn,
		snapshotter: snapshotter,
		store:       store,
		shutdownCh:  make(chan struct{}),
		logger:      logger,
	}
	s.registerRoutes()
	return s
}

// registerRoutes registers all HTTP handlers on the mux.
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/config", s.handleConfig)
	s.mux.HandleFunc("/initialization/start", s.handleInitializationStart)
	s.mux.HandleFunc("/initialization/status", s.handleInitializationStatus)
	s.mux.HandleFunc("/snapshot/full", s.handleSnapshotFull)
	s.mux.HandleFunc("/snapshot/delta", s.handleSnapshotDelta)
	s.mux.HandleFunc("/snapshot/latest", s.handleSnapshotLatest)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.Handle("/metrics", promhttp.Handler())
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.mux,
	}

	errCh := make(chan error, 1)
	go func() {
		if s.certFile != "" && s.keyFile != "" {
			s.logger.Info("starting HTTPS server", zap.Int("port", s.port))
			if err := srv.ListenAndServeTLS(s.certFile, s.keyFile); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		} else {
			s.logger.Info("starting HTTP server", zap.Int("port", s.port))
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		close(s.shutdownCh)
		s.isShutdown.Store(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.configFn == nil {
		http.Error(w, "config not available", http.StatusNotImplemented)
		return
	}
	data, err := s.configFn()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-yaml")
	w.WriteHeader(http.StatusOK)
	w.Write(data) //nolint:errcheck
}

func (s *Server) handleInitializationStart(w http.ResponseWriter, r *http.Request) {
	// Accept both GET (etcd-wrapper compat) and POST.
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "full"
	}

	if err := s.startFn(r.Context(), mode); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// GET is etcd-wrapper's fire-and-forget trigger — return immediately.
	// etcd-wrapper polls /initialization/status separately to detect completion.
	if r.Method == http.MethodGet {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK\n") //nolint:errcheck
		return
	}

	// POST blocks until Successful (used by tests and direct callers).
	// Poll until successful or timeout.
	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			http.Error(w, "initialization timed out", http.StatusInternalServerError)
			return
		case <-r.Context().Done():
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
			return
		case <-ticker.C:
			status := s.statusFn()
			if status == initializer.InitializationStatusSuccessful {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, "Successful\n") //nolint:errcheck
				return
			}
		}
	}
}

func (s *Server) handleInitializationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status := s.statusFn()
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, string(status)) //nolint:errcheck
}

func (s *Server) handleSnapshotFull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.snapshotter == nil {
		http.Error(w, "snapshots not configured", http.StatusNotImplemented)
		return
	}
	snap, err := s.snapshotter.TriggerFullSnapshot(r.Context(), false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(snap) //nolint:errcheck
}

func (s *Server) handleSnapshotDelta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.snapshotter == nil {
		http.Error(w, "snapshots not configured", http.StatusNotImplemented)
		return
	}
	snap, err := s.snapshotter.TriggerDeltaSnapshot(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(snap) //nolint:errcheck
}

func (s *Server) handleSnapshotLatest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.store == nil {
		http.Error(w, "snapstore not configured", http.StatusNotImplemented)
		return
	}

	snaps, err := s.store.List()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type latestResponse struct {
		FullSnapshot        *snapstore.Snapshot `json:"fullSnapshot,omitempty"`
		IncrementalSnapshot *snapstore.Snapshot `json:"incrementalSnapshot,omitempty"`
	}

	var resp latestResponse
	for i := len(snaps) - 1; i >= 0; i-- {
		if snaps[i].Kind == "Full" && resp.FullSnapshot == nil {
			s := snaps[i]
			resp.FullSnapshot = &s
		}
		if snaps[i].Kind == "Incremental" && resp.IncrementalSnapshot == nil {
			s := snaps[i]
			resp.IncrementalSnapshot = &s
		}
		if resp.FullSnapshot != nil && resp.IncrementalSnapshot != nil {
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	if s.isShutdown.Load() {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok") //nolint:errcheck
}
