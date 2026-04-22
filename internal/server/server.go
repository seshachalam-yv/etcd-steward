// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	// gracefulShutdownTimeout is the maximum duration to wait for in-flight
	// requests to complete during server shutdown.
	gracefulShutdownTimeout = 5 * time.Second
)

// InitStatus represents the initialization status of the etcd member.
type InitStatus string

const (
	// InitStatusNew indicates the member has not yet started initialization.
	InitStatusNew InitStatus = "New"
	// InitStatusProgress indicates initialization is in progress.
	InitStatusProgress InitStatus = "Progress"
	// InitStatusSuccessful indicates initialization completed successfully.
	InitStatusSuccessful InitStatus = "Successful"
	// InitStatusFailed indicates initialization failed.
	InitStatusFailed InitStatus = "Failed"
)

// Server is an HTTP server that exposes operational endpoints for etcd-steward.
// It always registers /metrics (Prometheus) and /healthz (liveness) handlers.
type Server struct {
	port   int
	mux    *http.ServeMux
	logger *zap.Logger
}

// New creates a new Server listening on the given port.
// /metrics and /healthz are registered automatically.
func New(port int, logger *zap.Logger) *Server {
	mux := http.NewServeMux()

	s := &Server{
		port:   port,
		mux:    mux,
		logger: logger,
	}

	// Always-on endpoints.
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "ok")
	})

	return s
}

// RegisterHandler registers an additional HTTP handler at the given path.
func (s *Server) RegisterHandler(path string, handler http.Handler) {
	s.mux.Handle(path, handler)
}

// RegisterInitializationEndpoints registers the etcd-wrapper-compatible
// initialization and configuration endpoints:
//
//   - GET  /initialization/status  -> {"status":"<InitStatus>"}
//   - POST /initialization/start   -> invokes startInit(ctx, mode) where
//     mode is read from the JSON body field "mode".
//   - GET  /config                 -> returns raw config bytes.
func (s *Server) RegisterInitializationEndpoints(
	getStatus func() InitStatus,
	startInit func(ctx context.Context, mode string) error,
	getConfig func() ([]byte, error),
) {
	s.mux.HandleFunc("/initialization/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]string{"status": string(getStatus())}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			s.logger.Error("failed to encode initialization status", zap.Error(err))
		}
	})

	s.mux.HandleFunc("/initialization/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		defer func() { _ = r.Body.Close() }()

		var req struct {
			Mode string `json:"mode"`
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				http.Error(w, "invalid JSON body", http.StatusBadRequest)
				return
			}
		}

		if err := startInit(r.Context(), req.Mode); err != nil {
			s.logger.Error("initialization start failed", zap.String("mode", req.Mode), zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	s.mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		data, err := getConfig()
		if err != nil {
			s.logger.Error("failed to get config", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	})
}

// Run starts the HTTP server and blocks until the context is cancelled.
// On context cancellation it performs a graceful shutdown, waiting up to 5
// seconds for in-flight requests to complete.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.mux,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("starting HTTP server", zap.Int("port", s.port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		s.logger.Info("shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown error: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}
