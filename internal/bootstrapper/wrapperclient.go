// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package bootstrapper

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPWrapperClient communicates with the etcd-wrapper sidecar over HTTP.
// It implements WrapperClient and adds SetReady for the Notes Option-1 flow
// where the steward controls pod readiness via POST /readyz/set.
type HTTPWrapperClient struct {
	baseURL string
	client  *http.Client
}

// NewHTTPWrapperClient creates a new HTTP-based wrapper client targeting the
// given base URL (e.g. "http://localhost:9095").
func NewHTTPWrapperClient(baseURL string) *HTTPWrapperClient {
	return &HTTPWrapperClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// StartEmbeddedEtcd sends the etcd configuration YAML to the wrapper via
// POST /embedded-etcd. The wrapper writes the config to a file and starts
// the embedded etcd process. It returns nil on 2xx responses.
func (w *HTTPWrapperClient) StartEmbeddedEtcd(ctx context.Context, configYAML []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.baseURL+"/embedded-etcd", bytes.NewReader(configYAML))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-yaml")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST /embedded-etcd: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("POST /embedded-etcd returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// SetReady tells the wrapper that the etcd member is fully initialised and
// ready to serve traffic. The wrapper's /readyz endpoint will start returning
// 200 OK after this call, which causes the Kubernetes readiness probe to pass.
func (w *HTTPWrapperClient) SetReady(ctx context.Context) error {
	body := bytes.NewBufferString("ready")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.baseURL+"/readyz/set", body)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST /readyz/set: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("POST /readyz/set returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// CheckReady polls the wrapper's /readyz endpoint until it returns 200 or the
// context is cancelled. This satisfies the WrapperClient interface.
func (w *HTTPWrapperClient) CheckReady(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.baseURL+"/readyz", nil)
			if err != nil {
				continue
			}
			resp, err := w.client.Do(req)
			if err != nil {
				continue
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
	}
}
