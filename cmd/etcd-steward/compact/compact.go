// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package compact provides the etcd-steward compact subcommand.
package compact

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Run executes the compact subcommand.
// Usage: etcd-steward compact --etcd-endpoint=<url> --revision=<N>
func Run(logger *zap.Logger, endpoint string, revision int64) error {
	if endpoint == "" {
		endpoint = os.Getenv("ETCD_ENDPOINT")
	}
	if endpoint == "" {
		return fmt.Errorf("--etcd-endpoint is required")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer client.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger.Info("compacting etcd",
		zap.String("endpoint", endpoint),
		zap.Int64("revision", revision),
	)

	_, err = client.Compact(ctx, revision, clientv3.WithCompactPhysical())
	if err != nil {
		return fmt.Errorf("compact failed: %w", err)
	}

	logger.Info("compaction completed successfully")
	return nil
}

// ParseArgs parses compact subcommand arguments from os.Args.
func ParseArgs(args []string) (endpoint string, revision int64, err error) {
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--etcd-endpoint" && i+1 < len(args):
			endpoint = args[i+1]
			i++
		case args[i] == "--revision" && i+1 < len(args):
			revision, err = strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return "", 0, fmt.Errorf("invalid revision: %w", err)
			}
			i++
		}
	}
	return endpoint, revision, nil
}
