// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"

	"github.com/gardener/etcd-steward/internal/errors"
)

// Validate checks the Config for mandatory fields and value constraints.
// It returns a non-nil *errors.Error on the first validation failure.
func Validate(cfg *Config) *errors.Error {
	if cfg.PodName == "" {
		return errors.New(errors.ErrCodeMissingRequired, "pod-name is required but was empty")
	}
	if cfg.PodNamespace == "" {
		return errors.New(errors.ErrCodeMissingRequired, "pod-namespace is required but was empty")
	}
	if len(cfg.EtcdEndpoints) == 0 {
		return errors.New(errors.ErrCodeMissingRequired, "etcd-endpoints must contain at least one endpoint")
	}
	if cfg.ServerPort < 1 || cfg.ServerPort > 65535 {
		return errors.New(errors.ErrCodeInvalidConfig,
			fmt.Sprintf("server-port must be between 1 and 65535, got %d", cfg.ServerPort))
	}
	if cfg.FullSnapshotInterval < cfg.DeltaSnapshotInterval {
		return errors.New(errors.ErrCodeInvalidConfig,
			fmt.Sprintf("full-snapshot-interval (%s) must not be less than delta-snapshot-interval (%s)",
				cfg.FullSnapshotInterval, cfg.DeltaSnapshotInterval))
	}
	return nil
}
