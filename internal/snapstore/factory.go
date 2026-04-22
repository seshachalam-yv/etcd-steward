// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore

import (
	"fmt"
)

const (
	// ProviderLocal is the provider name for local filesystem storage.
	ProviderLocal = "Local"
	// ProviderS3 is the provider name for AWS S3 storage.
	ProviderS3 = "S3"
	// ProviderGCS is the provider name for Google Cloud Storage.
	ProviderGCS = "GCS"
	// ProviderABS is the provider name for Azure Blob Storage.
	ProviderABS = "ABS"
)

// NewSnapStore creates a SnapStore for the given provider.
// provider must be one of "Local", "S3", "GCS", or "ABS".
// prefix is the key prefix under which snapshots are stored.
// container is the bucket/container name (or root directory for Local).
// config holds provider-specific key-value settings (e.g. region, credentials path).
func NewSnapStore(provider, prefix, container string, config map[string]string) (SnapStore, error) {
	switch provider {
	case ProviderLocal:
		return NewLocal(container)
	case ProviderS3:
		return NewS3(container, prefix, config)
	case ProviderGCS:
		return NewGCS(container, prefix, config)
	case ProviderABS:
		return NewABS(container, prefix, config)
	default:
		return nil, fmt.Errorf("unknown snapstore provider %q", provider)
	}
}
