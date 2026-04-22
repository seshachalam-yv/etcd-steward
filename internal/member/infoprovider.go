// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"

	"github.com/gardener/etcd-steward/internal/errors"
)

// MaintenanceStatusProvider implements MemberInfoProvider by querying the etcd
// maintenance status endpoint.
type MaintenanceStatusProvider struct {
	podName  string
	endpoint string
	status   StatusClient
}

// StatusClient abstracts the etcd maintenance status query.
type StatusClient interface {
	// Status returns the status of the etcd member at the given endpoint.
	Status(ctx context.Context, endpoint string) (StatusResponse, error)
}

// StatusResponse holds the response from an etcd maintenance status query.
type StatusResponse struct {
	// MemberID is the unique member ID within the cluster.
	MemberID uint64
	// Leader is the ID of the current cluster leader.
	Leader uint64
	// DBSize is the total allocated database size in bytes.
	DBSize int64
	// DBSizeInUse is the in-use database size in bytes.
	DBSizeInUse int64
	// IsLearner indicates whether this member is a learner.
	IsLearner bool
}

// NewMaintenanceStatusProvider creates a MemberInfoProvider that queries the
// given endpoint via the StatusClient.
func NewMaintenanceStatusProvider(podName, endpoint string, status StatusClient) *MaintenanceStatusProvider {
	return &MaintenanceStatusProvider{
		podName:  podName,
		endpoint: endpoint,
		status:   status,
	}
}

// MemberInfo queries the etcd maintenance status and maps the result to a
// MemberInfo struct.
func (p *MaintenanceStatusProvider) MemberInfo(ctx context.Context) (MemberInfo, error) {
	resp, err := p.status.Status(ctx, p.endpoint)
	if err != nil {
		return MemberInfo{}, errors.Wrap(errors.ErrCodeEtcd, "failed to get status from "+p.endpoint, err)
	}

	role := "Follower"
	if resp.IsLearner {
		role = "Learner"
	} else if resp.Leader == resp.MemberID {
		role = "Leader"
	}

	return MemberInfo{
		ID:          resp.MemberID,
		Name:        p.podName,
		Role:        role,
		DBSize:      resp.DBSize,
		DBSizeInUse: resp.DBSizeInUse,
		IsHealthy:   true,
	}, nil
}

// Compile-time assertion that MaintenanceStatusProvider implements MemberInfoProvider.
var _ MemberInfoProvider = (*MaintenanceStatusProvider)(nil)
