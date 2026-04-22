#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-etcd-steward-e2e}"
STEWARD_IMAGE="${STEWARD_IMAGE:-localhost/etcd-steward:latest}"
ETCD_IMAGE="${ETCD_IMAGE:-gcr.io/etcd-development/etcd:v3.5.12}"

echo "==> Creating KIND cluster '${CLUSTER_NAME}'..."
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  kind create cluster --name "${CLUSTER_NAME}" --wait 60s
else
  echo "    Cluster '${CLUSTER_NAME}' already exists, reusing."
fi

echo "==> Building etcd-steward binary..."
cd "${SOURCE_DIR}"
CGO_ENABLED=0 GOOS=linux GOARCH="$(go env GOARCH)" go build -o bin/etcd-steward-linux ./cmd/etcdsteward/

echo "==> Building Docker image '${STEWARD_IMAGE}'..."
docker build -f Dockerfile.e2e -t "${STEWARD_IMAGE}" "${SOURCE_DIR}"

echo "==> Loading image into KIND cluster..."
kind load docker-image "${STEWARD_IMAGE}" --name "${CLUSTER_NAME}"

echo "==> Pulling etcd image if needed..."
if ! docker image inspect "${ETCD_IMAGE}" >/dev/null 2>&1; then
  docker pull "${ETCD_IMAGE}"
fi
kind load docker-image "${ETCD_IMAGE}" --name "${CLUSTER_NAME}"

echo "==> Running e2e tests..."
export KUBECONFIG="$(kind get kubeconfig-path 2>/dev/null || echo "${HOME}/.kube/config")"
export STEWARD_IMAGE="${STEWARD_IMAGE}"
export ETCD_IMAGE="${ETCD_IMAGE}"

cd "${SOURCE_DIR}"
go test -count=1 -v -tags=e2e -timeout=15m ./test/e2e/...

echo "==> E2E tests completed successfully."
