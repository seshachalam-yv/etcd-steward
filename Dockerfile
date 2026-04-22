# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Build stage
FROM golang:1.24 AS builder

ARG TARGETARCH=arm64

WORKDIR /go/src/github.com/gardener/etcd-steward
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -o /etcd-steward \
    ./cmd/etcdsteward/

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /etcd-steward /etcd-steward

ENTRYPOINT ["/etcd-steward"]
