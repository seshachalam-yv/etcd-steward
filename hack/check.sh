#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0



set -e

GOLANGCI_LINT_CONFIG_FILE=""
GOLANGCI_LINT_CONFIG_PATH=""

for arg in "$@"; do
  case $arg in
    --golangci-lint-config=*)
    GOLANGCI_LINT_CONFIG_PATH="${arg#*=}"
    GOLANGCI_LINT_CONFIG_FILE="-c"
    shift
    ;;
  esac
done

echo "> Check"

echo "Executing golangci-lint"
golangci-lint run ${GOLANGCI_LINT_CONFIG_FILE} ${GOLANGCI_LINT_CONFIG_PATH} --timeout 10m "$@"
