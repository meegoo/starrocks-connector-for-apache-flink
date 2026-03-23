#!/usr/bin/env bash
#
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Run Flink connector integration tests against an external StarRocks FE (e.g. TSP).
# Intended for use on a host that has network access to the FE (e.g. after ssh to the remote node).
#
# Usage:
#   ./scripts/run-it-against-tsp.sh <fe_host> [flink_minor_version]
#
# Examples:
#   ./scripts/run-it-against-tsp.sh 172.26.95.50 1.20
#
#   # Branches whose IT base only reads -Dit.starrocks.fe.http / -Dit.starrocks.fe.jdbc:
#   IT_USE_LEGACY_FE_PROPS=1 ./scripts/run-it-against-tsp.sh 172.26.95.50 1.20
#
# Optional environment:
#   SKIP_SDK_TESTS=1   — mvn install stream-load-sdk with -DskipTests
#   SKIP_KAFKA_IT=1    — exclude KafkaToStarRocksITTest (default: 1; needs Docker if unset)
#   SR_USERNAME, SR_PASSWORD — FE auth (defaults: root / empty); legacy props use -Dit.starrocks.username/password when set
#   CUSTOM_MVN         — same as build.sh (e.g. "mvn -B -ntp")
#
set -eo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

FE_HOST="${1:?Usage: $0 <fe_host> [flink_minor_version e.g. 1.20]}"
FLINK_MINOR="${2:-1.20}"

# shellcheck source=common.sh
source "$REPO_ROOT/common.sh"

check_flink_version_supported "$FLINK_MINOR"
FLINK_VERSION="$(get_flink_version "$FLINK_MINOR")"
KAFKA_CV="$(get_kafka_connector_version "$FLINK_MINOR")"

SDK_SKIP=()
if [[ "${SKIP_SDK_TESTS:-0}" == "1" ]]; then
  SDK_SKIP=(-DskipTests)
fi

(
  cd "$REPO_ROOT/starrocks-stream-load-sdk"
  # shellcheck disable=SC2086
  ${MVN_CMD} -B -ntp -Dsurefire.forkCount=2 clean install "${SDK_SKIP[@]}"
)

MVN_ARGS=(
  -B -ntp clean package -DskipTests=false
  "-Dflink.minor.version=${FLINK_MINOR}"
  "-Dflink.version=${FLINK_VERSION}"
  "-Dkafka.connector.version=${KAFKA_CV}"
)

if [[ "${IT_USE_LEGACY_FE_PROPS:-0}" == "1" ]]; then
  MVN_ARGS+=(
    "-Dit.starrocks.fe.http=${FE_HOST}:8030"
    "-Dit.starrocks.fe.jdbc=jdbc:mysql://${FE_HOST}:9030?useSSL=false"
  )
  if [[ -n "${SR_USERNAME:-}" ]]; then
    MVN_ARGS+=("-Dit.starrocks.username=${SR_USERNAME}")
  fi
  if [[ -n "${SR_PASSWORD:-}" ]]; then
    MVN_ARGS+=("-Dit.starrocks.password=${SR_PASSWORD}")
  fi
else
  export SR_HTTP_URLS="${FE_HOST}:8030"
  export SR_JDBC_URLS="jdbc:mysql://${FE_HOST}:9030?useSSL=false"
  export SR_USERNAME="${SR_USERNAME:-root}"
  export SR_PASSWORD="${SR_PASSWORD:-}"
fi

if [[ "${SKIP_KAFKA_IT:-1}" == "1" ]]; then
  MVN_ARGS+=("-Dsurefire.excludes=**/KafkaToStarRocksITTest.java")
fi

# shellcheck disable=SC2086
${MVN_CMD} "${MVN_ARGS[@]}"
