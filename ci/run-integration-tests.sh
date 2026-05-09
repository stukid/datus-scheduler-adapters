#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE_FILE="datus-scheduler-airflow/tests/integration/docker-compose.yml"
TEST_PATH="datus-scheduler-airflow/tests/integration"
DOCKER_COMPOSE=()

usage() {
  cat <<'USAGE'
Usage: ci/run-integration-tests.sh [--list] [--dry-run]
       ci/run-integration-tests.sh --cleanup-only

Runs Docker-backed Airflow scheduler adapter integration tests.

Options:
  --list           List configured adapter target.
  --dry-run        Print resolved target and fixture env without starting Docker.
  --cleanup-only   Stop the integration compose project.
  -h, --help       Show this help.
USAGE
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name" >&2
    exit 127
  fi
}

detect_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE=(docker compose)
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1 && docker-compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE=(docker-compose)
    return 0
  fi
  return 1
}

install_docker_compose() {
  local version="${DOCKER_COMPOSE_VERSION:-v2.32.4}"
  local os
  local machine
  local arch
  local bin_dir
  local bin_path
  local url

  if ! command -v curl >/dev/null 2>&1; then
    echo "Missing required command: curl; cannot install Docker Compose." >&2
    return 1
  fi

  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$os" in
    linux|darwin) ;;
    *)
      echo "Unsupported OS for automatic Docker Compose install: $os" >&2
      return 1
      ;;
  esac

  machine="$(uname -m)"
  case "$machine" in
    x86_64|amd64) arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *)
      echo "Unsupported architecture for automatic Docker Compose install: $machine" >&2
      return 1
      ;;
  esac

  bin_dir="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/datus-docker-compose"
  bin_path="$bin_dir/docker-compose-$version-$os-$arch"
  url="https://github.com/docker/compose/releases/download/$version/docker-compose-$os-$arch"

  mkdir -p "$bin_dir"
  if [ ! -x "$bin_path" ]; then
    echo "Installing Docker Compose $version to $bin_path"
    curl -fsSL --retry 3 -o "$bin_path" "$url"
    chmod +x "$bin_path"
  fi

  DOCKER_COMPOSE=("$bin_path")
}

ensure_docker_compose() {
  detect_docker_compose || install_docker_compose
}

docker_compose() {
  if [ "${#DOCKER_COMPOSE[@]}" -eq 0 ]; then
    if ! ensure_docker_compose; then
      echo "Docker Compose is not available through 'docker compose' or 'docker-compose'." >&2
      return 127
    fi
  fi
  "${DOCKER_COMPOSE[@]}" "$@"
}

preflight() {
  require_command uv
  require_command docker
  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is not reachable. Start Docker and retry." >&2
    exit 1
  fi
  if ! ensure_docker_compose; then
    echo "Docker Compose is not available through 'docker compose' or 'docker-compose'." >&2
    exit 1
  fi
}

compose_down() {
  docker_compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}

default_dags_dir="${RUNNER_TEMP:-$ROOT_DIR/.tmp}/datus-scheduler-airflow-dags"

export AIRFLOW_HOST_PORT="${AIRFLOW_HOST_PORT:-18080}"
export AIRFLOW_URL="${AIRFLOW_URL:-http://127.0.0.1:${AIRFLOW_HOST_PORT}/api/v1}"
export AIRFLOW_USER="${AIRFLOW_USER:-admin}"
export AIRFLOW_USERNAME="${AIRFLOW_USERNAME:-$AIRFLOW_USER}"
export AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-admin}"
export AIRFLOW_DAGS_DIR="${AIRFLOW_DAGS_DIR:-$default_dags_dir}"

prepare_dags_dir() {
  mkdir -p "$AIRFLOW_DAGS_DIR"
  chmod 0777 "$AIRFLOW_DAGS_DIR"
}

compose_up_with_retry() {
  local attempts="${AIRFLOW_BOOTSTRAP_ATTEMPTS:-3}"
  local attempt=1
  local sleep_seconds

  while [ "$attempt" -le "$attempts" ]; do
    echo "Starting Airflow compose stack (attempt $attempt/$attempts)"
    compose_down
    prepare_dags_dir

    if docker_compose -f "$COMPOSE_FILE" up -d --build; then
      return 0
    fi

    if [ "$attempt" -eq "$attempts" ]; then
      echo "Airflow compose bootstrap failed after $attempts attempts." >&2
      return 1
    fi

    sleep_seconds=$((attempt * 10))
    echo "Airflow compose bootstrap failed; retrying in ${sleep_seconds}s." >&2
    compose_down
    sleep "$sleep_seconds"
    attempt=$((attempt + 1))
  done
}

dry_run=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --cleanup-only)
      compose_down
      exit 0
      ;;
    --list)
      printf '%s\t%s\t%s\n' "airflow" "$COMPOSE_FILE" "$TEST_PATH"
      exit 0
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ "$dry_run" -eq 1 ]; then
  echo "=== Airflow integration tests ==="
  echo "compose: $COMPOSE_FILE"
  echo "tests: $TEST_PATH"
  echo "service: airflow:900"
  echo "AIRFLOW_URL=$AIRFLOW_URL"
  echo "AIRFLOW_DAGS_DIR=$AIRFLOW_DAGS_DIR"
  exit 0
fi

preflight
trap compose_down EXIT

wait_for_service_health() {
  local service_name="$1"
  local timeout_seconds="$2"
  local container_id=""
  local status=""
  local deadline=$((SECONDS + timeout_seconds))

  container_id="$(docker_compose -f "$COMPOSE_FILE" ps -q "$service_name")"
  if [ -z "$container_id" ]; then
    echo "No container found for service '$service_name' in $COMPOSE_FILE" >&2
    docker_compose -f "$COMPOSE_FILE" ps || true
    return 1
  fi

  while [ "$SECONDS" -lt "$deadline" ]; do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || echo unknown)"
    if [ "$status" = "healthy" ] || [ "$status" = "running" ]; then
      echo "Service '$service_name' is $status"
      return 0
    fi
    sleep 5
  done

  echo "Timed out waiting for service '$service_name' from $COMPOSE_FILE" >&2
  docker_compose -f "$COMPOSE_FILE" ps || true
  docker_compose -f "$COMPOSE_FILE" logs --tail=200 || true
  return 1
}

echo "=== Airflow integration tests ==="
compose_up_with_retry
wait_for_service_health airflow 900

uv run --with pytest --with pytest-asyncio --package datus-scheduler-airflow pytest "$TEST_PATH" -m integration --tb=short --verbose
