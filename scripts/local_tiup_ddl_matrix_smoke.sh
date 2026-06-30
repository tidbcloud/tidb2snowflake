#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dt%H%M%S)}"
TIDB_VERSION="${TIDB_VERSION:-v8.5.6}"
PORT_OFFSET="${PORT_OFFSET:-23000}"
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-$((4000 + PORT_OFFSET))}"
MYSQL="${MYSQL:-/usr/local/mysql/bin/mysql}"
WORKDIR="${WORKDIR:-$ROOT/.local-ddl-matrix/$RUN_ID}"
FIXTURE="${FIXTURE:-$ROOT/test/fixtures/supported_matrix.sql}"
PLAYGROUND_TAG="${PLAYGROUND_TAG:-tidb2snowflake-ddl-$RUN_ID}"
PLAYGROUND_LOG="$WORKDIR/playground.log"
mkdir -p "$WORKDIR"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

PLAYGROUND_PID=

cleanup() {
  set +e
  if [[ -n "${PLAYGROUND_PID:-}" ]]; then
    kill "$PLAYGROUND_PID" >/dev/null 2>&1
    wait "$PLAYGROUND_PID" >/dev/null 2>&1
  fi
  tiup clean "$PLAYGROUND_TAG" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_tidb() {
  for _ in $(seq 1 90); do
    if "$MYSQL" -h "$TIDB_HOST" -P "$TIDB_PORT" -uroot -e 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  die "TiDB did not become ready on $TIDB_HOST:$TIDB_PORT"
}

main() {
  need tiup
  need go
  [[ -x "$MYSQL" ]] || die "mysql client not executable: $MYSQL"
  [[ -f "$FIXTURE" ]] || die "fixture not found: $FIXTURE"

  log "starting TiUP playground $TIDB_VERSION"
  tiup playground "$TIDB_VERSION" --tag "$PLAYGROUND_TAG" --db 1 --pd 1 --kv 1 \
    --tiflash 0 --without-monitor --port-offset "$PORT_OFFSET" >"$PLAYGROUND_LOG" 2>&1 &
  PLAYGROUND_PID=$!
  wait_for_tidb

  log "loading supported matrix fixture"
  "$MYSQL" -h "$TIDB_HOST" -P "$TIDB_PORT" -uroot <"$FIXTURE"

  log "running metadata/type/DDL assertions"
  TIDB_HOST="$TIDB_HOST" TIDB_PORT="$TIDB_PORT" go run -ldflags=-checklinkname=0 "$ROOT/scripts/local_tiup_ddl_matrix_probe"

  log "success"
  log "logs are in $WORKDIR"
}

main "$@"
