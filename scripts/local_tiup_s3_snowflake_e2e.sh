#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dt%H%M%S)}"

TIDB_VERSION="${TIDB_VERSION:-v8.5.6}"
PORT_OFFSET="${PORT_OFFSET:-21000}"
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-$((4000 + PORT_OFFSET))}"
PD_ADDR="${PD_ADDR:-http://127.0.0.1:$((2379 + PORT_OFFSET))}"
CDC_ADDR="${CDC_ADDR:-127.0.0.1:$((8300 + PORT_OFFSET))}"
MYSQL="${MYSQL:-/usr/local/mysql/bin/mysql}"

SOURCE_DB="${SOURCE_DB:-tidb2snowflake_local}"
SOURCE_TABLE="${SOURCE_TABLE:-t_${RUN_ID//[^0-9A-Za-z_]/_}}"
TABLE_FQN="$SOURCE_DB.$SOURCE_TABLE"
SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-TIDB2SNOWFLAKE_E2E}"
SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-LOCAL_${RUN_ID//[^0-9A-Za-z_]/_}}"
CHANGEFEED_ID="${CHANGEFEED_ID:-tidb2sf-${RUN_ID//[^0-9A-Za-z-]/-}}"
WORKDIR="${WORKDIR:-$ROOT/.local-e2e/$RUN_ID}"
SNAPSHOT_COMPRESSION="${SNAPSHOT_COMPRESSION:-none}"
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

extract_json_value() {
  local key="$1"
  sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" "$ENV_FILE" | head -1
}

extract_snowflake_account() {
  awk '
    /^Account identifier[[:space:]]*$/ { getline; if (NF) { print $1; exit } }
    /^Account identifier[[:space:]]+/ { print $NF; exit }
    /snowflakecomputing[.]com/ { sub(/[.]snowflakecomputing[.]com.*/, "", $1); print $1; exit }
  ' "$ENV_FILE"
}

extract_snowflake_user() {
  awk '
    /^Login name[[:space:]]*$/ { getline; if (NF) { print $1; exit } }
    /^Login name[[:space:]]+/ { print $NF; exit }
    found == 1 && NF { print $1; exit }
    /snowflakecomputing[.]com/ { found = 1 }
  ' "$ENV_FILE"
}

extract_snowflake_pass() {
  awk '
    /^密码是/ { sub(/^密码是[[:space:]]*/, ""); print; exit }
    /^Password[[:space:]]*$/ { getline; print; exit }
    /^Password[[:space:]]+/ { sub(/^Password[[:space:]]*/, ""); print; exit }
    found == 2 && NF { print $0; exit }
    /snowflakecomputing[.]com/ { found = 1; next }
    found == 1 && NF { found = 2; next }
  ' "$ENV_FILE"
}

extract_snowflake_warehouse() {
  awk '
    tolower($0) ~ /warehouse/ {
      line = $0
      sub(/^.*[Ww]arehouse[[:space:]]*/, "", line)
      sub(/^是[[:space:]]*/, "", line)
      sub(/^=[[:space:]]*/, "", line)
      print line
      exit
    }
  ' "$ENV_FILE"
}

load_env_file() {
  [[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE"

  STORAGE_URI="${STORAGE_URI:-$(extract_json_value storage_uri)}"
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$(extract_json_value aws_access_key_id)}"
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$(extract_json_value aws_secret_access_key)}"
  AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-$(extract_json_value aws_session_token)}"

  SNOWFLAKE_ACCOUNT_ID="${SNOWFLAKE_ACCOUNT_ID:-$(extract_snowflake_account)}"
  SNOWFLAKE_USER="${SNOWFLAKE_USER:-$(extract_snowflake_user)}"
  SNOWFLAKE_PASS="${SNOWFLAKE_PASS:-$(extract_snowflake_pass)}"
  SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-$(extract_snowflake_warehouse)}"

  [[ -n "${STORAGE_URI:-}" ]] || die "STORAGE_URI is empty"
  [[ -n "${AWS_ACCESS_KEY_ID:-}" ]] || die "AWS_ACCESS_KEY_ID is empty"
  [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]] || die "AWS_SECRET_ACCESS_KEY is empty"
  [[ -n "${SNOWFLAKE_ACCOUNT_ID:-}" ]] || die "SNOWFLAKE_ACCOUNT_ID is empty"
  [[ -n "${SNOWFLAKE_USER:-}" ]] || die "SNOWFLAKE_USER is empty"
  [[ -n "${SNOWFLAKE_PASS:-}" ]] || die "SNOWFLAKE_PASS is empty"
  [[ -n "${SNOWFLAKE_WAREHOUSE:-}" ]] || SNOWFLAKE_WAREHOUSE=COMPUTE_WH

  STORAGE_ROOT="${STORAGE_ROOT:-${STORAGE_URI%\?*}/local-$RUN_ID}"
  S3_REGION="${S3_REGION:-$(printf '%s\n' "$STORAGE_URI" | sed -n 's/.*[?&]region=\([^&]*\).*/\1/p')}"
  S3_REGION="${S3_REGION:-us-west-2}"

  export AWS_ACCESS_KEY_ID
  export AWS_SECRET_ACCESS_KEY
  export AWS_SESSION_TOKEN
  export AWS_REGION="$S3_REGION"
  export AWS_DEFAULT_REGION="$S3_REGION"
}

cleanup() {
  set +e
  if [[ -n "${TOOL_PID:-}" ]]; then
    kill "$TOOL_PID" >/dev/null 2>&1
    wait "$TOOL_PID" >/dev/null 2>&1
  fi
  if [[ -n "${PLAYGROUND_PID:-}" ]]; then
    kill "$PLAYGROUND_PID" >/dev/null 2>&1
    wait "$PLAYGROUND_PID" >/dev/null 2>&1
  fi
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

wait_for_cdc() {
  for _ in $(seq 1 90); do
    if tiup cdc cli capture list --server "http://$CDC_ADDR" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  die "TiCDC did not become ready on $CDC_ADDR"
}

wait_for_s3_objects() {
  local prefix="$1"
  local min_count="$2"
  for _ in $(seq 1 120); do
    local count
    count="$((AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
      aws s3 ls "$prefix" --recursive --region "$S3_REGION" 2>/dev/null || true) | wc -l | tr -d ' ')"
    if [[ "$count" -ge "$min_count" ]]; then
      return 0
    fi
    sleep 2
  done
  die "timed out waiting for at least $min_count S3 object(s) under $prefix"
}

write_cdc_config() {
  cat >"$WORKDIR/changefeed.toml" <<TOML
case-sensitive = true

[filter]
rules = ['$SOURCE_DB.*']

[sink]
protocol = 'csv'
date-separator = 'day'
enable-partition-separator = true

[sink.csv]
include-commit-ts = true
binary-encoding-method = 'hex'
delimiter = ','
quote = '"'
null = '\N'
TOML
}

write_snowflake_probe() {
  cat >"$WORKDIR/snowflake_probe.go" <<'GO'
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
)

func main() {
	var table string
	var timeout time.Duration
	flag.StringVar(&table, "table", "", "table name")
	flag.DurationVar(&timeout, "timeout", 5*time.Minute, "poll timeout")
	flag.Parse()
	if table == "" {
		fmt.Fprintln(os.Stderr, "--table is required")
		os.Exit(2)
	}
	cfg := &snowflake.Config{
		AccountId: os.Getenv("SNOWFLAKE_ACCOUNT_ID"),
		Warehouse: os.Getenv("SNOWFLAKE_WAREHOUSE"),
		User:      os.Getenv("SNOWFLAKE_USER"),
		Pass:      os.Getenv("SNOWFLAKE_PASS"),
		Database:  os.Getenv("SNOWFLAKE_DATABASE"),
		Schema:    os.Getenv("SNOWFLAKE_SCHEMA"),
	}
	db, err := cfg.OpenDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		var count int
		var amount sql.NullInt64
		var year sql.NullInt64
		var enumVal sql.NullString
		var vectorVal sql.NullString
		var deleted int
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*), COALESCE(MAX(IFF(id=2, amount, NULL)), -1), COALESCE(MAX(IFF(id=2, c_year, NULL)), -1), COALESCE(MAX(IFF(id=2, c_enum, NULL)), ''), COALESCE(MAX(IFF(id=2, c_vector, NULL)), ''), SUM(IFF(id=3, 1, 0)) FROM %s", table)).Scan(&count, &amount, &year, &enumVal, &vectorVal, &deleted)
		if err == nil {
			last = fmt.Sprintf("count=%d id2_amount=%d id2_year=%d id2_enum=%s id2_vector=%s id3_rows=%d", count, amount.Int64, year.Int64, enumVal.String, vectorVal.String, deleted)
			if count == 4 &&
				amount.Valid && amount.Int64 == 222 &&
				year.Valid && year.Int64 == 2030 &&
				enumVal.Valid && enumVal.String == "large" &&
				vectorVal.Valid && vectorVal.String == "[9,8,7]" &&
				deleted == 0 {
				fmt.Println(last)
				return
			}
		} else {
			last = err.Error()
		}
		time.Sleep(5 * time.Second)
	}
	panic("Snowflake assertion did not pass before timeout; last=" + last)
}
GO
}

main() {
  need tiup
  need aws
  need go
  [[ -x "$MYSQL" ]] || die "mysql client not executable: $MYSQL"
  load_env_file

  log "run id: $RUN_ID"
  log "workdir: $WORKDIR"
  log "storage root: $STORAGE_ROOT"
  log "snowflake database/schema: $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA"
  log "snapshot compression: $SNAPSHOT_COMPRESSION"

  log "building tidb2snowflake"
  make -C "$ROOT" build >/dev/null

  log "starting TiUP playground $TIDB_VERSION with TiCDC"
  tiup playground "$TIDB_VERSION" --tag "tidb2snowflake-$RUN_ID" --db 1 --pd 1 --kv 1 --ticdc 1 \
    --tiflash 0 --without-monitor --port-offset "$PORT_OFFSET" >"$WORKDIR/playground.log" 2>&1 &
  PLAYGROUND_PID=$!
  wait_for_tidb
  wait_for_cdc

  log "seeding TiDB source table $TABLE_FQN"
  "$MYSQL" -h "$TIDB_HOST" -P "$TIDB_PORT" -uroot <<SQL
CREATE DATABASE IF NOT EXISTS $SOURCE_DB;
DROP TABLE IF EXISTS $TABLE_FQN;
CREATE TABLE $TABLE_FQN (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64),
  amount BIGINT,
  c_year YEAR,
  c_enum ENUM('small', 'medium', 'large'),
  c_vector VECTOR(3)
);
INSERT INTO $TABLE_FQN (id, name, amount, c_year, c_enum, c_vector) VALUES
  (1,'a',100,2026,'small','[1,2,3]'),
  (2,'b',200,2027,'medium','[4,5,6]'),
  (3,'c',300,2028,'large','[7,8,9]');
SQL

  log "clearing S3 run prefix"
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    aws s3 rm "$STORAGE_ROOT" --recursive --region "$S3_REGION" >/dev/null 2>&1 || true

  log "dumping snapshot to S3"
  dumpling_extra_args=()
  if [[ "$SNAPSHOT_COMPRESSION" == "gzip" ]]; then
    dumpling_extra_args+=(--compress gzip)
  elif [[ "$SNAPSHOT_COMPRESSION" != "none" ]]; then
    die "unsupported SNAPSHOT_COMPRESSION: $SNAPSHOT_COMPRESSION"
  fi
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    tiup dumpling -h "$TIDB_HOST" -P "$TIDB_PORT" -u root \
      --filetype csv --no-header --no-schemas --csv-output-dialect snowflake --escape-backslash=false \
      --tables-list "$TABLE_FQN" --output "$STORAGE_ROOT/snapshot" --s3.region "$S3_REGION" \
      --output-filename-template '{{.DB}}.{{.Table}}.{{.Index}}' ${dumpling_extra_args[@]+"${dumpling_extra_args[@]}"} >"$WORKDIR/dumpling.log" 2>&1
  wait_for_s3_objects "$STORAGE_ROOT/snapshot" 1

  log "creating TiCDC changefeed to S3"
  write_cdc_config
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    tiup cdc cli changefeed create --server "http://$CDC_ADDR" --changefeed-id "$CHANGEFEED_ID" \
      --sink-uri "$STORAGE_ROOT/increment?protocol=csv&s3.region=$S3_REGION" \
      --config "$WORKDIR/changefeed.toml" --no-confirm >"$WORKDIR/changefeed-create.log" 2>&1

  log "applying incremental DMLs"
  "$MYSQL" -h "$TIDB_HOST" -P "$TIDB_PORT" -uroot <<SQL
INSERT INTO $TABLE_FQN (id, name, amount, c_year, c_enum, c_vector) VALUES
  (4,'d',400,2029,'small','[0.1,0.2,0.3]'),
  (5,'e',500,2031,'medium','[-1,0,1]');
UPDATE $TABLE_FQN SET amount = 222, c_year = 2030, c_enum = 'large', c_vector = '[9,8,7]' WHERE id = 2;
DELETE FROM $TABLE_FQN WHERE id = 3;
SQL
  wait_for_s3_objects "$STORAGE_ROOT/increment/$SOURCE_DB/$SOURCE_TABLE" 2

  log "starting tidb2snowflake full loader"
  "$ROOT/bin/tidb2snowflake" snowflake \
    --mode full \
    --tidb.host "$TIDB_HOST" --tidb.port "$TIDB_PORT" --tidb.user root \
    --snowflake.account-id "$SNOWFLAKE_ACCOUNT_ID" \
    --snowflake.user "$SNOWFLAKE_USER" \
    --snowflake.pass "$SNOWFLAKE_PASS" \
    --snowflake.warehouse "$SNOWFLAKE_WAREHOUSE" \
    --snowflake.database "$SNOWFLAKE_DATABASE" \
    --snowflake.schema "$SNOWFLAKE_SCHEMA" \
    --storage "$STORAGE_ROOT" \
    --aws.access-key "$AWS_ACCESS_KEY_ID" \
    --aws.secret-key "$AWS_SECRET_ACCESS_KEY" \
    --table "$TABLE_FQN" \
    --snapshot.compression "$SNAPSHOT_COMPRESSION" \
    --changefeed.flush-interval 10s \
    --log.level info >"$WORKDIR/tidb2snowflake.log" 2>&1 &
  TOOL_PID=$!

  log "waiting for Snowflake assertion"
  write_snowflake_probe
  SNOWFLAKE_ACCOUNT_ID="$SNOWFLAKE_ACCOUNT_ID" \
  SNOWFLAKE_USER="$SNOWFLAKE_USER" \
  SNOWFLAKE_PASS="$SNOWFLAKE_PASS" \
  SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE" \
  SNOWFLAKE_DATABASE="$SNOWFLAKE_DATABASE" \
  SNOWFLAKE_SCHEMA="$SNOWFLAKE_SCHEMA" \
    go run -ldflags=-checklinkname=0 "$WORKDIR/snowflake_probe.go" --table "$SOURCE_TABLE" --timeout 8m

  log "success"
  log "logs are in $WORKDIR"
}

main "$@"
