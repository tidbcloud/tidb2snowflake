#!/usr/bin/env bash
set -euo pipefail

MYSQL_HOST=""
MYSQL_PORT=""
MYSQL_DATABASE=""
MYSQL_USER=""
MYSQL_PASSWORD=""
MYSQL_CLIENT="${MYSQL_CLIENT:-mysql}"

SNOWFLAKE_ACCOUNT=""
SNOWFLAKE_USER=""
SNOWFLAKE_PASSWORD=""
SNOWFLAKE_DATABASE=""
SNOWFLAKE_SCHEMA=""
SNOWFLAKE_WAREHOUSE=""
SNOWSQL_CLIENT="${SNOWSQL_CLIENT:-snowsql}"

CASE_SENSITIVE=0
VERBOSE=0
TMP_DIR=""

usage() {
  cat <<'USAGE'
Compare row counts between all MySQL tables in one database and all Snowflake
tables in one database/schema.

Required:
  --mysql-host HOST
  --mysql-port PORT
  --mysql-database DATABASE
  --mysql-user USER
  --mysql-password PASSWORD
  --snowflake-account ACCOUNT_ID
  --snowflake-user USER
  --snowflake-password PASSWORD
  --snowflake-database DATABASE
  --snowflake-schema SCHEMA

Optional:
  --mysql-client PATH          Defaults to mysql, or MYSQL_CLIENT if set.
  --snowsql-client PATH        Defaults to snowsql, or SNOWSQL_CLIENT if set.
  --warehouse NAME             Snowflake warehouse to activate for count queries.
  --snowflake-warehouse NAME   Uses the Snowflake user's default if omitted.
  --case-sensitive             Match table names exactly instead of uppercasing.
  --verbose                    Print row counts for every compared table.
  -h, --help                   Show this help.

Output:
  pass

or:
  failed
  table	mysql_count	snowflake_count
  TABLE_A	10	12
USAGE
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}

cleanup() {
  if [[ -n "${TMP_DIR:-}" ]]; then
    rm -rf "$TMP_DIR"
  fi
}

cancel() {
  trap - INT TERM
  cleanup
  exit 130
}

trap cleanup EXIT
trap cancel INT TERM

require_arg() {
  local flag="$1"
  local count="$2"
  [[ "$count" -ge 2 ]] || die "$flag requires a value"
}

set_option() {
  local key="$1"
  local value="$2"

  case "$key" in
    --mysql-host) MYSQL_HOST="$value" ;;
    --mysql-port) MYSQL_PORT="$value" ;;
    --mysql-database) MYSQL_DATABASE="$value" ;;
    --mysql-user) MYSQL_USER="$value" ;;
    --mysql-password) MYSQL_PASSWORD="$value" ;;
    --mysql-client) MYSQL_CLIENT="$value" ;;
    --snowflake-account) SNOWFLAKE_ACCOUNT="$value" ;;
    --snowflake-user) SNOWFLAKE_USER="$value" ;;
    --snowflake-password) SNOWFLAKE_PASSWORD="$value" ;;
    --snowflake-database) SNOWFLAKE_DATABASE="$value" ;;
    --snowflake-schema) SNOWFLAKE_SCHEMA="$value" ;;
    --warehouse|--snowflake-warehouse) SNOWFLAKE_WAREHOUSE="$value" ;;
    --snowsql-client) SNOWSQL_CLIENT="$value" ;;
    *) die "unknown option: $key" ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysql-host=*|--mysql-port=*|--mysql-database=*|--mysql-user=*|--mysql-password=*|--mysql-client=*|--snowflake-account=*|--snowflake-user=*|--snowflake-password=*|--snowflake-database=*|--snowflake-schema=*|--warehouse=*|--snowflake-warehouse=*|--snowsql-client=*)
      set_option "${1%%=*}" "${1#*=}"
      shift
      ;;
    --mysql-host|--mysql-port|--mysql-database|--mysql-user|--mysql-password|--mysql-client|--snowflake-account|--snowflake-user|--snowflake-password|--snowflake-database|--snowflake-schema|--warehouse|--snowflake-warehouse|--snowsql-client)
      require_arg "$1" "$#"
      set_option "$1" "$2"
      shift 2
      ;;
    --case-sensitive)
      CASE_SENSITIVE=1
      shift
      ;;
    --verbose)
      VERBOSE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

require_non_empty() {
  local name="$1"
  local value="$2"
  [[ -n "$value" ]] || die "$name is required"
}

validate_args() {
  require_non_empty "--mysql-host" "$MYSQL_HOST"
  require_non_empty "--mysql-port" "$MYSQL_PORT"
  require_non_empty "--mysql-database" "$MYSQL_DATABASE"
  require_non_empty "--mysql-user" "$MYSQL_USER"
  require_non_empty "--mysql-password" "$MYSQL_PASSWORD"
  require_non_empty "--snowflake-account" "$SNOWFLAKE_ACCOUNT"
  require_non_empty "--snowflake-user" "$SNOWFLAKE_USER"
  require_non_empty "--snowflake-password" "$SNOWFLAKE_PASSWORD"
  require_non_empty "--snowflake-database" "$SNOWFLAKE_DATABASE"
  require_non_empty "--snowflake-schema" "$SNOWFLAKE_SCHEMA"

  [[ "$MYSQL_PORT" =~ ^[0-9]+$ ]] || die "--mysql-port must be numeric"
  command -v "$MYSQL_CLIENT" >/dev/null 2>&1 || die "mysql client not found: $MYSQL_CLIENT"
  command -v "$SNOWSQL_CLIENT" >/dev/null 2>&1 || die "snowsql client not found: $SNOWSQL_CLIENT"
}

sql_literal() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/''/g")"
}

mysql_ident() {
  printf '`%s`' "$(printf '%s' "$1" | sed 's/`/``/g')"
}

snowflake_ident() {
  printf '"%s"' "$(printf '%s' "$1" | sed 's/"/""/g')"
}

uppercase() {
  printf '%s' "$1" | LC_ALL=C tr '[:lower:]' '[:upper:]'
}

normalize_name() {
  if [[ "$CASE_SENSITIVE" -eq 1 ]]; then
    printf '%s' "$1"
  else
    uppercase "$1"
  fi
}

snowflake_object_name() {
  if [[ "$CASE_SENSITIVE" -eq 1 ]]; then
    printf '%s' "$1"
  else
    uppercase "$1"
  fi
}

clean_single_column_output() {
  sed \
    -e 's/\r$//' \
    -e '/^[[:space:]]*$/d' \
    -e 's/^[[:space:]]*//' \
    -e 's/[[:space:]]*$//' \
    -e 's/^"//' \
    -e 's/"$//' \
    -e 's/""/"/g'
}

run_mysql() {
  local query="$1"
  MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CLIENT" \
    -h "$MYSQL_HOST" \
    -P "$MYSQL_PORT" \
    -u "$MYSQL_USER" \
    --batch \
    --raw \
    --skip-column-names \
    --default-character-set=utf8mb4 \
    -e "$query"
}

run_snowsql() {
  local query="$1"
  local sf_database
  local sf_schema
  sf_database="$(snowflake_object_name "$SNOWFLAKE_DATABASE")"
  sf_schema="$(snowflake_object_name "$SNOWFLAKE_SCHEMA")"

  local args=(
    -a "$SNOWFLAKE_ACCOUNT"
    -u "$SNOWFLAKE_USER"
    -d "$sf_database"
    -s "$sf_schema"
    -o output_format=csv
    -o header=false
    -o friendly=false
    -o timing=false
    -o exit_on_error=true
    -q "$query"
  )

  if [[ -n "$SNOWFLAKE_WAREHOUSE" ]]; then
    args+=(-w "$SNOWFLAKE_WAREHOUSE")
  fi

  SNOWSQL_PWD="$SNOWFLAKE_PASSWORD" "$SNOWSQL_CLIENT" "${args[@]}"
}

fetch_mysql_tables() {
  local query
  query="SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = $(sql_literal "$MYSQL_DATABASE") AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;"
  run_mysql "$query" | clean_single_column_output
}

fetch_snowflake_tables() {
  local sf_database
  local sf_schema
  local query
  sf_database="$(snowflake_object_name "$SNOWFLAKE_DATABASE")"
  sf_schema="$(snowflake_object_name "$SNOWFLAKE_SCHEMA")"
  query="SELECT TABLE_NAME FROM $(snowflake_ident "$sf_database").INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = $(sql_literal "$sf_database") AND TABLE_SCHEMA = $(sql_literal "$sf_schema") AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;"
  run_snowsql "$query" | clean_single_column_output
}

first_value() {
  clean_single_column_output | head -n 1
}

require_count() {
  local label="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "expected numeric row count for $label, got: ${value:-<empty>}"
}

write_table_keys() {
  local tables_file="$1"
  local table
  local key

  while IFS= read -r table; do
    [[ -n "$table" ]] || continue
    key="$(normalize_name "$table")"
    printf '%s\t%s\n' "$key" "$table"
  done <"$tables_file"
}

check_duplicate_keys() {
  local file="$1"
  local label="$2"
  local duplicate
  duplicate="$(awk -F '\t' 'seen[$1]++ { print $1; exit }' "$file")"
  if [[ -n "$duplicate" ]]; then
    die "$label has multiple tables that normalize to $duplicate; rerun with --case-sensitive"
  fi
}

lookup_table_name() {
  local key="$1"
  local file="$2"

  awk -F '\t' -v lookup_key="$key" '$1 == lookup_key { print $2; exit }' "$file"
}

count_mysql_table() {
  local table="$1"
  local count
  local query

  query="SELECT COUNT(*) FROM $(mysql_ident "$MYSQL_DATABASE").$(mysql_ident "$table");"
  count="$(run_mysql "$query" | first_value)"
  require_count "MySQL table $table" "$count"
  printf '%s' "$count"
}

count_snowflake_table() {
  local table="$1"
  local sf_database
  local sf_schema
  local count
  local query
  sf_database="$(snowflake_object_name "$SNOWFLAKE_DATABASE")"
  sf_schema="$(snowflake_object_name "$SNOWFLAKE_SCHEMA")"

  query="SELECT COUNT(*) FROM $(snowflake_ident "$sf_database").$(snowflake_ident "$sf_schema").$(snowflake_ident "$table");"
  count="$(run_snowsql "$query" | first_value)"
  require_count "Snowflake table $table" "$count"
  printf '%s' "$count"
}

render_comparison_row() {
  local table="$1"
  local mysql_count="$2"
  local snowflake_count="$3"
  local is_diff="$4"
  local red=$'\033[31m'
  local reset=$'\033[0m'

  if [[ "$is_diff" == "1" ]]; then
    printf '%s%s\t%s\t%s%s\n' "$red" "$table" "$mysql_count" "$snowflake_count" "$reset"
  else
    printf '%s\t%s\t%s\n' "$table" "$mysql_count" "$snowflake_count"
  fi
}

render_comparison_rows() {
  local table
  local mysql_count
  local snowflake_count
  local is_diff

  while IFS=$'\t' read -r table mysql_count snowflake_count is_diff; do
    [[ -n "$table" ]] || continue
    render_comparison_row "$table" "$mysql_count" "$snowflake_count" "$is_diff"
  done
}

main() {
  validate_args

  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/compare_table_counts.XXXXXX")"

  local mysql_tables="$TMP_DIR/mysql_tables.txt"
  local snowflake_tables="$TMP_DIR/snowflake_tables.txt"
  local mysql_table_keys="$TMP_DIR/mysql_table_keys.tsv"
  local snowflake_table_keys="$TMP_DIR/snowflake_table_keys.tsv"
  local union_keys="$TMP_DIR/union_keys.txt"
  local diff_rows="$TMP_DIR/diff_rows.tsv"
  local has_diff=0
  local table
  local mysql_table
  local snowflake_table
  local mysql_count
  local snowflake_count
  local is_diff

  fetch_mysql_tables >"$mysql_tables"
  fetch_snowflake_tables >"$snowflake_tables"
  write_table_keys "$mysql_tables" >"$mysql_table_keys"
  write_table_keys "$snowflake_tables" >"$snowflake_table_keys"

  check_duplicate_keys "$mysql_table_keys" "MySQL"
  check_duplicate_keys "$snowflake_table_keys" "Snowflake"

  if [[ "$VERBOSE" -eq 1 ]]; then
    printf 'table\tmysql_count\tsnowflake_count\n'
  fi

  awk -F '\t' '{ print $1 }' "$mysql_table_keys" "$snowflake_table_keys" | sort -u >"$union_keys"
  : >"$diff_rows"

  while IFS= read -r table; do
    [[ -n "$table" ]] || continue
    mysql_table="$(lookup_table_name "$table" "$mysql_table_keys")"
    snowflake_table="$(lookup_table_name "$table" "$snowflake_table_keys")"
    mysql_count="MISSING"
    snowflake_count="MISSING"

    if [[ -n "$mysql_table" ]]; then
      mysql_count="$(count_mysql_table "$mysql_table")"
    fi
    if [[ -n "$snowflake_table" ]]; then
      snowflake_count="$(count_snowflake_table "$snowflake_table")"
    fi

    is_diff=0
    if [[ "$mysql_count" != "$snowflake_count" ]]; then
      is_diff=1
      has_diff=1
      printf '%s\t%s\t%s\t%s\n' "$table" "$mysql_count" "$snowflake_count" "$is_diff" >>"$diff_rows"
    fi

    if [[ "$VERBOSE" -eq 1 ]]; then
      render_comparison_row "$table" "$mysql_count" "$snowflake_count" "$is_diff"
    fi
  done <"$union_keys"

  if [[ "$has_diff" -eq 0 ]]; then
    printf 'pass\n'
    return 0
  fi

  if [[ "$VERBOSE" -eq 1 ]]; then
    printf 'failed\n'
  else
    printf 'failed\n'
    printf 'table\tmysql_count\tsnowflake_count\n'
    render_comparison_rows <"$diff_rows"
  fi
  return 1
}

main "$@"
