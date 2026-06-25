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

write_probe() {
  cat >"$WORKDIR/ddl_matrix_probe.go" <<'GO'
package main

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
)

const (
	dbName    = "t2sf_matrix"
	typeTable = "supported_type_matrix"
	ddlTable  = "supported_ddl_live"
)

func main() {
	cfg := &tidb.Config{
		Host: os.Getenv("TIDB_HOST"),
		Port: atoi(os.Getenv("TIDB_PORT")),
		User: "root",
	}
	db, err := cfg.OpenDB()
	must(err)
	defer db.Close()

	checkSupportedTypes(db)
	checkLiveDDL(db)
	checkSyntheticTableDDL()
	fmt.Println("ddl matrix smoke passed")
}

func atoi(s string) int {
	var n int
	_, _ = fmt.Sscanf(s, "%d", &n)
	return n
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func checkSupportedTypes(db *sql.DB) {
	cols, err := tidb.GetTiDBTableColumn(db, dbName, typeTable)
	must(err)
	if len(cols) < 30 {
		panic(fmt.Sprintf("expected many type columns, got %d", len(cols)))
	}
	byName := map[string]cloudstorage.TableCol{}
	for _, col := range cols {
		byName[col.Name] = col
	}
	for name, want := range map[string]string{
		"c_tinyint_unsigned":   "tinyint unsigned",
		"c_smallint_unsigned":  "smallint unsigned",
		"c_mediumint_unsigned": "mediumint unsigned",
		"c_int_unsigned":       "int unsigned",
		"c_bigint_unsigned":    "bigint unsigned",
		"c_year":               "year",
		"c_enum":               "enum",
		"c_vector":             "vector",
	} {
		if got := byName[name].Tp; got != want {
			panic(fmt.Sprintf("%s type = %q, want %q", name, got, want))
		}
	}
}

func checkLiveDDL(db *sql.DB) {
	exec(db, "DROP TABLE IF EXISTS "+dbName+"."+ddlTable)
	exec(db, `CREATE TABLE `+dbName+`.`+ddlTable+` (
		id BIGINT NOT NULL PRIMARY KEY,
		c_drop_me VARCHAR(16) NULL,
		c_rename_me VARCHAR(16) NULL,
		c_widen_int INT NULL,
		c_widen_varchar VARCHAR(32) NULL,
		c_widen_decimal DECIMAL(10, 2) NULL,
		c_nullable VARCHAR(64) NULL,
		c_default VARCHAR(32) DEFAULT 'v1'
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	exec(db, `INSERT INTO `+dbName+`.`+ddlTable+` VALUES
		(1, 'drop', 'rename', 1, 'v', 1.23, 'filled', 'v1')`)

	aliases := map[string]string{}
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" ADD COLUMN c_added_nullable VARCHAR(64) NULL",
		"ALTER TABLE \""+ddlTable+"\" ADD COLUMN \"c_added_nullable\" VARCHAR(64);")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" ADD COLUMN c_added_not_null_default INT NOT NULL DEFAULT 7",
		"ALTER TABLE \""+ddlTable+"\" ADD COLUMN \"c_added_not_null_default\" NUMBER NOT NULL DEFAULT 7;")
	expectDDL(db, aliases, map[string]string{"c_renamed": "c_rename_me"}, "ALTER TABLE "+dbName+"."+ddlTable+" RENAME COLUMN c_rename_me TO c_renamed",
		"ALTER TABLE \""+ddlTable+"\" RENAME COLUMN \"c_rename_me\" TO \"c_renamed\";")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_widen_int BIGINT NULL",
		"ALTER TABLE \""+ddlTable+"\" MODIFY COLUMN \"c_widen_int\" NUMBER;")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_widen_varchar VARCHAR(128) NULL",
		"ALTER TABLE \""+ddlTable+"\" MODIFY COLUMN \"c_widen_varchar\" VARCHAR(128);")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_widen_decimal DECIMAL(20, 6) NULL",
		"ALTER TABLE \""+ddlTable+"\" MODIFY COLUMN \"c_widen_decimal\" NUMBER(20, 6);")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_nullable VARCHAR(64) NOT NULL",
		"ALTER TABLE \""+ddlTable+"\" MODIFY COLUMN \"c_nullable\" SET NOT NULL;")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_nullable VARCHAR(64) NULL",
		"ALTER TABLE \""+ddlTable+"\" MODIFY COLUMN \"c_nullable\" DROP NOT NULL;")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" ALTER COLUMN c_default DROP DEFAULT",
		"ALTER TABLE \""+ddlTable+"\" MODIFY COLUMN \"c_default\" DROP DEFAULT;")
	expectDDL(db, aliases, nil, "ALTER TABLE "+dbName+"."+ddlTable+" DROP COLUMN c_drop_me",
		"ALTER TABLE \""+ddlTable+"\" DROP COLUMN \"c_drop_me\";")
}

func exec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		panic(fmt.Sprintf("%s: %v", query, err))
	}
}

func expectDDL(db *sql.DB, aliases map[string]string, newAliases map[string]string, query string, expected ...string) {
	prev := &table.Meta{
		Schema:  dbName,
		Table:   ddlTable,
		Columns: columns(db, aliases),
	}
	exec(db, query)
	for name, id := range newAliases {
		aliases[name] = id
	}
	next := table.FromSchemaFile(cloudstorage.SchemaFile{
		Schema:  dbName,
		Table:   ddlTable,
		Columns: columns(db, aliases),
	})
	got, err := snowflake.GenDDLViaMetaDiff(prev, next, model.ActionNone)
	must(err)
	assertElements(got, expected)
}

func columns(db *sql.DB, aliases map[string]string) []cloudstorage.TableCol {
	cols, err := tidb.GetTiDBTableColumn(db, dbName, ddlTable)
	must(err)
	for i := range cols {
		if id, ok := aliases[cols[i].Name]; ok {
			cols[i].ID = id
		} else {
			cols[i].ID = cols[i].Name
		}
	}
	return cols
}

func checkSyntheticTableDDL() {
	for _, tc := range []struct {
		name string
		def  cloudstorage.SchemaFile
		want []string
	}{
		{
			name: "truncate",
			def:  cloudstorage.SchemaFile{Table: "t_truncate", Type: model.ActionTruncateTable},
			want: []string{"TRUNCATE TABLE \"t_truncate\""},
		},
		{
			name: "drop table",
			def:  cloudstorage.SchemaFile{Table: "t_drop", Type: model.ActionDropTable},
			want: []string{"DROP TABLE \"t_drop\""},
		},
		{
			name: "drop schema",
			def:  cloudstorage.SchemaFile{Schema: "s_drop", Type: model.ActionDropSchema},
			want: []string{"DROP SCHEMA \"s_drop\""},
		},
	} {
		got, err := snowflake.GenDDLViaMetaDiff(nil, table.FromSchemaFile(tc.def), model.ActionType(tc.def.Type))
		must(err)
		assertElements(got, tc.want)
	}
}

func assertElements(got, want []string) {
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		panic(fmt.Sprintf("DDL mismatch\ngot:  %q\nwant: %q", got, want))
	}
}
GO
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
  write_probe
  TIDB_HOST="$TIDB_HOST" TIDB_PORT="$TIDB_PORT" go run -ldflags=-checklinkname=0 "$WORKDIR/ddl_matrix_probe.go"

  log "success"
  log "logs are in $WORKDIR"
}

main "$@"
