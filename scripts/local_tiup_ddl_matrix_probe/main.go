package main

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"go.uber.org/zap"
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
	db, err := tidb.OpenDB(cfg)
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
		log.Panic("unexpected error", zap.Error(err))
	}
}

func checkSupportedTypes(db *sql.DB) {
	meta := tableSchema(db, typeTable)
	if len(meta.Columns) < 30 {
		log.Panic("expected many type columns", zap.Int("got", len(meta.Columns)))
	}
	byName := map[string]table.Column{}
	for _, col := range meta.Columns {
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
			log.Panic("unexpected column type", zap.String("column", name), zap.String("got", got), zap.String("want", want))
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

	tableName := `"` + dbName + "." + ddlTable + `"`
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" ADD COLUMN c_added_nullable VARCHAR(64) NULL",
		"ALTER TABLE "+tableName+" ADD COLUMN \"c_added_nullable\" VARCHAR(64);")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" ADD COLUMN c_added_not_null_default INT NOT NULL DEFAULT 7",
		"ALTER TABLE "+tableName+" ADD COLUMN \"c_added_not_null_default\" NUMBER NOT NULL DEFAULT 7;")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" RENAME COLUMN c_rename_me TO c_renamed",
		"ALTER TABLE "+tableName+" RENAME COLUMN \"c_rename_me\" TO \"c_renamed\";")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_widen_int BIGINT NULL",
		"ALTER TABLE "+tableName+" MODIFY COLUMN \"c_widen_int\" NUMBER;")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_widen_varchar VARCHAR(128) NULL",
		"ALTER TABLE "+tableName+" MODIFY COLUMN \"c_widen_varchar\" VARCHAR(128);")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_widen_decimal DECIMAL(20, 6) NULL",
		"ALTER TABLE "+tableName+" MODIFY COLUMN \"c_widen_decimal\" NUMBER(20, 6);")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_nullable VARCHAR(64) NOT NULL",
		"ALTER TABLE "+tableName+" MODIFY COLUMN \"c_nullable\" SET NOT NULL;")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" MODIFY COLUMN c_nullable VARCHAR(64) NULL",
		"ALTER TABLE "+tableName+" MODIFY COLUMN \"c_nullable\" DROP NOT NULL;")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" ALTER COLUMN c_default DROP DEFAULT",
		"ALTER TABLE "+tableName+" MODIFY COLUMN \"c_default\" DROP DEFAULT;")
	expectDDL(db, "ALTER TABLE "+dbName+"."+ddlTable+" DROP COLUMN c_drop_me",
		"ALTER TABLE "+tableName+" DROP COLUMN \"c_drop_me\";")
}

func exec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		log.Panic("execute TiDB SQL", zap.String("query", query), zap.Error(err))
	}
}

func expectDDL(db *sql.DB, query string, expected ...string) {
	prev := tableSchema(db, ddlTable)
	exec(db, query)
	next := tableSchema(db, ddlTable)
	got, err := snowflake.GenDDLViaTiDBDDL(prev, next, model.ActionNone, query)
	must(err)
	assertElements(got, expected)
}

func tableSchema(db *sql.DB, tableName string) *table.Meta {
	var name, createTable string
	row := db.QueryRow("SHOW CREATE TABLE " + quoteTiDBIdent(dbName) + "." + quoteTiDBIdent(tableName))
	must(row.Scan(&name, &createTable))
	return table.BuildSchema(dbName, tableName, createTable)
}

func quoteTiDBIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func checkSyntheticTableDDL() {
	for _, tc := range []struct {
		name string
		def  cloudstorage.SchemaFile
		want []string
	}{
		{
			name: "truncate",
			def:  cloudstorage.SchemaFile{Schema: dbName, Table: "t_truncate", Type: byte(model.ActionTruncateTable)},
			want: []string{"TRUNCATE TABLE \"" + dbName + ".t_truncate\""},
		},
		{
			name: "drop table",
			def:  cloudstorage.SchemaFile{Schema: dbName, Table: "t_drop", Type: byte(model.ActionDropTable)},
			want: []string{"DROP TABLE \"" + dbName + ".t_drop\""},
		},
		{
			name: "drop schema",
			def:  cloudstorage.SchemaFile{Schema: "s_drop", Type: byte(model.ActionDropSchema)},
			want: []string{"DROP SCHEMA \"s_drop\""},
		},
	} {
		got, err := snowflake.GenDDLViaTiDBDDL(nil, table.FromSchemaFile(tc.def), model.ActionType(tc.def.Type), "")
		must(err)
		assertElements(got, tc.want)
	}
}

func assertElements(got, want []string) {
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		log.Panic("DDL mismatch", zap.Strings("got", got), zap.Strings("want", want))
	}
}
