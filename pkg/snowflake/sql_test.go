package snowflake

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
)

func TestSnapshotFileFormatCompression(t *testing.T) {
	require.Contains(t, snapshotFileFormat("none"), "COMPRESSION = 'NONE'")
	require.Contains(t, snapshotFileFormat("gzip"), "COMPRESSION = 'GZIP'")
	require.Contains(t, snapshotFileFormat("gzip"), `ESCAPE='\\'`)
}

func TestQuoteIdent(t *testing.T) {
	require.Equal(t, `"SIMPLE"`, quoteIdent("simple"))
	require.Equal(t, `"WORKLOAD070117"`, quoteIdent("workload070117"))
	require.Equal(t, `"SBTEST1"`, quoteIdent("sbtest1"))
	require.Equal(t, `"a""b"`, quoteIdent(`a"b`))
}

func TestEscapeString(t *testing.T) {
	require.Equal(t, `a\'b\\c\"d\n`, escapeString("a'b\\c\"d\n"))
}

func TestLoadSnapshotEscapesFilePath(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := &Connector{db: db, TargetDatabase: "ODS_DB"}
	mock.ExpectExec(regexp.QuoteMeta(`COPY INTO "ODS_DB"."DB"."TARGET" FROM @"ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."TIDB2SNOWFLAKE_EXTERNAL" FILES = ('dir/a\'b.csv')`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, conn.LoadSnapshot(context.Background(), "db", "target", "dir/a'b.csv", "none"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateSchemaUsesTargetDatabaseAndSourceDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := &Connector{db: db, TargetDatabase: "ODS_DB"}
	mock.ExpectExec(regexp.QuoteMeta(`CREATE SCHEMA IF NOT EXISTS "ODS_DB"."SOURCE";`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, conn.CreateSchema(context.Background(), "source"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGenMergeIntoEscapesIdentifiersAndFilePath(t *testing.T) {
	meta := &table.Meta{
		Schema: "db",
		Table:  `"target`,
		Columns: []table.Column{
			{Name: `id`, Tp: "int"},
		},
		PrimaryKeys: []string{"id"},
	}

	got := genMergeIntoSQL("ODS_DB", meta, "dir/a'b.csv", 100)

	require.Contains(t, got, `MERGE INTO "ODS_DB"."DB"."""target" AS T`)
	require.Contains(t, got, `FROM '@"ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."TIDB2SNOWFLAKE_EXTERNAL"/dir/a\'b.csv'`)
	require.Contains(t, got, `WHERE TO_NUMBER($4) > 100`)
	require.NotContains(t, got, `<=`)
	require.Contains(t, got, `T."ID" = S."ID"`)
}

func TestLoadIncrementMergesRowsAfterCheckpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := &Connector{db: db, TargetDatabase: "ODS_DB"}
	meta := &table.Meta{
		Schema: "db",
		Table:  "tbl",
		Columns: []table.Column{
			{Name: "id", Tp: "int"},
		},
		PrimaryKeys: []string{"id"},
	}
	mock.ExpectExec(`(?s)MERGE INTO .*WHERE TO_NUMBER\(\$4\) > 100`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = conn.LoadIncrement(context.Background(), meta, "dir/file.csv", 100)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBuildCreateTableSQLFromSnapshotSchema(t *testing.T) {
	tableSchema := table.BuildSchema("test", "bank0", `
CREATE TABLE `+"`bank0`"+` (
  `+"`id`"+` bigint NOT NULL,
  `+"`balance`"+` decimal,
  `+"`name`"+` varchar(30) DEFAULT 'Z',
  `+"`created_at`"+` datetime DEFAULT CURRENT_TIMESTAMP,
  `+"`stored_col`"+` bigint GENERATED ALWAYS AS (`+"`id`"+` + 1) STORED,
  `+"`virtual_col`"+` bigint GENERATED ALWAYS AS (`+"`id`"+` + 2) VIRTUAL,
  PRIMARY KEY (`+"`id`"+`)
	);`)
	got := buildCreateTableSQL("ODS_DB", tableSchema)
	require.Equal(t, `CREATE OR REPLACE TABLE "ODS_DB"."TEST"."BANK0" ("ID" NUMBER NOT NULL, "BALANCE" NUMBER(10, 0), "NAME" VARCHAR(30) DEFAULT 'Z', "CREATED_AT" DATETIME(0) DEFAULT CURRENT_TIMESTAMP(), PRIMARY KEY ("ID"))`, got)
}
