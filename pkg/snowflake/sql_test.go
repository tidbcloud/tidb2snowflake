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
	require.Equal(t, `"simple"`, quoteIdent("simple"))
	require.Equal(t, `"a""b"`, quoteIdent(`a"b`))
}

func TestEscapeString(t *testing.T) {
	require.Equal(t, `a\'b\\c\"d\n`, escapeString("a'b\\c\"d\n"))
}

func TestLoadSnapshotEscapesFilePath(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := &Connector{db: db}
	mock.ExpectExec(regexp.QuoteMeta(`FILES = ('dir/a\'b.csv')`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, conn.LoadSnapshot(context.Background(), "target", "dir/a'b.csv", "none"))
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

	got := genMergeIntoSQL(meta, "dir/a'b.csv", IncrementStageName, 100)

	require.Contains(t, got, `MERGE INTO "db.""target" AS T`)
	require.Contains(t, got, `FROM '@increment_external/dir/a\'b.csv'`)
	require.Contains(t, got, `WHERE TO_NUMBER($4) > 100`)
	require.NotContains(t, got, `<=`)
	require.Contains(t, got, `T."id" = S."id"`)
}

func TestLoadIncrementMergesRowsAfterCheckpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := &Connector{db: db}
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
  PRIMARY KEY (`+"`id`"+`)
	);`)
	got := buildCreateTableSQL(tableSchema)
	require.Equal(t, `CREATE OR REPLACE TABLE "test.bank0" ("id" NUMBER NOT NULL, "balance" NUMBER(10, 0), "name" VARCHAR(30) DEFAULT 'Z', "created_at" DATETIME(0) DEFAULT CURRENT_TIMESTAMP(), PRIMARY KEY ("id"))`, got)
}
