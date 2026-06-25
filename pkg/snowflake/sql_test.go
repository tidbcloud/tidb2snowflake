package snowflake

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
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

func TestLoadSnapshotFromStageEscapesFilePath(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(`FILES = ('dir/a\'b.csv')`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, LoadSnapshotFromStage(db, "target", "stage", "dir/a'b.csv"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGenMergeIntoEscapesIdentifiersAndFilePath(t *testing.T) {
	meta := &table.Meta{
		Table: `"target`,
		Columns: []cloudstorage.TableCol{
			{Name: `id`, Tp: "int"},
		},
		PrimaryKeys: []string{"id"},
	}

	got := GenMergeInto(meta, "dir/a'b.csv", `"stage`)

	require.Contains(t, got, `MERGE INTO """target" AS T`)
	require.Contains(t, got, `FROM '@"""stage"/dir/a\'b.csv'`)
	require.Contains(t, got, `T."id" = S."id"`)
}

func TestGenCreateSchemaFromSnapshotSchema(t *testing.T) {
	tableSchema := table.BuildSchema("test", "bank0", `
CREATE TABLE `+"`bank0`"+` (
  `+"`id`"+` bigint NOT NULL,
  `+"`balance`"+` decimal,
  `+"`name`"+` varchar(30) DEFAULT 'Z',
  `+"`created_at`"+` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`+"`id`"+`)
);`)
	got := buildCreateSchemaSQL(tableSchema)
	require.Equal(t, `CREATE OR REPLACE TABLE "bank0" (
    "id" NUMBER NOT NULL,
    "balance" NUMBER(10, 0),
    "name" VARCHAR(30) DEFAULT 'Z',
    "created_at" DATETIME(0) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY ("id")
)`, got)
}
