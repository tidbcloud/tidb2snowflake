package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
)

func TestStagePatternFromGlob(t *testing.T) {
	require.Equal(t, `.*db\.tbl\..*\.csv`, stagePatternFromGlob("db.tbl.*.csv"))
	require.Equal(t, `.*db\.table\$name\..*\.csv`, stagePatternFromGlob("db.table$name.*.csv"))
}

func TestSnapshotFileFormatCompression(t *testing.T) {
	require.Contains(t, snapshotFileFormat("none"), "COMPRESSION = 'NONE'")
	require.Contains(t, snapshotFileFormat("gzip"), "COMPRESSION = 'GZIP'")
	require.Contains(t, snapshotFileFormat("gzip"), `ESCAPE='\\'`)
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
	require.Equal(t, `CREATE OR REPLACE TABLE bank0 (
    id NUMBER NOT NULL,
    balance NUMBER(10, 0),
    name VARCHAR(30) DEFAULT 'Z',
    created_at DATETIME(0) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id)
)`, got)
}
