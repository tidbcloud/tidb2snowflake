package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaFilePath(t *testing.T) {
	require.Equal(t, "test.bank0-schema.sql", SchemaFilePath("test", "bank0"))
}

func TestSnowflakeTableName(t *testing.T) {
	require.Equal(t, "test.bank0", (&Meta{Schema: "test", Table: "bank0"}).SnowflakeTableName())
}

func TestParseTableSchema(t *testing.T) {
	schemaSQL := `
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET NAMES binary*/;
CREATE TABLE ` + "`bank0`" + ` (
  ` + "`id`" + ` bigint NOT NULL,
  ` + "`balance`" + ` decimal DEFAULT NULL,
  ` + "`name`" + ` varchar(30) DEFAULT 'Z',
  ` + "`created_at`" + ` datetime DEFAULT CURRENT_TIMESTAMP,
  ` + "`updated_at`" + ` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
  ` + "`payload`" + ` blob,
  ` + "`virtual_col`" + ` bigint GENERATED ALWAYS AS (` + "`id`" + ` + 1) VIRTUAL,
  PRIMARY KEY (` + "`id`" + `) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	tableSchema := BuildSchema("test", "bank0", schemaSQL)
	require.Equal(t, []string{"id"}, tableSchema.PrimaryKeys)
	require.Equal(t, "true", tableSchema.Columns[0].IsPK)

	balance := tableSchema.Columns[1]
	require.Equal(t, "decimal", balance.Tp)
	require.Equal(t, "10", balance.Precision)
	require.Equal(t, "0", balance.Scale)
}
