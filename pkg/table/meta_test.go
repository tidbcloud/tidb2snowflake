package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaFilePath(t *testing.T) {
	require.Equal(t, "test.bank0-schema.sql", SchemaFilePath("test", "bank0"))
	require.Equal(t, "test-schema-create.sql", SchemaCreateFilePath("test"))
	require.Equal(t, "d%2Eb.t%2Ea-schema.sql", SchemaFilePath("d.b", "t.a"))
	require.Equal(t, "d%2Eb-schema-create.sql", SchemaCreateFilePath("d.b"))
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
  ` + "`stored_col`" + ` bigint GENERATED ALWAYS AS (` + "`id`" + ` + 2) STORED,
  PRIMARY KEY (` + "`id`" + `) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	tableSchema := BuildSchema("test", "bank0", schemaSQL)
	require.Equal(t, []string{"id"}, tableSchema.PrimaryKeys)
	require.Equal(t, "true", tableSchema.Columns[0].IsPK)

	balance := tableSchema.Columns[1]
	require.Equal(t, "decimal", balance.Tp)
	require.Equal(t, "10", balance.Precision)
	require.Equal(t, "0", balance.Scale)
	columnNames := make([]string, 0, len(tableSchema.Columns))
	for _, column := range tableSchema.Columns {
		columnNames = append(columnNames, column.Name)
	}
	require.Equal(t, []string{"id", "balance", "name", "created_at", "updated_at", "payload"}, columnNames)
}

func TestParseExtendedColumnTypes(t *testing.T) {
	tableSchema := BuildSchema("test", "types", `
CREATE TABLE types (
  id bigint primary key,
  c_mediumblob mediumblob,
  c_longblob longblob,
  c_bit bit(12),
  c_json json,
  c_set set('a','b'),
  c_decimal_unsigned decimal(65,30) unsigned
);`)

	cols := map[string]Column{}
	for _, col := range tableSchema.Columns {
		cols[col.Name] = col
	}

	require.Equal(t, "mediumblob", cols["c_mediumblob"].Tp)
	require.Equal(t, "16777215", cols["c_mediumblob"].Precision)
	require.Equal(t, "longblob", cols["c_longblob"].Tp)
	require.Equal(t, "67108864", cols["c_longblob"].Precision)
	require.Equal(t, "bit", cols["c_bit"].Tp)
	require.Equal(t, "12", cols["c_bit"].Precision)
	require.Equal(t, "json", cols["c_json"].Tp)
	require.Equal(t, "set", cols["c_set"].Tp)
	require.Equal(t, "decimal unsigned", cols["c_decimal_unsigned"].Tp)
	require.Equal(t, "65", cols["c_decimal_unsigned"].Precision)
	require.Equal(t, "30", cols["c_decimal_unsigned"].Scale)
}
