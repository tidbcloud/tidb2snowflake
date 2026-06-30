package snowflake

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
)

func TestGenDDLViaTiDBDDLColumnDDL(t *testing.T) {
	prevMeta := testMeta(
		table.Column{Name: "id", Tp: "int", Precision: "11"},
		table.Column{Name: "name", Tp: "varchar", Precision: "16"},
		table.Column{Name: "age", Tp: "int", Precision: "11"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16", Default: "v1"},
	)

	ddl, err := GenDDLViaTiDBDDL(prevMeta, testMeta(
		table.Column{Name: "id", Tp: "int", Precision: "11"},
		table.Column{Name: "name", Tp: "varchar", Precision: "16"},
		table.Column{Name: "age", Tp: "int", Precision: "11"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16", Default: "v1"},
		table.Column{Name: "gender", Tp: "varchar", Precision: "10", Nullable: "false", Default: 7},
	), model.ActionAddColumn, "ALTER TABLE test_schema.test_table ADD COLUMN gender VARCHAR(10) NOT NULL DEFAULT 7")
	require.NoError(t, err)
	require.Equal(t, []string{`ALTER TABLE "test_schema.test_table" ADD COLUMN "gender" VARCHAR(10) NOT NULL DEFAULT 7;`}, ddl)

	ddl, err = GenDDLViaTiDBDDL(prevMeta, testMeta(
		table.Column{Name: "id", Tp: "int", Precision: "11"},
		table.Column{Name: "name", Tp: "varchar", Precision: "16"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16", Default: "v1"},
	), model.ActionDropColumn, "ALTER TABLE test_schema.test_table DROP COLUMN age")
	require.NoError(t, err)
	require.Equal(t, []string{`ALTER TABLE "test_schema.test_table" DROP COLUMN "age";`}, ddl)

	ddl, err = GenDDLViaTiDBDDL(prevMeta, testMeta(
		table.Column{Name: "id", Tp: "int", Precision: "11"},
		table.Column{Name: "color", Tp: "varchar", Precision: "16"},
		table.Column{Name: "age", Tp: "int", Precision: "11"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16", Default: "v1"},
	), model.ActionModifyColumn, "ALTER TABLE test_schema.test_table RENAME COLUMN name TO color")
	require.NoError(t, err)
	require.Equal(t, []string{`ALTER TABLE "test_schema.test_table" RENAME COLUMN "name" TO "color";`}, ddl)

	ddl, err = GenDDLViaTiDBDDL(prevMeta, testMeta(
		table.Column{Name: "id", Tp: "char", Precision: "10"},
		table.Column{Name: "name", Tp: "varchar", Precision: "16"},
		table.Column{Name: "age", Tp: "int", Precision: "11"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16", Default: "v1"},
	), model.ActionModifyColumn, "ALTER TABLE test_schema.test_table MODIFY COLUMN id CHAR(10)")
	require.NoError(t, err)
	require.Equal(t, []string{`ALTER TABLE "test_schema.test_table" MODIFY COLUMN "id" CHAR(10);`}, ddl)

	ddl, err = GenDDLViaTiDBDDL(prevMeta, testMeta(
		table.Column{Name: "id", Tp: "int", Precision: "11"},
		table.Column{Name: "color", Tp: "varchar", Precision: "32"},
		table.Column{Name: "age", Tp: "int", Precision: "11"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16", Default: "v1"},
	), model.ActionModifyColumn, "ALTER TABLE test_schema.test_table CHANGE COLUMN name color VARCHAR(32)")
	require.NoError(t, err)
	require.Equal(t, []string{
		`ALTER TABLE "test_schema.test_table" RENAME COLUMN "name" TO "color";`,
		`ALTER TABLE "test_schema.test_table" MODIFY COLUMN "color" VARCHAR(32);`,
	}, ddl)

	ddl, err = GenDDLViaTiDBDDL(prevMeta, testMeta(
		table.Column{Name: "id", Tp: "int", Precision: "11"},
		table.Column{Name: "name", Tp: "varchar", Precision: "16"},
		table.Column{Name: "age", Tp: "int", Precision: "11"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16"},
	), model.ActionSetDefaultValue, "ALTER TABLE test_schema.test_table ALTER COLUMN c_default DROP DEFAULT")
	require.NoError(t, err)
	require.Equal(t, []string{`ALTER TABLE "test_schema.test_table" MODIFY COLUMN "c_default" DROP DEFAULT;`}, ddl)

	ddl, err = GenDDLViaTiDBDDL(prevMeta, prevMeta, model.ActionAddIndex, "ALTER TABLE test_schema.test_table ADD INDEX idx_name(name)")
	require.NoError(t, err)
	require.Empty(t, ddl)

	ddl, err = GenDDLViaTiDBDDL(prevMeta, testMeta(
		table.Column{Name: "id", Tp: "int", Precision: "11"},
		table.Column{Name: "color", Tp: "varchar", Precision: "32"},
		table.Column{Name: "c_default", Tp: "varchar", Precision: "16", Default: "v1"},
		table.Column{Name: "created_at", Tp: "datetime", Precision: "0"},
	), model.ActionMultiSchemaChange, "ALTER TABLE test_schema.test_table DROP COLUMN age, CHANGE COLUMN name color VARCHAR(32), ADD COLUMN created_at DATETIME")
	require.NoError(t, err)
	require.Equal(t, []string{
		`ALTER TABLE "test_schema.test_table" DROP COLUMN "age";`,
		`ALTER TABLE "test_schema.test_table" RENAME COLUMN "name" TO "color";`,
		`ALTER TABLE "test_schema.test_table" MODIFY COLUMN "color" VARCHAR(32);`,
		`ALTER TABLE "test_schema.test_table" ADD COLUMN "created_at" DATETIME(0);`,
	}, ddl)
}

func TestGenDDLViaTiDBDDLTableDDL(t *testing.T) {
	meta := testMeta()

	ddl, err := GenDDLViaTiDBDDL(nil, meta, model.ActionTruncateTable, "")
	require.NoError(t, err)
	require.Equal(t, []string{`TRUNCATE TABLE "test_schema.test_table"`}, ddl)

	ddl, err = GenDDLViaTiDBDDL(nil, meta, model.ActionDropTable, "")
	require.NoError(t, err)
	require.Equal(t, []string{`DROP TABLE IF EXISTS "test_schema.test_table"`}, ddl)

	_, err = GenDDLViaTiDBDDL(
		&table.Meta{Schema: "test_schema", Table: "old_table"},
		&table.Meta{Schema: "test_schema", Table: "new_table"},
		model.ActionRenameTable,
		"RENAME TABLE `old_table` TO `new_table`",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rename table ddl is not supported")

	ddl, err = GenDDLViaTiDBDDL(nil, &table.Meta{Schema: "test_schema"}, model.ActionDropSchema, "")
	require.NoError(t, err)
	require.Empty(t, ddl)
}

func testMeta(columns ...table.Column) *table.Meta {
	return &table.Meta{
		Schema:  "test_schema",
		Table:   "test_table",
		Columns: columns,
	}
}

func TestGetSnowflakeTypeString_NewScalarMappings(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  table.Column
		want string
	}{
		{
			name: "year",
			col:  table.Column{Name: "c_year", Tp: "year"},
			want: `"c_year" NUMBER`,
		},
		{
			name: "enum",
			col:  table.Column{Name: "c_enum", Tp: "enum"},
			want: `"c_enum" VARCHAR`,
		},
		{
			name: "vector",
			col:  table.Column{Name: "c_vector", Tp: "vector"},
			want: `"c_vector" VARCHAR`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := buildColumn(tc.col)
			require.Equal(t, tc.want, got)
		})
	}
}
