package snowflake

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
)

func TestGenDDLViaMetaDiff(t *testing.T) {
	prevMeta := &table.Meta{
		Table:  "test_table",
		Schema: "test_schema",
		Columns: []cloudstorage.TableCol{
			{
				ID:        "1",
				Name:      "id",
				Tp:        "int",
				Precision: "11",
			},
			{
				ID:   "2",
				Name: "name",
				Tp:   "varchar",
			},
			{
				ID:   "3",
				Name: "age",
				Tp:   "int",
			},
			{
				ID:   "4",
				Name: "birth",
				Tp:   "date",
			},
		},
	}
	nextMeta := table.FromSchemaFile(cloudstorage.SchemaFile{
		Table:  "test_table",
		Schema: "test_schema",
		Columns: []cloudstorage.TableCol{
			{
				ID:        "5",
				Name:      "id",
				Tp:        "char",
				Precision: "10",
			},
			{
				ID:   "2",
				Name: "color",
				Tp:   "varchar",
			},
			{
				ID:   "4",
				Name: "birth",
				Tp:   "date",
			},
			{
				ID:        "6",
				Name:      "gender",
				Tp:        "varchar",
				Precision: "10",
			},
		},
	})

	expectedDDLs := []string{
		`ALTER TABLE "test_schema.test_table" MODIFY COLUMN "id" CHAR(10);`,
		`ALTER TABLE "test_schema.test_table" RENAME COLUMN "name" TO "color";`,
		`ALTER TABLE "test_schema.test_table" DROP COLUMN "age";`,
		`ALTER TABLE "test_schema.test_table" ADD COLUMN "gender" VARCHAR(10);`,
	}

	ddl, err := GenDDLViaMetaDiff(prevMeta, nextMeta, model.ActionNone)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedDDLs, ddl)
}

func TestGetSnowflakeTypeString_NewScalarMappings(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  cloudstorage.TableCol
		want string
	}{
		{
			name: "year",
			col:  cloudstorage.TableCol{Name: "c_year", Tp: "year"},
			want: `"c_year" NUMBER`,
		},
		{
			name: "enum",
			col:  cloudstorage.TableCol{Name: "c_enum", Tp: "enum"},
			want: `"c_enum" VARCHAR`,
		},
		{
			name: "vector",
			col:  cloudstorage.TableCol{Name: "c_vector", Tp: "vector"},
			want: `"c_vector" VARCHAR`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := buildColumn(tc.col)
			require.Equal(t, tc.want, got)
		})
	}
}
