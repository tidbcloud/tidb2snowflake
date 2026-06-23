package tidb_test

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
)

func TestGetColumnDiff(t *testing.T) {
	prev := []cloudstorage.TableCol{
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
	}
	curr := []cloudstorage.TableCol{
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
			ID:   "6",
			Name: "gender",
			Tp:   "varchar",
		},
	}
	expected := []tidb.ColumnDiff{
		{
			Action: tidb.MODIFY_COLUMN,
			Before: &prev[0],
			After:  &curr[0],
		},
		{
			Action: tidb.RENAME_COLUMN,
			Before: &prev[1],
			After:  &curr[1],
		},
		{
			Action: tidb.DROP_COLUMN,
			Before: &prev[2],
			After:  nil,
		},
		{
			Action: tidb.UNCHANGE,
			Before: &prev[3],
			After:  &curr[2],
		},
		{
			Action: tidb.ADD_COLUMN,
			Before: nil,
			After:  &curr[3],
		},
	}
	columnDiff, err := tidb.GetColumnDiff(prev, curr)
	require.NoError(t, err)
	require.ElementsMatch(t, expected, columnDiff)
}
