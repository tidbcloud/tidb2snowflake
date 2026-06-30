package tidb

import (
	"cmp"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/dumpling/export"
)

func GetTiDBTablePKColumns(db *sql.DB, sourceDatabase, sourceTable string) ([]string, error) {
	indexQuery := fmt.Sprintf("SHOW INDEX FROM %s.%s", quoteIdent(sourceDatabase), quoteIdent(sourceTable))
	indexRows, err := db.Query(indexQuery)
	if err != nil {
		return nil, errors.Trace(err)
	}
	indexResults, err := export.GetSpecifiedColumnValuesAndClose(indexRows, "KEY_NAME", "COLUMN_NAME", "SEQ_IN_INDEX")
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Sort by key_name, seq_in_index
	slices.SortStableFunc(indexResults, func(i, j []string) int {
		if i[0] == j[0] {
			return cmp.Compare(i[2], j[2]) // Sort by seq_in_index
		}
		return cmp.Compare(i[0], j[0]) // Sort by key_name
	})
	pkColumns := make([]string, 0)
	for _, row := range indexResults {
		keyName, columnName := row[0], row[1]
		if keyName == "PRIMARY" {
			pkColumns = append(pkColumns, columnName)
		}
	}
	return pkColumns, nil
}

func quoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}
