package snowflake

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"
)

func quoteQualifiedIdent(idents ...string) string {
	parts := make([]string, 0, len(idents))
	for _, ident := range idents {
		parts = append(parts, quoteIdent(ident))
	}
	return strings.Join(parts, ".")
}

func snapshotFileFormat(compression string) string {
	parts := []string{
		"TYPE = 'CSV'",
		"EMPTY_FIELD_AS_NULL = FALSE",
		"NULL_IF=('\\\\N')",
		`FIELD_OPTIONALLY_ENCLOSED_BY='"'`,
		`ESCAPE='\\'`,
		"BINARY_FORMAT = 'UTF8'",
	}
	switch compression {
	case "none":
		parts = append(parts, "COMPRESSION = 'NONE'")
	case "gzip":
		parts = append(parts, "COMPRESSION = 'GZIP'")
	default:
		log.Panic("unknown snapshot compression", zap.String("compression", compression))
	}
	return strings.Join(parts, " ")
}

type defaultSQLExpression interface {
	SQLExpression() string
}

func quoteIdent(ident string) string {
	if isSnowflakeRegularIdent(ident) {
		ident = strings.ToUpper(ident)
	}
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func isSnowflakeRegularIdent(ident string) bool {
	if ident == "" {
		return false
	}
	if !isSnowflakeIdentStart(ident[0]) {
		return false
	}
	for i := 1; i < len(ident); i++ {
		if !isSnowflakeIdentPart(ident[i]) {
			return false
		}
	}
	return true
}

func isSnowflakeIdentStart(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
}

func isSnowflakeIdentPart(ch byte) bool {
	return isSnowflakeIdentStart(ch) || (ch >= '0' && ch <= '9') || ch == '$'
}

func quoteIdents(idents []string) []string {
	out := make([]string, 0, len(idents))
	for _, ident := range idents {
		out = append(out, quoteIdent(ident))
	}
	return out
}

func escapeString(s string) string {
	// See https://docs.snowflake.com/en/sql-reference/data-types-text#escape-sequences-in-single-quoted-string-constants
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		r := s[i]
		switch r {
		case '\'':
			sb.Write([]byte{'\\', '\''})
		case '"':
			sb.Write([]byte{'\\', '"'})
		case '\\':
			sb.Write([]byte{'\\', '\\'})
		case '\b':
			sb.Write([]byte{'\\', 'b'})
		case '\f':
			sb.Write([]byte{'\\', 'f'})
		case '\n':
			sb.Write([]byte{'\\', 'n'})
		case '\r':
			sb.Write([]byte{'\\', 'r'})
		case '\t':
			sb.Write([]byte{'\\', 't'})
		case 0:
			sb.Write([]byte{'\\', '0'})
		default:
			if strconv.IsPrint(rune(r)) {
				sb.WriteByte(r)
				continue
			}
			sb.WriteString("\\u")
			sb.WriteString(strconv.FormatInt(int64(r), 16))
		}
	}
	return sb.String()
}

func defaultString(val any) string {
	if expr, ok := val.(defaultSQLExpression); ok {
		return expr.SQLExpression()
	}
	_, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
	if err != nil {
		return fmt.Sprintf("'%s'", escapeString(fmt.Sprintf("%v", val)))
	}
	return fmt.Sprintf("%v", val)
}

func buildCreateTableSQL(targetDatabase string, tableSchema *table.Meta) string {
	defs := make([]string, 0, len(tableSchema.Columns)+1)
	for _, column := range tableSchema.Columns {
		defs = append(defs, buildColumn(column))
	}
	if len(tableSchema.PrimaryKeys) > 0 {
		defs = append(defs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quoteIdents(tableSchema.PrimaryKeys), ", ")))
	}

	return fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s (%s)",
		quoteQualifiedIdent(targetDatabase, tableSchema.Schema, tableSchema.Table),
		strings.Join(defs, ", "),
	)
}

func genMergeIntoSQL(targetDatabase string, tableMeta *table.Meta, filePath string, checkpointTs uint64) string {
	selectStat := make([]string, 0, len(tableMeta.Columns)+1)
	selectStat = append(selectStat, `$1 AS "METADATA$FLAG"`)
	for i, col := range tableMeta.Columns {
		colName := quoteIdent(col.Name)
		if TiDB2SnowflakeTypeMap[strings.ToLower(col.Tp)] == "BINARY" {
			selectStat = append(selectStat, fmt.Sprintf(`TO_BINARY($%d, 'HEX') AS %s`, i+5, colName))
		} else {
			selectStat = append(selectStat, fmt.Sprintf(`$%d AS %s`, i+5, colName))
		}
	}

	pkColumn := make([]string, 0, len(tableMeta.PrimaryKeys))
	onStat := make([]string, 0, len(tableMeta.PrimaryKeys))
	for _, col := range tableMeta.PrimaryKeys {
		colName := quoteIdent(col)
		pkColumn = append(pkColumn, colName)
		onStat = append(onStat, fmt.Sprintf(`T.%s = S.%s`, colName, colName))
	}

	updateStat := make([]string, 0, len(tableMeta.Columns))
	for _, col := range tableMeta.Columns {
		colName := quoteIdent(col.Name)
		updateStat = append(updateStat, fmt.Sprintf(`%s = S.%s`, colName, colName))
	}

	insertStat := make([]string, 0, len(tableMeta.Columns))
	for _, col := range tableMeta.Columns {
		insertStat = append(insertStat, quoteIdent(col.Name))
	}

	valuesStat := make([]string, 0, len(tableMeta.Columns))
	for _, col := range tableMeta.Columns {
		valuesStat = append(valuesStat, fmt.Sprintf(`S.%s`, quoteIdent(col.Name)))
	}

	// TODO: Remove QUALIFY row_number() after cdc support merge dml or snowflake support deterministic merge
	stageFile := fmt.Sprintf("@%s/%s", quoteQualifiedIdent(targetDatabase, InternalSchemaName, ExternalStageName), escapeString(filePath))
	mergeQuery := fmt.Sprintf(
		`MERGE INTO %s AS T USING
			(
			SELECT
				%s
			FROM '%s'
			WHERE TO_NUMBER($4) > %d
			QUALIFY row_number() over (partition by %s order by $4 desc) = 1
		) AS S
		ON
		(
			%s
		)
			WHEN MATCHED AND S.METADATA$FLAG != 'D' THEN UPDATE SET %s
			WHEN MATCHED AND S.METADATA$FLAG = 'D' THEN DELETE
			WHEN NOT MATCHED AND S.METADATA$FLAG != 'D' THEN INSERT (%s) VALUES (%s);`,
		quoteQualifiedIdent(targetDatabase, tableMeta.Schema, tableMeta.Table),
		strings.Join(selectStat, ",\n"),
		stageFile,
		checkpointTs,
		strings.Join(pkColumn, ", "),
		strings.Join(onStat, " AND "),
		strings.Join(updateStat, ", "),
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "))

	return mergeQuery
}
