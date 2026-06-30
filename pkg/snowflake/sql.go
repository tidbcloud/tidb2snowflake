package snowflake

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

func createExternalStage(db *sql.DB, stageName, s3WorkspaceURL string, cred *credentials.Value) error {
	sql := fmt.Sprintf(`CREATE OR REPLACE STAGE %s URL = '%s' CREDENTIALS = (AWS_KEY_ID = '%s' AWS_SECRET_KEY = '%s' AWS_TOKEN = '%s')
		FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE='\\' BINARY_FORMAT = 'HEX');`,
		stageName, escapeString(s3WorkspaceURL), escapeString(cred.AccessKeyID), escapeString(cred.SecretAccessKey), escapeString(cred.SessionToken))
	_, err := db.Exec(sql)
	return err
}

func dropStage(db *sql.DB, stageName string) error {
	sql := fmt.Sprintf(`DROP STAGE IF EXISTS %s;`, stageName)
	_, err := db.Exec(sql)
	return err
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
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
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

func buildCreateTableSQL(tableSchema *table.Meta) string {
	defs := make([]string, 0, len(tableSchema.Columns)+1)
	for _, column := range tableSchema.Columns {
		defs = append(defs, buildColumn(column))
	}
	if len(tableSchema.PrimaryKeys) > 0 {
		defs = append(defs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quoteIdents(tableSchema.PrimaryKeys), ", ")))
	}

	return fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s (%s)",
		quoteIdent(tableSchema.SnowflakeTableName()),
		strings.Join(defs, ", "),
	)
}

func genMergeIntoSQL(tableMeta *table.Meta, filePath string, stageName string, highWatermark uint64) string {
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
	stageFile := fmt.Sprintf("@%s/%s", stageName, escapeString(filePath))
	mergeQuery := fmt.Sprintf(
		`MERGE INTO %s AS T USING
		(
			SELECT
				%s
			FROM '%s'
			WHERE TO_NUMBER($4) <= %d
			QUALIFY row_number() over (partition by %s order by $4 desc) = 1
		) AS S
		ON
		(
			%s
		)
		WHEN MATCHED AND S.METADATA$FLAG != 'D' THEN UPDATE SET %s
		WHEN MATCHED AND S.METADATA$FLAG = 'D' THEN DELETE
		WHEN NOT MATCHED AND S.METADATA$FLAG != 'D' THEN INSERT (%s) VALUES (%s);`,
		quoteIdent(tableMeta.SnowflakeTableName()),
		strings.Join(selectStat, ",\n"),
		stageFile,
		highWatermark,
		strings.Join(pkColumn, ", "),
		strings.Join(onStat, " AND "),
		strings.Join(updateStat, ", "),
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "))

	return mergeQuery
}

func genCountCommitTsAfter(filePath string, stageName string, highWatermark uint64) string {
	stageFile := fmt.Sprintf("@%s/%s", stageName, escapeString(filePath))
	return fmt.Sprintf(`SELECT COUNT(*) FROM (
	SELECT 1
	FROM '%s'
	WHERE TO_NUMBER($4) > %d
	LIMIT 1
);`, stageFile, highWatermark)
}
