package snowflake

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"gitlab.com/tymonx/go-formatter/formatter"
)

func CreateExternalStage(db *sql.DB, stageName, s3WorkspaceURL string, cred *credentials.Value) error {
	sql, err := formatter.Format(`
CREATE OR REPLACE STAGE {stageName}
URL = '{url}'
CREDENTIALS = (AWS_KEY_ID = '{awsKeyId}' AWS_SECRET_KEY = '{awsSecretKey}' AWS_TOKEN = '{awsToken}')
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE='\\' BINARY_FORMAT = 'HEX');
	`, formatter.Named{
		"stageName":    utils.EscapeString(stageName),
		"url":          utils.EscapeString(s3WorkspaceURL),
		"awsKeyId":     utils.EscapeString(cred.AccessKeyID),
		"awsSecretKey": utils.EscapeString(cred.SecretAccessKey),
		"awsToken":     utils.EscapeString(cred.SessionToken),
	})
	if err != nil {
		return err
	}
	_, err = db.Exec(sql)
	return err
}

func DropStage(db *sql.DB, stageName string) error {
	sql, err := formatter.Format(`
DROP STAGE IF EXISTS {stageName};
`, formatter.Named{
		"stageName": utils.EscapeString(stageName),
	})
	if err != nil {
		return errors.Trace(err)
	}
	_, err = db.Exec(sql)
	return err
}

func LoadSnapshotFromStage(db *sql.DB, targetTable, stageName, filePath string, compression ...string) error {
	fileFormat := snapshotFileFormat(compressionValue(compression))
	if strings.Contains(filePath, "*") {
		return LoadSnapshotFromStagePattern(db, targetTable, stageName, stagePatternFromGlob(filePath), compression...)
	}
	sql, err := formatter.Format(`
COPY INTO {targetTable}
FROM @{stageName}/{filePath}
FILE_FORMAT = ({fileFormat});
`, formatter.Named{
		"targetTable": utils.EscapeString(targetTable),
		"stageName":   utils.EscapeString(stageName),
		"filePath":    utils.EscapeString(filePath),
		"fileFormat":  fileFormat,
	})
	if err != nil {
		return errors.Trace(err)
	}
	_, err = db.Exec(sql)
	return err
}

func LoadSnapshotFromStagePattern(db *sql.DB, targetTable, stageName, pattern string, compression ...string) error {
	fileFormat := snapshotFileFormat(compressionValue(compression))
	sql, err := formatter.Format(`
COPY INTO {targetTable}
FROM @{stageName}
FILE_FORMAT = ({fileFormat})
PATTERN = '{pattern}';
`, formatter.Named{
		"targetTable": utils.EscapeString(targetTable),
		"stageName":   utils.EscapeString(stageName),
		"fileFormat":  fileFormat,
		"pattern":     utils.EscapeString(pattern),
	})
	if err != nil {
		return errors.Trace(err)
	}
	_, err = db.Exec(sql)
	return err
}

func compressionValue(compression []string) string {
	if len(compression) == 0 || compression[0] == "" {
		return "none"
	}
	return compression[0]
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
	switch strings.ToLower(compression) {
	case "gzip":
		parts = append(parts, "COMPRESSION = 'GZIP'")
	default:
		parts = append(parts, "COMPRESSION = 'NONE'")
	}
	return strings.Join(parts, " ")
}

func stagePatternFromGlob(glob string) string {
	pattern := regexp.QuoteMeta(glob)
	pattern = strings.ReplaceAll(pattern, `\*`, ".*")
	return ".*" + pattern
}

type defaultSQLExpression interface {
	SQLExpression() string
}

func GetDefaultString(val any) string {
	if expr, ok := val.(defaultSQLExpression); ok {
		return expr.SQLExpression()
	}
	_, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
	if err != nil {
		return fmt.Sprintf("'%v'", val) // FIXME: escape
	}
	return fmt.Sprintf("%v", val)
}

func buildCreateSchemaSQL(tableSchema *table.Meta) string {
	columns := make([]string, 0, len(tableSchema.Columns))
	for _, column := range tableSchema.Columns {
		columns = append(columns, buildColumn(column))
	}

	sqlRows := make([]string, 0, len(columns)+1)
	sqlRows = append(sqlRows, columns...)
	if len(tableSchema.PrimaryKeys) > 0 {
		sqlRows = append(sqlRows, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(tableSchema.PrimaryKeys, ", ")))
	}
	// Add idents
	for i := 0; i < len(sqlRows); i++ {
		sqlRows[i] = fmt.Sprintf("    %s", sqlRows[i])
	}

	sql := []string{}
	sql = append(sql, fmt.Sprintf(`CREATE OR REPLACE TABLE %s (`, tableSchema.Table)) // TODO: Escape
	sql = append(sql, strings.Join(sqlRows, ",\n"))
	sql = append(sql, ")")

	return strings.Join(sql, "\n")
}

func GenMergeInto(tableDef cloudstorage.TableDefinition, filePath string, stageName string) string {
	selectStat := make([]string, 0, len(tableDef.Columns)+1)
	selectStat = append(selectStat, `$1 AS "METADATA$FLAG"`)
	for i, col := range tableDef.Columns {
		if TiDB2SnowflakeTypeMap[strings.ToLower(col.Tp)] == "BINARY" {
			selectStat = append(selectStat, fmt.Sprintf(`TO_BINARY($%d, 'HEX') AS %s`, i+5, col.Name))
		} else {
			selectStat = append(selectStat, fmt.Sprintf(`$%d AS %s`, i+5, col.Name))
		}
	}

	pkColumn := make([]string, 0)
	onStat := make([]string, 0)
	for _, col := range tableDef.Columns {
		if col.IsPK == "true" {
			pkColumn = append(pkColumn, col.Name)
			onStat = append(onStat, fmt.Sprintf(`T.%s = S.%s`, col.Name, col.Name))
		}
	}

	updateStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		updateStat = append(updateStat, fmt.Sprintf(`%s = S.%s`, col.Name, col.Name))
	}

	insertStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		insertStat = append(insertStat, col.Name)
	}

	valuesStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		valuesStat = append(valuesStat, fmt.Sprintf(`S.%s`, col.Name))
	}

	// TODO: Remove QUALIFY row_number() after cdc support merge dml or snowflake support deterministic merge
	mergeQuery := fmt.Sprintf(
		`MERGE INTO %s AS T USING
		(
			SELECT
				%s
			FROM '@%s/%s'
			QUALIFY row_number() over (partition by %s order by $4 desc) = 1
		) AS S
		ON
		(
			%s
		)
		WHEN MATCHED AND S.METADATA$FLAG != 'D' THEN UPDATE SET %s
		WHEN MATCHED AND S.METADATA$FLAG = 'D' THEN DELETE
		WHEN NOT MATCHED AND S.METADATA$FLAG != 'D' THEN INSERT (%s) VALUES (%s);`,
		tableDef.Table,
		strings.Join(selectStat, ",\n"),
		stageName,
		filePath,
		strings.Join(pkColumn, ", "),
		strings.Join(onStat, " AND "),
		strings.Join(updateStat, ", "),
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "))

	return mergeQuery
}
