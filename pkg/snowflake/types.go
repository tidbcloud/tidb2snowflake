package snowflake

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"
)

// TiDB2SnowflakeTypeMap is a map from TiDB type to Snowflake type.
var TiDB2SnowflakeTypeMap map[string]string = map[string]string{
	"text":               "TEXT",
	"tinytext":           "TEXT",
	"mediumtext":         "TEXT",
	"longtext":           "TEXT",
	"blob":               "BINARY",
	"tinyblob":           "BINARY",
	"mediumblob":         "BINARY",
	"longblob":           "BINARY",
	"varchar":            "VARCHAR",
	"char":               "CHAR",
	"binary":             "BINARY",
	"varbinary":          "BINARY",
	"tinyint":            "NUMBER",
	"smallint":           "NUMBER",
	"int":                "NUMBER",
	"mediumint":          "NUMBER",
	"bigint":             "NUMBER",
	"tinyint unsigned":   "NUMBER",
	"smallint unsigned":  "NUMBER",
	"int unsigned":       "NUMBER",
	"mediumint unsigned": "NUMBER",
	"bigint unsigned":    "NUMBER",
	"float":              "FLOAT",
	"float unsigned":     "FLOAT",
	"double":             "FLOAT",
	"double unsigned":    "FLOAT",
	"decimal":            "NUMBER",
	"decimal unsigned":   "NUMBER",
	"numeric":            "NUMBER",
	"numeric unsigned":   "NUMBER",
	"bool":               "BOOLEAN",
	"boolean":            "BOOLEAN",
	"bit":                "NUMBER",
	"year":               "NUMBER",
	"date":               "DATE",
	"datetime":           "DATETIME",
	"timestamp":          "TIMESTAMP",
	"time":               "TIME",
	"enum":               "VARCHAR",
	"set":                "VARCHAR",
	"json":               "VARCHAR",
	"vector":             "VARCHAR",
}

func newType(column table.Column) string {
	tp := strings.ToLower(column.Tp)
	columnName := quoteIdent(column.Name)
	switch tp {
	case "text", "longtext", "mediumtext", "tinytext":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	case "tinyblob", "blob", "mediumblob", "longblob":
		return fmt.Sprintf("%s %s(%s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision)
	case "int", "mediumint", "bigint", "tinyint", "smallint", "float", "double", "bool", "boolean", "bit", "year", "date":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	case "int unsigned", "mediumint unsigned", "tinyint unsigned", "smallint unsigned", "bigint unsigned", "float unsigned", "double unsigned":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	case "varchar", "char", "binary", "varbinary":
		return fmt.Sprintf("%s %s(%s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision)
	case "decimal", "numeric":
		return fmt.Sprintf("%s %s(%s, %s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision, column.Scale)
	case "decimal unsigned", "numeric unsigned":
		precision, err := strconv.Atoi(column.Precision)
		if err == nil && precision > 38 {
			return fmt.Sprintf("%s VARCHAR", columnName)
		}
		return fmt.Sprintf("%s %s(%s, %s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision, column.Scale)
	case "datetime", "timestamp", "time":
		return fmt.Sprintf("%s %s(%s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision)
	case "enum", "set", "json", "vector":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	default:
	}
	log.Panic("unsupported data type", zap.Any("type", column.Tp))
	return ""
}
