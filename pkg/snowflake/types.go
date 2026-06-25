package snowflake

import (
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"go.uber.org/zap"
)

// TiDB2SnowflakeTypeMap is a map from TiDB type to Snowflake type.
var TiDB2SnowflakeTypeMap map[string]string = map[string]string{
	"text":       "TEXT",
	"tinytext":   "TEXT",
	"mediumtext": "TEXT",
	"longtext":   "TEXT",
	"blob":       "BINARY",
	"tinyblob":   "BINARY",
	// The maximum size of Snowflake's BINARY type is 8 MB, so can not support mediumblob and longblob.
	// "mediumblob": "TEXT",
	// "longblob":   "TEXT",
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
	"numeric":            "NUMBER",
	"bool":               "BOOLEAN",
	"boolean":            "BOOLEAN",
	"year":               "NUMBER",
	"date":               "DATE",
	"datetime":           "DATETIME",
	"timestamp":          "TIMESTAMP",
	"time":               "TIME",
	"enum":               "VARCHAR",
	"vector":             "VARCHAR",
}

func newType(column cloudstorage.TableCol) string {
	tp := strings.ToLower(column.Tp)
	columnName := quoteIdent(column.Name)
	switch tp {
	case "text", "longtext", "mediumtext", "tinytext":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	case "tinyblob", "blob":
		return fmt.Sprintf("%s %s(%s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision)
	case "longblob", "mediumblob":
		// todo: can we fix this ?
		log.Panic("The maximum size of Snowflake's BINARY type is 8 MB, so can not support mediumblob and longblob.")
	case "int", "mediumint", "bigint", "tinyint", "smallint", "float", "double", "bool", "boolean", "year", "date":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	case "int unsigned", "mediumint unsigned", "tinyint unsigned", "smallint unsigned", "bigint unsigned", "float unsigned", "double unsigned":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	case "varchar", "char", "binary", "varbinary":
		return fmt.Sprintf("%s %s(%s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision)
	case "decimal", "numeric":
		return fmt.Sprintf("%s %s(%s, %s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision, column.Scale)
	case "datetime", "timestamp", "time":
		return fmt.Sprintf("%s %s(%s)", columnName, TiDB2SnowflakeTypeMap[tp], column.Precision)
	case "enum", "vector":
		return fmt.Sprintf("%s %s", columnName, TiDB2SnowflakeTypeMap[tp])
	default:
	}
	log.Panic("unsupported data type", zap.Any("type", column.Tp))
	return ""
}
