package snowflake

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"go.uber.org/zap"
)

func GetColumnModifyString(diff *tidb.ColumnDiff) string {
	strs := make([]string, 0, 3)
	if diff.Before.Tp != diff.After.Tp || diff.Before.Precision != diff.After.Precision || diff.Before.Scale != diff.After.Scale {
		colStr := buildColumn(*diff.After)
		strs = append(strs, fmt.Sprintf("COLUMN %s", colStr))
	}
	if diff.Before.Default != diff.After.Default {
		if diff.After.Default == nil {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP DEFAULT", diff.After.Name))
		} else {
			log.Warn("Snowflake does not support update column default value", zap.String("column", diff.After.Name), zap.Any("before", diff.Before.Default), zap.Any("after", diff.After.Default))
		}
	}
	if diff.Before.Nullable != diff.After.Nullable {
		if diff.After.Nullable == "true" {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP NOT NULL", diff.After.Name))
		} else {
			strs = append(strs, fmt.Sprintf("COLUMN %s SET NOT NULL", diff.After.Name))
		}
	}
	return strings.Join(strs, ", ")
}

func GenDDLViaColumnsDiff(prevColumns []cloudstorage.TableCol, curTableDef cloudstorage.TableDefinition) ([]string, error) {
	if curTableDef.Type == model.ActionTruncateTable {
		return []string{fmt.Sprintf("TRUNCATE TABLE %s", curTableDef.Table)}, nil
	}
	if curTableDef.Type == model.ActionDropTable {
		return []string{fmt.Sprintf("DROP TABLE %s", curTableDef.Table)}, nil
	}
	if curTableDef.Type == model.ActionCreateTable {
		return nil, errors.New("Received create table ddl, which should not happen") // FIXME: drop table and create table
	}
	if curTableDef.Type == model.ActionRenameTables {
		return nil, errors.New("Received rename table ddl, new change data can not be capture by TiCDC any more." +
			"If you want to rename table, please start a new task to capture the new table") // FIXME: rename table to new table and rename back
	}
	if curTableDef.Type == model.ActionDropSchema {
		return []string{fmt.Sprintf("DROP SCHEMA %s", curTableDef.Schema)}, nil
	}
	if curTableDef.Type == model.ActionCreateSchema {
		return nil, errors.New("Received create schema ddl, which should not happen") // FIXME: drop schema and create schema
	}

	columnDiff, err := tidb.GetColumnDiff(prevColumns, curTableDef.Columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddls := make([]string, 0, len(columnDiff))
	for _, item := range columnDiff {
		ddl := ""
		switch item.Action {
		case tidb.ADD_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s ADD COLUMN ", curTableDef.Table)
			colStr := buildColumn(*item.After)
			ddl += colStr
		case tidb.DROP_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", curTableDef.Table, item.Before.Name)
		case tidb.MODIFY_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s MODIFY ", curTableDef.Table)
			modifyStr := GetColumnModifyString(&item)
			ddl += modifyStr
		case tidb.RENAME_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", curTableDef.Table, item.Before.Name, item.After.Name)
		default:
			// UNCHANGE
		}
		if ddl != "" {
			ddl += ";"
			ddls = append(ddls, ddl)
		}
	}

	// TODO: handle primary key
	return ddls, nil
}

// GetSnowflakeColumnString returns a string describing the column in Snowflake, e.g.
// "id INT NOT NULL DEFAULT '0'"
// Refer to:
// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
// https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
func buildColumn(column cloudstorage.TableCol) string {
	var sb strings.Builder

	sb.WriteString(newType(column))
	if column.Nullable == "false" {
		sb.WriteString(" NOT NULL")
	}
	if column.Default != nil {
		sb.WriteString(fmt.Sprintf(` DEFAULT %s`, GetDefaultString(column.Default)))
	}
	return sb.String()
}
