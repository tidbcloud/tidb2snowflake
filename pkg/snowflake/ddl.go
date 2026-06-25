package snowflake

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
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
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP DEFAULT", quoteIdent(diff.After.Name)))
		} else {
			log.Warn("Snowflake does not support update column default value", zap.String("column", diff.After.Name), zap.Any("before", diff.Before.Default), zap.Any("after", diff.After.Default))
		}
	}
	if diff.Before.Nullable != diff.After.Nullable {
		if diff.After.Nullable == "true" {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP NOT NULL", quoteIdent(diff.After.Name)))
		} else {
			strs = append(strs, fmt.Sprintf("COLUMN %s SET NOT NULL", quoteIdent(diff.After.Name)))
		}
	}
	return strings.Join(strs, ", ")
}

func GenDDLViaMetaDiff(prevMeta, nextMeta *table.Meta, action model.ActionType) ([]string, error) {
	switch action {
	case model.ActionTruncateTable:
		return []string{fmt.Sprintf("TRUNCATE TABLE %s", quoteIdent(nextMeta.Table))}, nil
	case model.ActionDropTable:
		return []string{fmt.Sprintf("DROP TABLE %s", quoteIdent(nextMeta.Table))}, nil
	case model.ActionCreateTable:
		return nil, errors.New("Received create table ddl, which should not happen") // FIXME: drop table and create table
	case model.ActionRenameTable:
		return nil, errors.New("Received rename table ddl, which should not happen") // FIXME: rename table to new table and rename back
	case model.ActionDropSchema:
		return []string{fmt.Sprintf("DROP SCHEMA %s", quoteIdent(nextMeta.Schema))}, nil
	case model.ActionCreateSchema:
		return nil, errors.New("Received create schema ddl, which should not happen") // FIXME: drop schema and create schema
	default:
		// continue
	}
	if prevMeta == nil {
		return nil, errors.New("previous table metadata is empty")
	}

	columnDiff, err := tidb.GetColumnDiff(prevMeta.Columns, nextMeta.Columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddls := make([]string, 0, len(columnDiff))
	for _, item := range columnDiff {
		ddl := ""
		switch item.Action {
		case tidb.ADD_COLUMN:
			ddl = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", quoteIdent(nextMeta.Table), buildColumn(*item.After))
		case tidb.DROP_COLUMN:
			ddl = fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quoteIdent(nextMeta.Table), quoteIdent(item.Before.Name))
		case tidb.MODIFY_COLUMN:
			ddl = fmt.Sprintf("ALTER TABLE %s MODIFY %s", quoteIdent(nextMeta.Table), GetColumnModifyString(&item))
		case tidb.RENAME_COLUMN:
			ddl = fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", quoteIdent(nextMeta.Table), quoteIdent(item.Before.Name), quoteIdent(item.After.Name))
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
