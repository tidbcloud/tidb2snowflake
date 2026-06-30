package snowflake

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"
)

func columnModifyString(before, after table.Column) string {
	strs := make([]string, 0, 3)
	if before.Tp != after.Tp || before.Precision != after.Precision || before.Scale != after.Scale {
		colStr := buildColumn(after)
		strs = append(strs, fmt.Sprintf("COLUMN %s", colStr))
	}
	if !reflect.DeepEqual(before.Default, after.Default) {
		if after.Default == nil {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP DEFAULT", quoteIdent(after.Name)))
		} else {
			log.Warn("Snowflake does not support update column default value", zap.String("column", after.Name), zap.Any("before", before.Default), zap.Any("after", after.Default))
		}
	}
	if before.Nullable != after.Nullable {
		if after.Nullable == "true" {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP NOT NULL", quoteIdent(after.Name)))
		} else {
			strs = append(strs, fmt.Sprintf("COLUMN %s SET NOT NULL", quoteIdent(after.Name)))
		}
	}
	return strings.Join(strs, ", ")
}

func GenDDLViaTiDBDDL(prevMeta, nextMeta *table.Meta, action model.ActionType, query string) ([]string, error) {
	switch action {
	case model.ActionTruncateTable:
		return []string{fmt.Sprintf("TRUNCATE TABLE %s", quoteIdent(nextMeta.SnowflakeTableName()))}, nil
	case model.ActionDropTable:
		return []string{fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(nextMeta.SnowflakeTableName()))}, nil
	case model.ActionCreateTable:
		return nil, errors.New("Received create table ddl, which should not happen")
	case model.ActionRenameTable:
		return nil, errors.New("rename table ddl is not supported")
	case model.ActionDropSchema:
		log.Warn("ignore unsupported TiDB drop schema DDL",
			zap.String("schema", nextMeta.Schema),
			zap.String("query", query))
		return nil, nil
	case model.ActionCreateSchema:
		return nil, errors.New("Received create schema ddl, which should not happen")
	default:
		// continue
	}
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}

	stmt, err := parser.New().ParseOneStmt(query, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch stmt := stmt.(type) {
	case *ast.AlterTableStmt:
		return genAlterTableDDLs(prevMeta, nextMeta, stmt)
	default:
		return nil, errors.Errorf("unsupported TiDB DDL query %T: %s", stmt, query)
	}
}

func genAlterTableDDLs(prevMeta, nextMeta *table.Meta, alterStmt *ast.AlterTableStmt) ([]string, error) {
	tableName := quoteIdent(nextMeta.SnowflakeTableName())
	ddls := make([]string, 0, len(alterStmt.Specs))
	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			for _, colDef := range spec.NewColumns {
				col, err := columnInMeta(nextMeta, colDef.Name.Name.O)
				if err != nil {
					return nil, errors.Trace(err)
				}
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", tableName, buildColumn(col)))
			}
		case ast.AlterTableDropColumn:
			if _, err := columnInMeta(prevMeta, spec.OldColumnName.Name.O); err != nil {
				return nil, errors.Trace(err)
			}
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableName, quoteIdent(spec.OldColumnName.Name.O)))
		case ast.AlterTableRenameColumn:
			if _, err := columnInMeta(prevMeta, spec.OldColumnName.Name.O); err != nil {
				return nil, errors.Trace(err)
			}
			if _, err := columnInMeta(nextMeta, spec.NewColumnName.Name.O); err != nil {
				return nil, errors.Trace(err)
			}
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;",
				tableName,
				quoteIdent(spec.OldColumnName.Name.O),
				quoteIdent(spec.NewColumnName.Name.O)))
		case ast.AlterTableModifyColumn, ast.AlterTableAlterColumn:
			before, after, err := modifiedColumns(prevMeta, nextMeta, spec.NewColumns[0].Name.Name.O, spec.NewColumns[0].Name.Name.O)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if modify := columnModifyString(before, after); modify != "" {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s MODIFY %s;", tableName, modify))
			}
		case ast.AlterTableChangeColumn:
			oldCol, newCol, err := modifiedColumns(prevMeta, nextMeta, spec.OldColumnName.Name.O, spec.NewColumns[0].Name.Name.O)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !strings.EqualFold(oldCol.Name, newCol.Name) {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, quoteIdent(oldCol.Name), quoteIdent(newCol.Name)))
			}
			if modify := columnModifyString(oldCol, newCol); modify != "" {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s MODIFY %s;", tableName, modify))
			}
		case ast.AlterTableRenameTable:
			return nil, errors.New("rename table ddl is not supported")
		default:
			continue
		}
	}
	return ddls, nil
}

func modifiedColumns(prevMeta, nextMeta *table.Meta, prevName, nextName string) (table.Column, table.Column, error) {
	before, err := columnInMeta(prevMeta, prevName)
	if err != nil {
		return table.Column{}, table.Column{}, errors.Trace(err)
	}
	after, err := columnInMeta(nextMeta, nextName)
	if err != nil {
		return table.Column{}, table.Column{}, errors.Trace(err)
	}
	return before, after, nil
}

func columnInMeta(meta *table.Meta, name string) (table.Column, error) {
	if meta == nil {
		return table.Column{}, errors.Errorf("column %s not found", name)
	}
	for _, col := range meta.Columns {
		if strings.EqualFold(col.Name, name) {
			return col, nil
		}
	}
	return table.Column{}, errors.Errorf("column %s not found in table %s", name, meta.SnowflakeTableName())
}

// GetSnowflakeColumnString returns a string describing the column in Snowflake, e.g.
// "id INT NOT NULL DEFAULT '0'"
// Refer to:
// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
// https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
func buildColumn(column table.Column) string {
	var sb strings.Builder

	sb.WriteString(newType(column))
	if column.Nullable == "false" {
		sb.WriteString(" NOT NULL")
	}
	if column.Default != nil {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(defaultString(column.Default))
	}
	return sb.String()
}
