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
		return []string{fmt.Sprintf("DROP TABLE %s", quoteIdent(nextMeta.SnowflakeTableName()))}, nil
	case model.ActionCreateTable:
		return nil, errors.New("Received create table ddl, which should not happen")
	case model.ActionRenameTable:
		if strings.TrimSpace(query) == "" {
			return []string{renameTableDDL(prevMeta.SnowflakeTableName(), nextMeta.SnowflakeTableName())}, nil
		}
	case model.ActionDropSchema:
		return []string{fmt.Sprintf("DROP SCHEMA %s", quoteIdent(nextMeta.Schema))}, nil
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
		return genAlterTableDDLs(prevMeta, nextMeta, stmt), nil
	case *ast.RenameTableStmt:
		return genRenameTableDDLs(prevMeta, nextMeta, stmt), nil
	default:
		return nil, errors.Errorf("unsupported TiDB DDL query %T: %s", stmt, query)
	}
}

func genAlterTableDDLs(prevMeta, nextMeta *table.Meta, alterStmt *ast.AlterTableStmt) []string {
	added, removed, before, after := diffColumns(prevMeta.Columns, nextMeta.Columns)
	tableName := quoteIdent(nextMeta.SnowflakeTableName())
	ddls := make([]string, 0, len(alterStmt.Specs))
	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			for _, col := range added {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", tableName, buildColumn(col)))
			}
		case ast.AlterTableDropColumn:
			for _, col := range removed {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableName, quoteIdent(col.Name)))
			}
		case ast.AlterTableRenameColumn:
			oldCol := removed[0]
			newCol := added[0]
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, quoteIdent(oldCol.Name), quoteIdent(newCol.Name)))
		case ast.AlterTableModifyColumn, ast.AlterTableAlterColumn:
			ddls = append(ddls, modifyColumnDDLs(tableName, before, after)...)
		case ast.AlterTableChangeColumn:
			if len(removed) == 0 || len(added) == 0 {
				ddls = append(ddls, modifyColumnDDLs(tableName, before, after)...)
				continue
			}
			oldCol := removed[0]
			newCol := added[0]
			if !strings.EqualFold(oldCol.Name, newCol.Name) {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, quoteIdent(oldCol.Name), quoteIdent(newCol.Name)))
			}
			if modify := columnModifyString(oldCol, newCol); modify != "" {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s MODIFY %s;", tableName, modify))
			}
		case ast.AlterTableRenameTable:
			ddls = append(ddls, renameTableDDL(
				astTableName(alterStmt.Table, prevMeta.Schema),
				astTableName(spec.NewTable, nextMeta.Schema),
			))
		default:
			continue
		}
	}
	return ddls
}

func genRenameTableDDLs(prevMeta, nextMeta *table.Meta, stmt *ast.RenameTableStmt) []string {
	ddls := make([]string, 0, len(stmt.TableToTables))
	oldDefaultSchema := nextMeta.Schema
	if prevMeta != nil {
		oldDefaultSchema = prevMeta.Schema
	}
	for _, tableToTable := range stmt.TableToTables {
		ddls = append(ddls, renameTableDDL(
			astTableName(tableToTable.OldTable, oldDefaultSchema),
			astTableName(tableToTable.NewTable, nextMeta.Schema),
		))
	}
	return ddls
}

func renameTableDDL(oldTable, newTable string) string {
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", quoteIdent(oldTable), quoteIdent(newTable))
}

func astTableName(tableName *ast.TableName, defaultSchema string) string {
	schema := tableName.Schema.O
	if schema == "" {
		schema = defaultSchema
	}
	return fmt.Sprintf("%s.%s", schema, tableName.Name.O)
}

func modifyColumnDDLs(tableName string, beforeColumns, afterColumns []table.Column) []string {
	ddls := make([]string, 0, len(beforeColumns))
	for i, before := range beforeColumns {
		modify := columnModifyString(before, afterColumns[i])
		if modify != "" {
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s MODIFY %s;", tableName, modify))
		}
	}
	return ddls
}

func diffColumns(prevColumns, nextColumns []table.Column) (
	added []table.Column,
	removed []table.Column,
	before []table.Column,
	after []table.Column,
) {
	nextByName := make(map[string]table.Column, len(nextColumns))
	for _, col := range nextColumns {
		nextByName[strings.ToLower(col.Name)] = col
	}

	prevNames := make(map[string]struct{}, len(prevColumns))
	for _, prev := range prevColumns {
		name := strings.ToLower(prev.Name)
		prevNames[name] = struct{}{}
		if next, ok := nextByName[name]; ok {
			before = append(before, prev)
			after = append(after, next)
		} else {
			removed = append(removed, prev)
		}
	}

	for _, next := range nextColumns {
		if _, ok := prevNames[strings.ToLower(next.Name)]; !ok {
			added = append(added, next)
		}
	}
	return
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
