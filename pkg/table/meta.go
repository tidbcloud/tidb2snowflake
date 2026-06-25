package table

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // register parser value expressions
	"go.uber.org/zap"
)

const SchemaFileSuffix = "-schema.sql"

func SchemaFilePath(database, table string) string {
	return fmt.Sprintf("%s.%s%s", database, table, SchemaFileSuffix)
}

type Meta struct {
	Schema      string
	Table       string
	Columns     []cloudstorage.TableCol
	PrimaryKeys []string
}

func BuildSchema(database, table, createTableDDL string) *Meta {
	p := parser.New()
	stmts, _, err := p.Parse(createTableDDL, "", "")
	if err != nil {
		log.Panic("parse tidb create table DDL", zap.Error(err))
	}
	createTableStmt := findCreateTableStmt(stmts, database, table)
	if createTableStmt == nil {
		log.Panic("create table DDL not found", zap.String("database", database), zap.String("table", table))
	}

	primaryKeys := collectPrimaryKeys(createTableStmt)
	pkColumnSet := make(map[string]struct{}, len(primaryKeys))
	for _, col := range primaryKeys {
		pkColumnSet[strings.ToLower(col)] = struct{}{}
	}

	columns := make([]cloudstorage.TableCol, 0, len(createTableStmt.Cols))
	for _, colDef := range createTableStmt.Cols {
		col, skip := tableColFromColumnDef(colDef)
		if skip {
			continue
		}
		if _, ok := pkColumnSet[strings.ToLower(col.Name)]; ok {
			col.Nullable = "false"
			col.IsPK = "true"
		} else {
			col.IsPK = "false"
		}
		columns = append(columns, col)
	}
	return &Meta{
		Schema:      database,
		Table:       table,
		Columns:     columns,
		PrimaryKeys: primaryKeys,
	}
}

func findCreateTableStmt(stmts []ast.StmtNode, database, table string) *ast.CreateTableStmt {
	for _, stmt := range stmts {
		createTableStmt, ok := stmt.(*ast.CreateTableStmt)
		if !ok {
			continue
		}
		if createTableStmt.Table.Schema.O != "" && !strings.EqualFold(createTableStmt.Table.Schema.O, database) {
			log.Panic("create table schema mismatch", zap.String("got", createTableStmt.Table.Schema.O), zap.String("want", database))
		}
		if !strings.EqualFold(createTableStmt.Table.Name.O, table) {
			log.Panic("create table name mismatch", zap.String("got", createTableStmt.Table.Name.O), zap.String("want", table))
		}
		return createTableStmt
	}
	return nil
}

func collectPrimaryKeys(stmt *ast.CreateTableStmt) []string {
	pkColumns := make([]string, 0)
	for _, colDef := range stmt.Cols {
		for _, opt := range colDef.Options {
			if opt.Tp != ast.ColumnOptionPrimaryKey {
				continue
			}
			pkColumns = append(pkColumns, colDef.Name.Name.O)
		}
	}
	for _, constraint := range stmt.Constraints {
		if constraint.Tp != ast.ConstraintPrimaryKey {
			continue
		}
		for _, key := range constraint.Keys {
			if key.Column == nil {
				continue
			}
			pkColumns = append(pkColumns, key.Column.Name.O)
		}
	}
	return pkColumns
}

func tableColFromColumnDef(colDef *ast.ColumnDef) (cloudstorage.TableCol, bool) {
	col := cloudstorage.TableCol{
		Name:     colDef.Name.Name.O,
		Nullable: "true",
	}

	tp, precision, scale := columnType(colDef.Tp)
	col.Tp = tp
	col.Precision = precision
	col.Scale = scale
	if mysql.HasNotNullFlag(colDef.Tp.GetFlag()) {
		col.Nullable = "false"
	}

	for _, opt := range colDef.Options {
		switch opt.Tp {
		case ast.ColumnOptionPrimaryKey, ast.ColumnOptionNotNull:
			col.Nullable = "false"
		case ast.ColumnOptionNull:
			col.Nullable = "true"
		case ast.ColumnOptionDefaultValue:
			defaultValue, err := columnDefault(opt.Expr)
			if err != nil {
				log.Panic("parse default value for column", zap.String("column", col.Name), zap.Error(err))
			}
			col.Default = defaultValue
		case ast.ColumnOptionGenerated:
			if !opt.Stored {
				return col, true
			}
		}
	}
	return col, false
}

func columnType(ft *parsertypes.FieldType) (string, string, string) {
	baseType := parsertypes.TypeToStr(ft.GetType(), ft.GetCharset())
	if mysql.HasIsBooleanFlag(ft.GetFlag()) && ft.GetType() == mysql.TypeTiny {
		baseType = "bool"
	}
	if mysql.HasUnsignedFlag(ft.GetFlag()) && ft.GetType() != mysql.TypeBit && ft.GetType() != mysql.TypeYear {
		baseType += " unsigned"
	}

	switch baseType {
	case "decimal", "numeric":
		return baseType, intString(defaultedFlen(ft)), intString(defaultedDecimal(ft))
	case "varchar", "char", "binary", "varbinary":
		return baseType, intString(defaultedFlen(ft)), ""
	case "datetime", "timestamp", "time":
		return baseType, intString(defaultedDecimal(ft)), ""
	case "tinyblob":
		return baseType, "255", ""
	case "blob":
		return baseType, "65535", ""
	default:
		return baseType, intString(defaultedFlen(ft)), intString(defaultedDecimal(ft))
	}
}

func defaultedFlen(ft *parsertypes.FieldType) int {
	if ft.GetFlen() != parsertypes.UnspecifiedLength {
		return ft.GetFlen()
	}
	flen, _ := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
	return flen
}

func defaultedDecimal(ft *parsertypes.FieldType) int {
	if ft.GetDecimal() != parsertypes.UnspecifiedLength {
		return ft.GetDecimal()
	}
	_, decimal := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
	return decimal
}

func intString(v int) string {
	if v == parsertypes.UnspecifiedLength {
		return ""
	}
	return fmt.Sprintf("%d", v)
}

// DefaultExpression marks a parsed SQL expression used by a DEFAULT clause.
type DefaultExpression string

func (e DefaultExpression) SQLExpression() string {
	return string(e)
}

func columnDefault(expr ast.ExprNode) (any, error) {
	if expr == nil {
		return nil, nil
	}
	if valueExpr, ok := expr.(ast.ValueExpr); ok {
		value := valueExpr.GetValue()
		if value == nil {
			return nil, nil
		}
		return value, nil
	}
	exprSQL, err := restoreExpr(expr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return DefaultExpression(exprSQL), nil
}

func restoreExpr(expr ast.ExprNode) (string, error) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset|format.RestoreStringWithoutDefaultCharset, &sb)
	if err := expr.Restore(restoreCtx); err != nil {
		return "", errors.Trace(err)
	}
	return sb.String(), nil
}
