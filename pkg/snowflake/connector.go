package snowflake

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"
)

type Connector struct {
	// db is the connection to snowflake.
	db             *sql.DB
	TargetDatabase string
}

func NewConnector(sfConfig *Config) (*Connector, error) {
	db, err := OpenDB(sfConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Connector{db: db, TargetDatabase: sfConfig.Database}, nil
}

func (sc *Connector) ExecDDL(ctx context.Context, ddl string) error {
	_, err := sc.db.ExecContext(ctx, ddl)
	return errors.Trace(err)
}

func (sc *Connector) CreateSchema(ctx context.Context, sourceDatabase string) error {
	_, err := sc.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", quoteQualifiedIdent(sc.TargetDatabase, sourceDatabase)))
	return errors.Trace(err)
}

func (sc *Connector) CreateTable(ctx context.Context, tableSchema *table.Meta) error {
	createTableSQL := buildCreateTableSQL(sc.TargetDatabase, tableSchema)
	_, err := sc.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		log.Error("snowflake create table failed", zap.String("query", createTableSQL), zap.Error(err))
	}
	return errors.Trace(err)
}

func (sc *Connector) LoadSnapshot(ctx context.Context, sourceDatabase, sourceTable, filePath, compression string) error {
	fileFormat := snapshotFileFormat(compression)
	query := fmt.Sprintf(`COPY INTO %s FROM @%s FILES = ('%s') FILE_FORMAT = (%s);`,
		quoteQualifiedIdent(sc.TargetDatabase, sourceDatabase, sourceTable),
		quoteQualifiedIdent(sc.TargetDatabase, InternalSchemaName, ExternalStageName),
		escapeString(filePath),
		fileFormat)
	_, err := sc.db.ExecContext(ctx, query)
	return errors.Trace(err)
}

func (sc *Connector) LoadIncrement(ctx context.Context, tableMeta *table.Meta, filePath string, checkpointTs uint64) error {
	if len(tableMeta.PrimaryKeys) == 0 {
		return errors.Errorf("table %s has no primary key", tableMeta.Table)
	}
	// merge staged file into table
	mergeQuery := genMergeIntoSQL(sc.TargetDatabase, tableMeta, filePath, checkpointTs)
	result, err := sc.db.ExecContext(ctx, mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}

	rowsAffected := int64(-1)
	if result != nil {
		affected, err := result.RowsAffected()
		if err != nil {
			log.Warn("failed to read Snowflake increment rows affected",
				zap.String("filePath", filePath),
				zap.Uint64("checkpointTsUsedByMerge", checkpointTs),
				zap.Error(err))
		} else {
			rowsAffected = affected
		}
	}
	log.Info("DML file loaded into data warehouse",
		zap.String("filePath", filePath),
		zap.Uint64("checkpointTsUsedByMerge", checkpointTs),
		zap.Int64("rowsAffected", rowsAffected))
	return nil
}

func (sc *Connector) Close() {
	sc.db.Close()
}
