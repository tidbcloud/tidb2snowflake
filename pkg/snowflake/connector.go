package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"
)

const (
	SnapshotStageName  = "snapshot_external"
	IncrementStageName = "increment_external"
)

type Connector struct {
	// db is the connection to snowflake.
	db *sql.DB
}

func NewConnector(sfConfig *Config) (*Connector, error) {
	db, err := OpenDB(sfConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Connector{db: db}, nil
}

func (sc *Connector) CreateStage(ctx context.Context, stageName string, storageURI *url.URL, cred *credentials.Value) error {
	stageUrl := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)
	if err := createExternalStage(ctx, sc.db, stageName, stageUrl, cred); err != nil {
		log.Error("Snowflake external stage creation failed",
			zap.String("stage", stageName),
			zap.String("stageUrl", stageUrl),
			zap.Error(err))
		return errors.Annotate(err, "Failed to create stage")
	}
	log.Info("Snowflake external stage created",
		zap.String("stage", stageName),
		zap.String("stageUrl", stageUrl))
	return nil
}

func (sc *Connector) DropStage(ctx context.Context, stageName string) {
	if err := dropStage(ctx, sc.db, stageName); err != nil {
		log.Error("snowflake drop external stage failed", zap.String("stage", stageName), zap.Error(err))
		return
	}
	log.Info("snowflake external stage dropped", zap.String("stage", stageName))
}

func (sc *Connector) ExecDDL(ctx context.Context, ddl string) error {
	_, err := sc.db.ExecContext(ctx, ddl)
	return errors.Trace(err)
}

func (sc *Connector) CreateTable(ctx context.Context, tableSchema *table.Meta) error {
	createTableSQL := buildCreateTableSQL(tableSchema)
	_, err := sc.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		log.Error("snowflake create table failed", zap.String("query", createTableSQL), zap.Error(err))
	}
	return errors.Trace(err)
}

func (sc *Connector) LoadSnapshot(ctx context.Context, targetTable, filePath, compression string) error {
	fileFormat := snapshotFileFormat(compression)
	query := fmt.Sprintf(`COPY INTO %s FROM @%s FILES = ('%s') FILE_FORMAT = (%s);`,
		quoteIdent(targetTable), SnapshotStageName, escapeString(filePath), fileFormat)
	_, err := sc.db.ExecContext(ctx, query)
	return errors.Trace(err)
}

func (sc *Connector) LoadIncrement(ctx context.Context, tableMeta *table.Meta, filePath string, checkpointTs uint64) error {
	if len(tableMeta.PrimaryKeys) == 0 {
		return errors.Errorf("table %s has no primary key", tableMeta.Table)
	}
	// merge staged file into table
	mergeQuery := genMergeIntoSQL(tableMeta, filePath, IncrementStageName, checkpointTs)
	_, err := sc.db.ExecContext(ctx, mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (sc *Connector) Close() {
	sc.db.Close()
}
