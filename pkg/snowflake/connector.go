package snowflake

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"
)

type Connector struct {
	// db is the connection to snowflake.
	db *sql.DB

	stageName            string
	stageFileCompression string
}

type Option func(*Connector)

func WithStageFileCompression(compression string) Option {
	return func(sc *Connector) {
		sc.stageFileCompression = compression
	}
}

func NewConnector(sfConfig *Config, stageName string, storageURI *url.URL, credentials *credentials.Value, opts ...Option) (*Connector, error) {
	db, err := OpenDB(sfConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// create stage
	stageUrl := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)
	log.Info("creating Snowflake external stage",
		zap.String("stage", stageName),
		zap.String("url", stageUrl))
	if err := CreateExternalStage(db, stageName, stageUrl, credentials); err != nil {
		return nil, errors.Annotate(err, "Failed to create stage")
	}

	sc := &Connector{
		db:        db,
		stageName: stageName,
	}
	for _, opt := range opts {
		opt(sc)
	}
	log.Info("Snowflake connector initialized",
		zap.String("stage", stageName),
		zap.String("stageFileCompression", sc.stageFileCompression))
	return sc, nil
}

func (sc *Connector) ExecDDL(ddl string) error {
	_, err := sc.db.Exec(ddl)
	return errors.Trace(err)
}

func (sc *Connector) CopyTableSchema(tableSchema *table.Meta) error {
	createTableQuery := buildCreateSchemaSQL(tableSchema)
	_, err := sc.db.Exec(createTableQuery)
	if err != nil {
		log.Error("table in Snowflake failed", zap.String("query", createTableQuery), zap.Error(err))
		return errors.Trace(err)
	}

	log.Info("Snowflake table schema is ready",
		zap.String("sourceDatabase", tableSchema.Schema),
		zap.String("sourceTable", tableSchema.Table))
	return nil
}

func (sc *Connector) LoadSnapshot(targetTable, filePath string) error {
	if err := LoadSnapshotFromStage(sc.db, targetTable, sc.stageName, filePath, sc.stageFileCompression); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully loaded snapshot file",
		zap.String("table", targetTable),
		zap.String("file", filePath),
		zap.String("stage", sc.stageName))
	return nil
}

func (sc *Connector) LoadIncrement(tableMeta *table.Meta, filePath string) error {
	if len(tableMeta.PrimaryKeys) == 0 {
		return errors.Errorf("table %s has no primary key", tableMeta.Table)
	}
	// merge staged file into table
	mergeQuery := GenMergeInto(tableMeta, filePath, sc.stageName)
	_, err := sc.db.Exec(mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (sc *Connector) Close() {
	// drop stage
	if err := DropStage(sc.db, sc.stageName); err != nil {
		log.Error("fail to drop stage", zap.Error(err))
	} else {
		log.Info("Snowflake external stage dropped", zap.String("stage", sc.stageName))
	}
	sc.db.Close()
}
