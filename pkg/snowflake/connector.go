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

const (
	SnapshotStageName  = "snapshot_external"
	IncrementStageName = "increment_external"
)

type Connector struct {
	// db is the connection to snowflake.
	db *sql.DB

	stageName            string
	stageFileCompression string
}

func NewConnector(sfConfig *Config, stageName string, storageURI *url.URL, credentials *credentials.Value, stageFileCompression string) (*Connector, error) {
	db, err := OpenDB(sfConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	// create stage
	stageUrl := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)
	if err := createExternalStage(db, stageName, stageUrl, credentials); err != nil {
		log.Error("snowflake connector failed on create external stage", zap.String("stageUrl", stageUrl), zap.Error(err))
		return nil, errors.Annotate(err, "Failed to create stage")
	}

	sc := &Connector{
		db:                   db,
		stageName:            stageName,
		stageFileCompression: stageFileCompression,
	}
	log.Info("Snowflake connector initialized", zap.String("stage", stageName),
		zap.String("stageUrl", stageUrl), zap.String("compression", sc.stageFileCompression))
	return sc, nil
}

func (sc *Connector) ExecDDL(ddl string) error {
	_, err := sc.db.Exec(ddl)
	return errors.Trace(err)
}

func (sc *Connector) CreateTable(tableSchema *table.Meta) error {
	createTableSQL := buildCreateTableSQL(tableSchema)
	_, err := sc.db.Exec(createTableSQL)
	if err != nil {
		log.Error("table in Snowflake failed", zap.String("query", createTableSQL), zap.Error(err))
		return errors.Trace(err)
	}

	log.Info("Snowflake table schema is ready",
		zap.String("sourceDatabase", tableSchema.Schema),
		zap.String("sourceTable", tableSchema.Table))
	return nil
}

func (sc *Connector) LoadSnapshot(targetTable, filePath string) error {
	fileFormat := snapshotFileFormat(sc.stageFileCompression)
	query := fmt.Sprintf(`COPY INTO %s FROM @%s FILES = ('%s') FILE_FORMAT = (%s);`,
		quoteIdent(targetTable), sc.stageName, escapeString(filePath), fileFormat)
	_, err := sc.db.Exec(query)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (sc *Connector) LoadIncrement(tableMeta *table.Meta, filePath string, highWatermark uint64) (bool, error) {
	if len(tableMeta.PrimaryKeys) == 0 {
		return false, errors.Errorf("table %s has no primary key", tableMeta.Table)
	}
	// merge staged file into table
	mergeQuery := genMergeIntoSQL(tableMeta, filePath, sc.stageName, highWatermark)
	_, err := sc.db.Exec(mergeQuery)
	if err != nil {
		return false, errors.Trace(err)
	}
	fullyConsumed, err := sc.isFullyConsumed(filePath, highWatermark)
	if err != nil {
		return false, errors.Trace(err)
	}
	return fullyConsumed, nil
}

func (sc *Connector) isFullyConsumed(filePath string, highWatermark uint64) (bool, error) {
	var count int
	query := genCountCommitTsAfter(filePath, sc.stageName, highWatermark)
	if err := sc.db.QueryRow(query).Scan(&count); err != nil {
		return false, errors.Trace(err)
	}
	return count == 0, nil
}

func (sc *Connector) Close() {
	// drop stage
	if err := dropStage(sc.db, sc.stageName); err != nil {
		log.Error("fail to drop stage", zap.Error(err))
	} else {
		log.Info("Snowflake external stage dropped", zap.String("stage", sc.stageName))
	}
	sc.db.Close()
}
