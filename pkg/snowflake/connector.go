package snowflake

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"go.uber.org/zap"
)

// A Wrapper of snowflake connection.
// It implements the coreinterfaces.Connector interface.
type Connector struct {
	// db is the connection to snowflake.
	db *sql.DB

	stageName            string
	stageFileCompression string

	s3Credentials *credentials.Value

	columns []cloudstorage.TableCol
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
		db:            db,
		stageName:     stageName,
		s3Credentials: credentials,
		columns:       nil,
	}
	for _, opt := range opts {
		opt(sc)
	}
	log.Info("Snowflake connector initialized",
		zap.String("stage", stageName),
		zap.String("stageFileCompression", sc.stageFileCompression))
	return sc, nil
}

func (sc *Connector) InitSchema(columns []cloudstorage.TableCol) error {
	if len(sc.columns) != 0 {
		return nil
	}
	if len(columns) == 0 {
		return errors.New("Columns in schema is empty")
	}
	sc.columns = columns
	log.Info("table columns initialized",
		zap.Int("columnCount", len(columns)),
		zap.Strings("columns", tableColumnNames(columns)))
	return nil
}

func (sc *Connector) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if len(sc.columns) == 0 {
		return errors.New("Columns not initialized. Maybe you execute a DDL before all DMLs, which is not supported now.")
	}
	ddls, err := GenDDLViaColumnsDiff(sc.columns, tableDef)
	if err != nil {
		return errors.Trace(err)
	}
	if len(ddls) == 0 {
		log.Info("No need to execute this DDL in Snowflake",
			zap.String("ddl", tableDef.Query),
			zap.Uint64("tableVersion", tableDef.TableVersion))
		return nil
	}
	// One DDL may be rewritten to multiple DDLs
	for _, ddl := range ddls {
		_, err := sc.db.Exec(ddl)
		if err != nil {
			log.Error("Failed to executed DDL",
				zap.String("received", tableDef.Query),
				zap.String("rewritten", strings.Join(ddls, "\n")),
				zap.Uint64("tableVersion", tableDef.TableVersion))
			return errors.Annotate(err, fmt.Sprint("failed to execute", ddl))
		}
	}
	// update columns
	sc.columns = tableDef.Columns
	log.Info("Successfully executed DDL",
		zap.String("received", tableDef.Query),
		zap.String("rewritten", strings.Join(ddls, "\n")),
		zap.Uint64("tableVersion", tableDef.TableVersion))
	return nil
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

func (sc *Connector) LoadIncrement(tableDef cloudstorage.TableDefinition, filePath string) error {
	// merge staged file into table
	mergeQuery := GenMergeInto(tableDef, filePath, sc.stageName)
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

func tableColumnNames(columns []cloudstorage.TableCol) []string {
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, col.Name)
	}
	return names
}
