package replicate

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	putil "github.com/pingcap/ticdc/pkg/util"
	storage "github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"
	"go.uber.org/zap"
)

func snapshotLoadInfoPath(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf("%s.%s.loadinfo", sourceDatabase, sourceTable)
}

type session struct {
	connector *snowflake.Connector

	SourceDatabase string
	SourceTable    string

	StorageWorkspaceUri url.URL
	storage             storage.Storage

	ctx    context.Context
	logger *zap.Logger
}

func newSession(
	ctx context.Context,
	connector *snowflake.Connector,
	sourceDatabase, sourceTable string,
	storageUri *url.URL,
	logger *zap.Logger,
) (*session, error) {
	sess := &session{
		connector:           connector,
		SourceDatabase:      sourceDatabase,
		SourceTable:         sourceTable,
		StorageWorkspaceUri: *storageUri,
		ctx:                 ctx,
		logger:              logger,
	}
	sess.logger.Info("Creating replicate session",
		zap.String("storageScheme", sess.StorageWorkspaceUri.Scheme),
		zap.String("storagePath", sess.StorageWorkspaceUri.Path))

	externalStorage, err := putil.GetExternalStorageWithDefaultTimeout(sess.ctx, storageUri.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	sess.storage = externalStorage
	return sess, nil
}

func (sess *session) Run() error {
	loadInfoPath := snapshotLoadInfoPath(sess.SourceDatabase, sess.SourceTable)
	sess.logger.Info("checking snapshot load marker", zap.String("loadinfo", loadInfoPath))
	loaded, err := sess.storage.FileExists(sess.ctx, loadInfoPath)
	if err != nil {
		return errors.Annotate(err, "check snapshot loadinfo")
	}
	if loaded {
		sess.logger.Info("snapshot has already been loaded, skipping snapshot load",
			zap.String("loadinfo", loadInfoPath))
		return nil
	}

	switch sess.StorageWorkspaceUri.Scheme {
	case "s3", "gcs", "gs":
		sess.logger.Info("copying source table schema to data warehouse")
		schemaFilePath := table.SchemaFilePath(sess.SourceDatabase, sess.SourceTable)
		schemaSQL, err := sess.storage.ReadFile(sess.ctx, schemaFilePath)
		if err != nil {
			return errors.Annotatef(err, "read snapshot schema file %s", schemaFilePath)
		}
		tableSchema := table.BuildSchema(sess.SourceDatabase, sess.SourceTable, string(schemaSQL))
		if err := sess.connector.CopyTableSchema(tableSchema); err != nil {
			return errors.Trace(err)
		}
	default:
		return errors.Errorf("%s does not supprt data warehouse connector now...", sess.StorageWorkspaceUri.Scheme)
	}

	startTime := time.Now()
	pattern := fmt.Sprintf("%s.%s.*%s*", sess.SourceDatabase, sess.SourceTable, CSVFileExtension)
	sess.logger.Info("loading snapshot data into data warehouse", zap.String("pattern", pattern))
	if err := sess.connector.LoadSnapshot(sess.SourceTable, pattern); err != nil {
		sess.logger.Error("Failed to load snapshot data into data warehouse", zap.Error(err))
		return errors.Trace(err)
	}
	endTime := time.Now()
	sess.logger.Info("Successfully load all snapshot data into data warehouse", zap.Duration("cost", endTime.Sub(startTime)))

	// Write load info to workspace to record the status of load,
	// loadinfo exists means the data has been all loaded into data warehouse.
	loadinfo := fmt.Sprintf("Copy to data warehouse start time: %s\nCopy to data warehouse end time: %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	if err := sess.storage.WriteFile(sess.ctx, loadInfoPath, []byte(loadinfo)); err != nil {
		sess.logger.Error("Failed to upload loadinfo", zap.Error(err))
		return errors.Annotate(err, "upload snapshot loadinfo")
	}
	sess.logger.Info("Successfully upload loadinfo",
		zap.String("path", loadInfoPath),
		zap.Time("startTime", startTime),
		zap.Time("endTime", endTime))
	return nil
}

func Snapshot(
	ctx context.Context,
	connector *snowflake.Connector,
	tableFQN string,
	storageUri *url.URL,
) error {
	logger := log.L().With(zap.String("table", tableFQN))
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	session, err := newSession(ctx, connector, sourceDatabase, sourceTable, storageUri, logger)
	if err != nil {
		logger.Error("Failed to create snapshot replicate session", zap.Error(err))
		return errors.Trace(err)
	}
	if err := session.Run(); err != nil {
		logger.Error("Failed to load snapshot", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
