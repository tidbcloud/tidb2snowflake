package replicate

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	putil "github.com/pingcap/ticdc/pkg/util"
	storage "github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"
	"go.uber.org/zap"
)

const (
	DataWarehouseLoadConcurrency = 16
)

func snapshotLoadInfoPath(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf("%s.%s.loadinfo", sourceDatabase, sourceTable)
}

func isSnapshotDataFile(path string) bool {
	return strings.HasSuffix(path, CSVFileExtension) || strings.HasSuffix(path, CSVFileExtension+".gz")
}

type session struct {
	connector *snowflake.Connector

	SourceDatabase string
	SourceTable    string

	StorageWorkspaceUri url.URL
	storage             storage.Storage
	ParrallelLoad       bool

	ctx    context.Context
	logger *zap.Logger
}

func newSession(
	ctx context.Context,
	connector *snowflake.Connector,
	sourceDatabase, sourceTable string,
	storageUri *url.URL,
	parrallelLoad bool,
	logger *zap.Logger,
) (*session, error) {
	sess := &session{
		connector:           connector,
		SourceDatabase:      sourceDatabase,
		SourceTable:         sourceTable,
		StorageWorkspaceUri: *storageUri,
		ParrallelLoad:       parrallelLoad,
		ctx:                 ctx,
		logger:              logger,
	}
	sess.logger.Info("Creating replicate session",
		zap.String("storageScheme", sess.StorageWorkspaceUri.Scheme),
		zap.String("storagePath", sess.StorageWorkspaceUri.Path),
		zap.Bool("parallelLoad", sess.ParrallelLoad))

	externalStorage, err := putil.GetExternalStorageFromURI(sess.ctx, storageUri.String())
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
	if sess.ParrallelLoad {
		var snapshotFileSize int64
		var fileCount int64
		tableFQN := fmt.Sprintf("%s.%s", sess.SourceDatabase, sess.SourceTable)
		opt := &storage.WalkOption{ObjPrefix: fmt.Sprintf("%s.", tableFQN)}
		if err := sess.storage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
			if isSnapshotDataFile(path) {
				snapshotFileSize += size
				fileCount++
			}
			return nil
		}); err != nil {
			return errors.Trace(err)
		}
		sess.logger.Info("snapshot files discovered",
			zap.Int64("fileCount", fileCount),
			zap.Int64("totalBytes", snapshotFileSize),
			zap.Int("loadConcurrency", DataWarehouseLoadConcurrency))
		metrics.AddCounter(metrics.SnapshotTotalSizeCounter, float64(snapshotFileSize), tableFQN)
		errFileCh := make(chan string, fileCount)
		blockCh := make(chan struct{}, DataWarehouseLoadConcurrency)
		var wg sync.WaitGroup
		if err := sess.storage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
			if isSnapshotDataFile(path) {
				blockCh <- struct{}{}
				wg.Add(1)
				go func(path string, size int64) {
					defer func() {
						<-blockCh
						wg.Done()
					}()
					sess.logger.Info("Loading snapshot data into data warehouse", zap.String("path", path))
					if err := sess.connector.LoadSnapshot(sess.SourceTable, path); err != nil {
						sess.logger.Error("Failed to load snapshot data into data warehouse", zap.Error(err), zap.String("path", path))
						errFileCh <- path
					} else {
						sess.logger.Info("Successfully load snapshot data into data warehouse", zap.String("path", path))
						metrics.AddCounter(metrics.SnapshotLoadedSizeCounter, float64(size), tableFQN)
					}
				}(path, size)
			}
			return nil
		}); err != nil {
			return errors.Trace(err)
		}
		wg.Wait()
		close(errFileCh)
		close(blockCh)
		errFileList := make([]string, 0, len(errFileCh))
		for len(errFileCh) > 0 {
			errFileList = append(errFileList, <-errFileCh)
		}
		if len(errFileList) > 0 {
			return errors.Errorf("Failed to load snapshot data into data warehouse, error files: %v", errFileList)
		}
	} else {
		pattern := fmt.Sprintf("%s.%s.*%s*", sess.SourceDatabase, sess.SourceTable, CSVFileExtension)
		sess.logger.Info("loading snapshot data into data warehouse", zap.String("pattern", pattern))
		if err := sess.connector.LoadSnapshot(sess.SourceTable, pattern); err != nil {
			sess.logger.Error("Failed to load snapshot data into data warehouse", zap.Error(err))
			return errors.Trace(err)
		}
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
	parrallelLoad bool,
) error {
	logger := log.L().With(zap.String("table", tableFQN))
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	session, err := newSession(ctx, connector, sourceDatabase, sourceTable, storageUri, parrallelLoad, logger)
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
