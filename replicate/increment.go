package replicate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	putil "github.com/pingcap/ticdc/pkg/util"
	storage "github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const CSVFileExtension = ".csv"

// fileIndexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type fileIndexRange struct {
	start uint64
	end   uint64
}

type objectFile struct {
	path string
	size int64
}

type incrementalScanStats struct {
	objectFiles  int
	schemaFiles  int
	dmlFiles     int
	pendingFiles int
	pendingBytes int64
	ignoredFiles int
}

type IncrementReplicateSession struct {
	dwConnector *snowflake.Connector
	storage     storage.Storage
	ctx         context.Context
	// tableDMLIdxMap maintains a map of <DMLPathKey, max file index>
	tableDMLIdxMap map[cloudstorage.DMLPathKey]uint64
	// tableDefMap maintains a map of <tableVersion, tableDef>
	tableDefMap map[uint64]*cloudstorage.SchemaFile
	// dataFileMap maintains a map of <dataFilePath, fileSize>
	dataFileMap    map[string]int64
	fileExtension  string
	sourceDatabase string
	sourceTable    string
	tableFQN       string
	logger         *zap.Logger
}

func NewIncrementReplicateSession(
	ctx context.Context,
	dwConnector *snowflake.Connector,
	fileExtension string,
	storageURI *url.URL,
	tableFQN string,
	logger *zap.Logger,
) (*IncrementReplicateSession, error) {
	externalStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	sess := &IncrementReplicateSession{
		dwConnector:    dwConnector,
		storage:        externalStorage,
		ctx:            ctx,
		tableDMLIdxMap: make(map[cloudstorage.DMLPathKey]uint64),
		tableDefMap:    make(map[uint64]*cloudstorage.SchemaFile),
		fileExtension:  fileExtension,
		sourceDatabase: sourceDatabase,
		sourceTable:    sourceTable,
		tableFQN:       tableFQN,
		logger:         logger,
	}
	sess.logger.Info("creating increment replicate session",
		zap.String("storageScheme", storageURI.Scheme),
		zap.String("storagePath", storageURI.Path),
		zap.String("fileExtension", fileExtension))
	return sess, nil
}

func (sess *IncrementReplicateSession) parseDMLFilePath(path string) (dmlkey cloudstorage.DMLPathKey, fileIdx uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			dmlkey = cloudstorage.DMLPathKey{}
			fileIdx = 0
			err = errors.Errorf("parse dml file path %s: %v", path, r)
		}
	}()

	fileIndex := dmlkey.ParseDMLFilePath(config.DateSeparatorDay.String(), path, sess.fileExtension)
	fileIdx = fileIndex.Idx
	if _, ok := sess.tableDMLIdxMap[dmlkey]; !ok || fileIdx >= sess.tableDMLIdxMap[dmlkey] {
		sess.tableDMLIdxMap[dmlkey] = fileIdx
	}
	return dmlkey, fileIdx, nil
}

func (sess *IncrementReplicateSession) parseSchemaFilePath(path string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("parse schema file path %s: %v", path, r)
		}
	}()

	var schemaKey cloudstorage.SchemaPathKey
	schemaKey.Parse(path)
	if _, ok := sess.tableDefMap[schemaKey.TableVersion]; ok {
		// Skip if tableDef already exists.
		return nil
	}
	if schemaKey.Schema != sess.sourceDatabase || schemaKey.Table != sess.sourceTable {
		// ignore schema files do not belong to the current table.
		// This should not happen.
		sess.logger.Error("schema file path not match", zap.String("path", path))
		return nil
	}

	// Read tableDef from schema file and check checksum.
	var tableDef cloudstorage.SchemaFile
	schemaContent, err := sess.storage.ReadFile(sess.ctx, path)
	if err != nil {
		return errors.Trace(err)
	}
	if err = json.Unmarshal(schemaContent, &tableDef); err != nil {
		return errors.Trace(err)
	}
	expectedPath := tableDef.Path(false, 0)
	if expectedPath != path || schemaKey.TableVersion != tableDef.TableVersion {
		sess.logger.Error("checksum mismatch",
			zap.String("expectedPath", expectedPath),
			zap.Uint64("tableversionInMem", schemaKey.TableVersion),
			zap.Uint64("tableversionInFile", tableDef.TableVersion),
			zap.String("path", path))
		return errors.Errorf("checksum mismatch")
	}

	// Update tableDefMap.
	sess.tableDefMap[tableDef.TableVersion] = &tableDef

	// Fake a dml key for schema.json file, which is useful for putting DDL
	// in front of the DML files when sorting.
	// e.g, for the partitioned table:
	//
	// test/test1/439972354120482843/schema.json					(partitionNum = -1)
	// test/test1/439972354120482843/55/2023-03-09/CDC000001.csv	(partitionNum = 55)
	// test/test1/439972354120482843/66/2023-03-09/CDC000001.csv	(partitionNum = 66)
	//
	// and for the non-partitioned table:
	// test/test2/439972354120482843/schema.json				(partitionNum = -1)
	// test/test2/439972354120482843/2023-03-09/CDC000001.csv	(partitionNum = 0)
	// test/test2/439972354120482843/2023-03-09/CDC000002.csv	(partitionNum = 0)
	//
	// the DDL event recorded in schema.json should be executed first, then the DML events
	// in csv files can be executed.
	dmlkey := cloudstorage.NewSchemaFileDMLPathKey(schemaKey)
	if _, ok := sess.tableDMLIdxMap[dmlkey]; !ok {
		sess.tableDMLIdxMap[dmlkey] = 0
	} else {
		// duplicate table schema file found, this should not happen.
		sess.logger.Panic("duplicate schema file found",
			zap.String("path", path), zap.Any("tableDef", tableDef),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	return nil
}

// map1 - map2
func diffDMLMaps(
	map1, map2 map[cloudstorage.DMLPathKey]uint64,
) map[cloudstorage.DMLPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DMLPathKey]fileIndexRange)
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			resMap[k] = fileIndexRange{
				start: 1,
				end:   v,
			}
		} else if v > map2[k] {
			resMap[k] = fileIndexRange{
				start: map2[k] + 1,
				end:   v,
			}
		}
	}
	return resMap
}

// getNewFiles returns newly created dml files in specific ranges
func (sess *IncrementReplicateSession) getNewFiles() (map[cloudstorage.DMLPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DMLPathKey]fileIndexRange)
	origDMLIdxMap := make(map[cloudstorage.DMLPathKey]uint64, len(sess.tableDMLIdxMap))
	for k, v := range sess.tableDMLIdxMap {
		origDMLIdxMap[k] = v
	}
	sess.dataFileMap = make(map[string]int64)
	files := make([]objectFile, 0)
	stats := incrementalScanStats{}
	opt := &storage.WalkOption{SubDir: fmt.Sprintf("%s/%s", sess.sourceDatabase, sess.sourceTable)}
	err := sess.storage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
		stats.objectFiles++
		files = append(files, objectFile{path: path, size: size})
		return nil
	})
	if err != nil {
		return tableDMLMap, err
	}

	for _, file := range files {
		path := file.path
		if cloudstorage.IsSchemaFile(path) {
			stats.schemaFiles++
			if err := sess.parseSchemaFilePath(path); err != nil {
				sess.logger.Error("failed to parse schema file path", zap.Error(err))
				// skip handling this file
				continue
			}
		} else if strings.HasSuffix(path, sess.fileExtension) {
			stats.dmlFiles++
			key, fileIdx, err := sess.parseDMLFilePath(path)
			if err != nil {
				sess.logger.Error("failed to parse dml file path", zap.Error(err))
				// skip handling this file
				continue
			}
			if origDMLIdxMap[key] < fileIdx {
				stats.pendingFiles++
				stats.pendingBytes += file.size
				sess.dataFileMap[path] = file.size
				metrics.AddGauge(metrics.IncrementPendingSizeGauge, float64(file.size), sess.tableFQN)
			}
		} else {
			stats.ignoredFiles++
			sess.logger.Debug("ignore handling file", zap.String("path", path))
		}
	}

	tableDMLMap = diffDMLMaps(sess.tableDMLIdxMap, origDMLIdxMap)
	if len(tableDMLMap) > 0 || stats.pendingFiles > 0 {
		sess.logger.Info("increment storage scan completed",
			zap.Int("newRanges", len(tableDMLMap)),
			zap.Int("objectFiles", stats.objectFiles),
			zap.Int("schemaFiles", stats.schemaFiles),
			zap.Int("dmlFiles", stats.dmlFiles),
			zap.Int("pendingFiles", stats.pendingFiles),
			zap.Int64("pendingBytes", stats.pendingBytes),
			zap.Int("ignoredFiles", stats.ignoredFiles))
	} else {
		sess.logger.Debug("increment storage scan completed",
			zap.Int("objectFiles", stats.objectFiles))
	}
	return tableDMLMap, err
}

func (sess *IncrementReplicateSession) getTableDef(tableVersion uint64) cloudstorage.SchemaFile {
	if td, ok := sess.tableDefMap[tableVersion]; ok {
		return *td
	} else {
		sess.logger.Panic("tableDef not found", zap.Any("table version", tableVersion), zap.Any("tableDefMap", sess.tableDefMap))
		return cloudstorage.SchemaFile{}
	}
}

func (sess *IncrementReplicateSession) syncExecDMLEvents(
	tableDef cloudstorage.SchemaFile,
	key cloudstorage.DMLPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(&cloudstorage.FileIndex{Idx: fileIdx}, sess.fileExtension, config.DefaultFileIndexWidth)
	fileSize := sess.dataFileMap[filePath]
	sess.logger.Info("loading DML file into data warehouse",
		zap.String("filePath", filePath),
		zap.Int64("fileSize", fileSize),
		zap.Uint64("tableVersion", key.TableVersion),
		zap.Int64("partitionNum", key.PartitionNum),
		zap.String("date", key.Date),
		zap.Uint64("fileIndex", fileIdx))
	if err := sess.dwConnector.LoadIncrement(tableDef, filePath); err != nil {
		sess.logger.Error("failed to load DML file into data warehouse",
			zap.Error(err),
			zap.String("filePath", filePath),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Uint64("fileIndex", fileIdx))
		return errors.Trace(err)
	}

	sess.logger.Info("DML file loaded",
		zap.String("filePath", filePath),
		zap.Uint64("tableVersion", key.TableVersion),
		zap.Int64("partitionNum", key.PartitionNum),
		zap.String("date", key.Date),
		zap.Uint64("fileIndex", fileIdx))

	// update metrics
	metrics.SubGauge(metrics.IncrementPendingSizeGauge, float64(fileSize), sess.tableFQN)
	metrics.AddCounter(metrics.IncrementLoadedSizeCounter, float64(fileSize), sess.tableFQN)
	delete(sess.dataFileMap, filePath)
	return nil
}

func (sess *IncrementReplicateSession) syncExecDDLEvents(tableDef cloudstorage.SchemaFile) error {
	if len(tableDef.Query) == 0 {
		// schema.json file without query is used to initialize the schema.
		sess.logger.Info("initializing table schema from schema file",
			zap.Uint64("tableVersion", tableDef.TableVersion),
			zap.Int("columnCount", len(tableDef.Columns)))
		err := sess.dwConnector.InitSchema(tableDef.Columns)
		return errors.Wrap(err, "failed to init schema")
	}

	sess.logger.Info("executing DDL from schema file",
		zap.Uint64("tableVersion", tableDef.TableVersion),
		zap.String("query", tableDef.Query),
		zap.Int("columnCount", len(tableDef.Columns)))
	if err := sess.dwConnector.ExecDDL(tableDef); err != nil {
		// FIXME: if there is a DDL before all the DMLs, will return error here.
		return errors.Annotate(err,
			fmt.Sprintf("Please check the DDL query, "+
				"if necessary, please manually execute the DDL query in data warehouse, "+
				"update the `query` of the %s/%s/%s/meta/schema_%d_{hash}.json to empty, "+
				"and restart the program",
				sess.storage.URI(), tableDef.Schema, tableDef.Table, tableDef.TableVersion))
	}
	metrics.AddCounter(metrics.TableVersionsCounter, float64(tableDef.TableVersion), fmt.Sprintf("%s/%s", sess.sourceDatabase, sess.sourceTable))

	// The following logic is used to handle pause and resume.
	// Keep the current table definition file with len(query) == 0 and delete all the outdated files.

	// Delete all the outdated table definition files.
	for _, item := range sess.tableDefMap {
		if item.TableVersion < tableDef.TableVersion {
			filePath := item.Path(false, 0)
			if err := sess.storage.DeleteFile(sess.ctx, filePath); err != nil {
				return errors.Trace(err)
			}
			delete(sess.tableDefMap, item.TableVersion)
		}
	}
	// clear the query in the current table definition file.
	tableDef.Query = ""
	data := tableDef.Marshal()
	filePath := tableDef.Path(false, 0)
	// update the current table definition file.
	if err := sess.storage.WriteFile(sess.ctx, filePath, data); err != nil {
		return errors.Annotate(err, "update current schema file")
	}
	sess.logger.Info("schema file marked as applied",
		zap.String("path", filePath),
		zap.Uint64("tableVersion", tableDef.TableVersion))
	return nil
}

func (sess *IncrementReplicateSession) handleNewFiles(dmlFileMap map[cloudstorage.DMLPathKey]fileIndexRange) error {
	keys := make([]cloudstorage.DMLPathKey, 0, len(dmlFileMap))
	for k := range dmlFileMap {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		sess.logger.Debug("no new files found since last round")
		return nil
	}
	slices.SortStableFunc(keys, func(x, y cloudstorage.DMLPathKey) int {
		return cloudstorage.CompareDMLPathKey(x, y)
	})
	sess.logger.Info("new increment ranges found",
		zap.Int("rangeCount", len(keys)),
		zap.Uint64("fileCount", countFilesInRanges(dmlFileMap)))

	for _, key := range keys {
		tableDef := sess.getTableDef(key.SchemaPathKey.TableVersion)
		// if the key is a fake dml path key which is mainly used for
		// sorting schema.json file before the dml files, which means it is a schema.json file.
		if key.IsSchemaFileDMLPathKey() {
			if err := sess.syncExecDDLEvents(tableDef); err != nil {
				return errors.Trace(err)
			}
			continue
		}

		fileRange := dmlFileMap[key]
		sess.logger.Info("processing increment range",
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Uint64("startFileIndex", fileRange.start),
			zap.Uint64("endFileIndex", fileRange.end))
		for i := fileRange.start; i <= fileRange.end; i++ {
			if err := sess.syncExecDMLEvents(tableDef, key, i); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func countFilesInRanges(ranges map[cloudstorage.DMLPathKey]fileIndexRange) uint64 {
	var count uint64
	for key, fileRange := range ranges {
		if key.IsSchemaFileDMLPathKey() {
			continue
		}
		if fileRange.end >= fileRange.start {
			count += fileRange.end - fileRange.start + 1
		}
	}
	return count
}

func (sess *IncrementReplicateSession) Run(scanInterval time.Duration) error {
	sess.logger.Info("increment replicate session started", zap.Duration("scanInterval", scanInterval))
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sess.ctx.Done():
			return sess.ctx.Err()
		case <-ticker.C:
		}
		dmlFileMap, err := sess.getNewFiles()
		if err != nil {
			return errors.Trace(err)
		}

		if err = sess.handleNewFiles(dmlFileMap); err != nil {
			return errors.Trace(err)
		}
	}
}

func StartReplicateIncrement(
	ctx context.Context,
	dwConnector *snowflake.Connector,
	tableFQN string,
	storageURI *url.URL,
	scanInterval time.Duration,
) error {
	fileExtension := CSVFileExtension

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// init metric IncrementPendingSizeGauge
	metrics.AddGauge(metrics.IncrementPendingSizeGauge, 0, tableFQN)
	logger := log.L().With(zap.String("table", tableFQN))
	session, err := NewIncrementReplicateSession(ctx, dwConnector, fileExtension, storageURI, tableFQN, logger)
	if err != nil {
		logger.Error("error occurred while creating increment replicate session", zap.Error(err))
		return errors.Trace(err)
	}
	if err = session.Run(scanInterval); err != nil {
		logger.Error("error occurred while running increment replicate session", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
