package replicate

import (
	"cmp"
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

const (
	CSVFileExtension        = ".csv"
	checkpointFileExtension = ".checkpoint"
)

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
	objectFiles     int
	schemaFiles     int
	dmlFiles        int
	checkpointFiles int
	pendingFiles    int
	pendingBytes    int64
	ignoredFiles    int
}

type progressFile struct {
	DML []progressEntry `json:"dml"`
}

type progressEntry struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	TableVersion uint64 `json:"table_version"`
	PartitionNum int64  `json:"partition_num"`
	Date         string `json:"date"`
	FileIndex    uint64 `json:"file_index"`
}

type IncrementReplicateSession struct {
	dwConnector *snowflake.Connector
	storage     storage.Storage
	ctx         context.Context
	// tableDMLIdxMap maintains a map of <DMLPathKey, max file index>
	tableDMLIdxMap map[cloudstorage.DMLPathKey]uint64
	// progressDMLIdxMap maintains a map of <DMLPathKey, max applied file index>
	progressDMLIdxMap map[cloudstorage.DMLPathKey]uint64
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
		dwConnector:       dwConnector,
		storage:           externalStorage,
		ctx:               ctx,
		tableDMLIdxMap:    make(map[cloudstorage.DMLPathKey]uint64),
		progressDMLIdxMap: make(map[cloudstorage.DMLPathKey]uint64),
		tableDefMap:       make(map[uint64]*cloudstorage.SchemaFile),
		fileExtension:     fileExtension,
		sourceDatabase:    sourceDatabase,
		sourceTable:       sourceTable,
		tableFQN:          tableFQN,
		logger:            logger,
	}
	sess.logger.Info("creating increment replicate session",
		zap.String("storageScheme", storageURI.Scheme),
		zap.String("storagePath", storageURI.Path),
		zap.String("fileExtension", fileExtension))
	if err := sess.loadProgress(); err != nil {
		return nil, errors.Trace(err)
	}
	return sess, nil
}

func (sess *IncrementReplicateSession) parseDMLFilePath(path string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("parse dml file path %s: %v", path, r)
		}
	}()

	var dmlkey cloudstorage.DMLPathKey
	fileIndex := dmlkey.ParseDMLFilePath(config.DateSeparatorDay.String(), path, sess.fileExtension)
	fileIdx := fileIndex.Idx
	if _, ok := sess.tableDMLIdxMap[dmlkey]; !ok || fileIdx >= sess.tableDMLIdxMap[dmlkey] {
		sess.tableDMLIdxMap[dmlkey] = fileIdx
	}
	return nil
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

func (sess *IncrementReplicateSession) progressPath() string {
	return fmt.Sprintf("%s/%s/_consumer/progress.json", sess.sourceDatabase, sess.sourceTable)
}

func progressEntryToDMLKey(entry progressEntry) cloudstorage.DMLPathKey {
	return cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       entry.Schema,
			Table:        entry.Table,
			TableVersion: entry.TableVersion,
		},
		PartitionNum: entry.PartitionNum,
		Date:         entry.Date,
	}
}

func progressEntryFromDMLKey(key cloudstorage.DMLPathKey, fileIdx uint64) progressEntry {
	return progressEntry{
		Schema:       key.Schema,
		Table:        key.Table,
		TableVersion: key.TableVersion,
		PartitionNum: key.PartitionNum,
		Date:         key.Date,
		FileIndex:    fileIdx,
	}
}

func (sess *IncrementReplicateSession) loadProgress() error {
	path := sess.progressPath()
	exists, err := sess.storage.FileExists(sess.ctx, path)
	if err != nil {
		return errors.Annotate(err, "check increment progress")
	}
	if !exists {
		sess.logger.Info("no increment progress found", zap.String("path", path))
		return nil
	}
	data, err := sess.storage.ReadFile(sess.ctx, path)
	if err != nil {
		return errors.Annotate(err, "read increment progress")
	}
	var progress progressFile
	if err := json.Unmarshal(data, &progress); err != nil {
		return errors.Annotate(err, "decode increment progress")
	}
	for _, entry := range progress.DML {
		if entry.Schema != sess.sourceDatabase || entry.Table != sess.sourceTable {
			sess.logger.Warn("ignore progress entry for different table",
				zap.String("schema", entry.Schema),
				zap.String("table", entry.Table))
			continue
		}
		key := progressEntryToDMLKey(entry)
		sess.progressDMLIdxMap[key] = entry.FileIndex
		sess.tableDMLIdxMap[key] = entry.FileIndex
	}
	sess.logger.Info("loaded increment progress",
		zap.String("path", path),
		zap.Int("entries", len(sess.progressDMLIdxMap)))
	return nil
}

func (sess *IncrementReplicateSession) saveProgress() error {
	progress := progressFile{DML: make([]progressEntry, 0, len(sess.progressDMLIdxMap))}
	for key, fileIdx := range sess.progressDMLIdxMap {
		progress.DML = append(progress.DML, progressEntryFromDMLKey(key, fileIdx))
	}
	slices.SortStableFunc(progress.DML, func(x, y progressEntry) int {
		if r := cmp.Compare(x.Schema, y.Schema); r != 0 {
			return r
		}
		if r := cmp.Compare(x.Table, y.Table); r != 0 {
			return r
		}
		if r := cmp.Compare(x.TableVersion, y.TableVersion); r != 0 {
			return r
		}
		if r := cmp.Compare(x.PartitionNum, y.PartitionNum); r != 0 {
			return r
		}
		return cmp.Compare(x.Date, y.Date)
	})
	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}
	if err := sess.storage.WriteFile(sess.ctx, sess.progressPath(), data); err != nil {
		return errors.Annotate(err, "write increment progress")
	}
	sess.logger.Debug("increment progress saved",
		zap.String("path", sess.progressPath()),
		zap.Int("entries", len(progress.DML)))
	return nil
}

func (sess *IncrementReplicateSession) markDMLApplied(key cloudstorage.DMLPathKey, fileIdx uint64) error {
	if key.IsSchemaFileDMLPathKey() {
		return nil
	}
	if cur, ok := sess.progressDMLIdxMap[key]; ok && cur >= fileIdx {
		return nil
	}
	sess.progressDMLIdxMap[key] = fileIdx
	if err := sess.saveProgress(); err != nil {
		return err
	}
	sess.logger.Info("marked DML file applied",
		zap.Uint64("tableVersion", key.TableVersion),
		zap.Int64("partitionNum", key.PartitionNum),
		zap.String("date", key.Date),
		zap.Uint64("fileIndex", fileIdx))
	return nil
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
	checkpointSet := make(map[string]struct{})
	stats := incrementalScanStats{}
	opt := &storage.WalkOption{SubDir: fmt.Sprintf("%s/%s", sess.sourceDatabase, sess.sourceTable)}
	err := sess.storage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
		if strings.HasSuffix(path, checkpointFileExtension) {
			stats.checkpointFiles++
			checkpointSet[path] = struct{}{}
			return nil
		}
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
			if err := sess.parseDMLFilePath(path); err != nil {
				sess.logger.Error("failed to parse dml file path", zap.Error(err))
				// skip handling this file
				continue
			}
			if !checkpointExistsInSet(path, sess.fileExtension, checkpointSet) {
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
			zap.Int("checkpointFiles", stats.checkpointFiles),
			zap.Int("pendingFiles", stats.pendingFiles),
			zap.Int64("pendingBytes", stats.pendingBytes),
			zap.Int("ignoredFiles", stats.ignoredFiles))
	} else {
		sess.logger.Debug("increment storage scan completed",
			zap.Int("objectFiles", stats.objectFiles),
			zap.Int("checkpointFiles", stats.checkpointFiles))
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

func (sess *IncrementReplicateSession) CheckpointExists(filePath string) bool {
	checkpointFileName := checkpointPath(filePath, sess.fileExtension)
	exist, err := sess.storage.FileExists(sess.ctx, checkpointFileName)
	if err != nil {
		return false
	}
	return exist
}

func checkpointPath(filePath, fileExtension string) string {
	return strings.TrimSuffix(filePath, fileExtension) + checkpointFileExtension
}

func checkpointExistsInSet(filePath, fileExtension string, checkpointSet map[string]struct{}) bool {
	_, ok := checkpointSet[checkpointPath(filePath, fileExtension)]
	return ok
}

func (sess *IncrementReplicateSession) syncExecDMLEvents(
	tableDef cloudstorage.SchemaFile,
	key cloudstorage.DMLPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(&cloudstorage.FileIndex{Idx: fileIdx}, sess.fileExtension, config.DefaultFileIndexWidth)
	checkpointFileName := checkpointPath(filePath, sess.fileExtension)

	// check if the file has been loaded into data warehouse
	exist, err := sess.storage.FileExists(sess.ctx, checkpointFileName)
	if err != nil {
		return errors.Annotate(err, "failed to check if checkpoint file exists")
	}
	if exist {
		sess.logger.Info("DML file already has checkpoint, skipping load",
			zap.String("filePath", filePath),
			zap.String("checkpoint", checkpointFileName),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Uint64("fileIndex", fileIdx))
		if size, ok := sess.dataFileMap[filePath]; ok {
			metrics.SubGauge(metrics.IncrementPendingSizeGauge, float64(size), sess.tableFQN)
			delete(sess.dataFileMap, filePath)
		}
		return sess.markDMLApplied(key, fileIdx)
	}

	// merge file into data warehouse
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

	// upload a checkpoint file to indicate that the file has been loaded into data warehouse
	if err := sess.storage.WriteFile(sess.ctx, checkpointFileName, []byte{}); err != nil {
		return errors.Annotate(err, "write DML checkpoint")
	}
	sess.logger.Info("DML checkpoint written",
		zap.String("filePath", filePath),
		zap.String("checkpoint", checkpointFileName),
		zap.Uint64("tableVersion", key.TableVersion),
		zap.Int64("partitionNum", key.PartitionNum),
		zap.String("date", key.Date),
		zap.Uint64("fileIndex", fileIdx))
	if err := sess.markDMLApplied(key, fileIdx); err != nil {
		return errors.Trace(err)
	}

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

func (sess *IncrementReplicateSession) Run(flushInterval time.Duration) error {
	sess.logger.Info("increment replicate session started", zap.Duration("scanInterval", flushInterval))
	ticker := time.NewTicker(flushInterval)
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
	flushInterval time.Duration,
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
	if err = session.Run(flushInterval); err != nil {
		logger.Error("error occurred while running increment replicate session", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
