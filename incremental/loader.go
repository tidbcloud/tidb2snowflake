package incremental

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/workerpool"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

const (
	metadataFileName = "metadata"
	dmlIndexFileName = "CDC.index"
)

type Config struct {
	Tables       []string
	StorageDir   string
	ScanInterval time.Duration
}

func Load(ctx context.Context, cfg Config, store *storage.Storage, stateManager state.Manager, pool *workerpool.Pool, conn *snowflake.Connector) error {
	log.Info("starting Snowflake incremental load phase",
		zap.Int("tableCount", len(cfg.Tables)), zap.Duration("scanInterval", cfg.ScanInterval))

	loader := newLoader(cfg, store, conn, stateManager)
	if err := loader.run(ctx, pool); err != nil {
		log.Error("Snowflake incremental load phase stopped", zap.Int("tableCount", len(cfg.Tables)), zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("Snowflake incremental load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func (loader *loader) run(ctx context.Context, pool *workerpool.Pool) error {
	ticker := time.NewTicker(loader.scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		bounds, hasScan, err := loader.beginScan(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !hasScan {
			log.Info("incremental skip scan", zap.Uint64("checkpointTs", bounds.checkpointTs))
			continue
		}
		startedAt := time.Now()
		summary, err := loader.processTables(ctx, bounds, pool)
		if err != nil {
			return errors.Trace(err)
		}
		if bounds.targetCheckpointTs > bounds.checkpointTs {
			if err := loader.state.SetCheckpointTS(ctx, bounds.targetCheckpointTs); err != nil {
				return errors.Trace(err)
			}
		}
		log.Info("incremental scan completed",
			zap.Uint64("checkpointTs", bounds.checkpointTs),
			zap.Uint64("targetCheckpointTs", bounds.targetCheckpointTs),
			zap.Int("activeTables", summary.activeTables),
			zap.Uint64("loadedFiles", summary.loadedFiles),
			zap.Duration("duration", time.Since(startedAt)))
	}
}

// indexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type indexRange struct {
	start uint64
	end   uint64
}

type incrementalScanStats struct {
	objectFiles     int
	schemaFiles     int
	indexFiles      int
	skippedDateDirs int
	pendingFiles    int
}

type scanBounds struct {
	checkpointTs       uint64
	targetCheckpointTs uint64
}

type scanSummary struct {
	// activeTables is the number of configured tables that had schema or DML work in this scan.
	activeTables int
	// loadedFiles is the number of DML files fully loaded into Snowflake.
	loadedFiles uint64
}

type tableScan struct {
	dmlFileMap      map[cloudstorage.DMLPathKey]indexRange
	schemaFilePaths map[uint64]string
	schemaFiles     map[uint64]cloudstorage.SchemaFile
}

type loader struct {
	conn         *snowflake.Connector
	storage      *storage.Storage
	storageDir   string
	scanInterval time.Duration
	state        state.Manager
	tables       []*tableState
}

type tableState struct {
	// tableDMLIdxMap records the in-memory contiguous DML file prefix fully consumed for each path.
	tableDMLIdxMap           map[cloudstorage.DMLPathKey]uint64
	activeDateByDMLScope     map[dmlScope]string
	dmlCursors               map[string]state.DMLCursor
	dirtyDMLCursors          map[string]state.DMLCursor
	currentMeta              *table.Meta
	ddlTableVersionWatermark uint64
	sourceDatabase           string
	sourceTable              string
	tableFQN                 string
}

type dmlScope struct {
	tableVersion uint64
	partitionNum int64
}

func dmlCursorKey(key cloudstorage.DMLPathKey) string {
	return fmt.Sprintf("%d/%d/CDC", key.TableVersion, key.PartitionNum)
}

func parseDMLCursorKey(cursor string) (uint64, int64, string) {
	parts := strings.Split(cursor, "/")
	if len(parts) != 3 {
		log.Panic("invalid DML cursor key", zap.String("cursor", cursor))
	}
	tableVersion, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		log.Panic("invalid DML cursor table version", zap.String("cursor", cursor), zap.Error(err))
	}
	partitionNum, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		log.Panic("invalid DML cursor partition number", zap.String("cursor", cursor), zap.Error(err))
	}
	return tableVersion, partitionNum, parts[2]
}

func newLoader(cfg Config, store *storage.Storage, conn *snowflake.Connector, stateManager state.Manager) *loader {
	l := &loader{
		conn:         conn,
		storage:      store,
		storageDir:   cfg.StorageDir,
		scanInterval: cfg.ScanInterval,
		state:        stateManager,
		tables:       make([]*tableState, 0, len(cfg.Tables)),
	}
	st := stateManager.Snapshot()
	for _, tableFQN := range cfg.Tables {
		sourceDatabase, sourceTable, ok := strings.Cut(tableFQN, ".")
		if !ok {
			sourceDatabase, sourceTable = "", ""
		}
		storedTable := st.Tables[tableFQN]
		metrics.AddGauge(metrics.IncrementPendingSizeGauge, 0, tableFQN)
		table := &tableState{
			tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]uint64),
			activeDateByDMLScope:     make(map[dmlScope]string),
			dmlCursors:               make(map[string]state.DMLCursor, len(storedTable.DMLCursors)),
			dirtyDMLCursors:          make(map[string]state.DMLCursor),
			ddlTableVersionWatermark: storedTable.DDLTableVersionWatermark,
			sourceDatabase:           sourceDatabase,
			sourceTable:              sourceTable,
			tableFQN:                 tableFQN,
		}
		for cursorKey, cursor := range storedTable.DMLCursors {
			table.dmlCursors[cursorKey] = cursor
			tableVersion, partitionNum, indexName := parseDMLCursorKey(cursorKey)
			if indexName != "CDC" {
				log.Panic("unsupported DML cursor index name", zap.String("cursor", cursorKey), zap.String("indexName", indexName))
			}
			key := cloudstorage.DMLPathKey{
				SchemaPathKey: cloudstorage.SchemaPathKey{
					Schema:       sourceDatabase,
					Table:        sourceTable,
					TableVersion: tableVersion,
				},
				PartitionNum: partitionNum,
				Date:         cursor.Date,
			}
			table.tableDMLIdxMap[key] = cursor.FileIndex
			scope := dmlScope{tableVersion: tableVersion, partitionNum: partitionNum}
			if cursor.Date > table.activeDateByDMLScope[scope] {
				table.activeDateByDMLScope[scope] = cursor.Date
			}
		}
		l.tables = append(l.tables, table)
	}
	return l
}

func (loader *loader) beginScan(ctx context.Context) (scanBounds, bool, error) {
	st := loader.state.Snapshot()
	checkpointTs := st.CheckpointTS

	metadataCheckpointTs, ok, err := loader.readMetadata(ctx)
	if err != nil {
		return scanBounds{}, false, err
	}
	if !ok {
		return scanBounds{checkpointTs: checkpointTs}, false, nil
	}
	return scanBounds{checkpointTs: checkpointTs, targetCheckpointTs: metadataCheckpointTs}, true, nil
}

func (loader *loader) readMetadata(ctx context.Context) (uint64, bool, error) {
	metadataPath := path.Join(loader.storageDir, metadataFileName)
	exists, err := loader.storage.FileExists(ctx, metadataPath)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if !exists {
		return 0, false, nil
	}
	data, err := loader.storage.ReadFile(ctx, metadataPath)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	var metadata struct {
		CheckpointTS uint64 `json:"checkpoint-ts"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return 0, false, errors.Trace(err)
	}
	return metadata.CheckpointTS, true, nil
}

func (loader *loader) processTable(
	ctx context.Context,
	bounds scanBounds,
	table *tableState,
) (scanSummary, error) {
	scan, err := loader.getNewFiles(ctx, bounds, table)
	if err != nil {
		metrics.AddCounter(metrics.ErrorCounter, 1, table.tableFQN)
		log.Error("incremental scan failed", zap.String("table", table.tableFQN), zap.Error(err))
		return scanSummary{}, errors.Trace(err)
	}
	if len(scan.dmlFileMap) == 0 {
		return scanSummary{}, nil
	}
	if err := loader.loadSchemaFiles(ctx, table, scan); err != nil {
		metrics.AddCounter(metrics.ErrorCounter, 1, table.tableFQN)
		log.Error("incremental scan failed", zap.String("table", table.tableFQN), zap.Error(err))
		return scanSummary{}, errors.Trace(err)
	}

	summary, err := loader.handleNewFiles(ctx, bounds, table, scan)
	if err != nil {
		metrics.AddCounter(metrics.ErrorCounter, 1, table.tableFQN)
		log.Error("incremental load failed", zap.String("table", table.tableFQN), zap.Error(err))
		return summary, errors.Trace(err)
	}
	if len(table.dirtyDMLCursors) > 0 {
		if err := loader.state.SetDMLCursors(ctx, table.tableFQN, table.dirtyDMLCursors); err != nil {
			return summary, errors.Trace(err)
		}
		table.dirtyDMLCursors = make(map[string]state.DMLCursor)
	}
	return summary, nil
}

func (loader *loader) processTables(ctx context.Context, bounds scanBounds, pool *workerpool.Pool) (scanSummary, error) {
	summaries := make([]scanSummary, len(loader.tables))
	group := pool.NewGroup(ctx, 0)
	var first error
	for i, table := range loader.tables {
		task := workerpool.TaskFunc(func(ctx context.Context) error {
			summary, err := loader.processTable(ctx, bounds, table)
			if err != nil {
				return errors.Trace(err)
			}
			summaries[i] = summary
			return nil
		})
		if err := group.Submit(task); err != nil {
			first = err
			break
		}
	}
	if err := group.Wait(); err != nil && first == nil {
		first = err
	}
	if first != nil {
		return scanSummary{}, errors.Trace(first)
	}

	var summary scanSummary
	for _, tableSummary := range summaries {
		summary.activeTables += tableSummary.activeTables
		summary.loadedFiles += tableSummary.loadedFiles
	}
	return summary, nil
}

func (loader *loader) parseSchemaFilePath(
	bounds scanBounds,
	table *tableState,
	objectPath string,
	schemaFilePaths map[uint64]string,
	seenDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
) {
	filePath := strings.TrimPrefix(objectPath, loader.storageDir+"/")

	var schemaKey cloudstorage.SchemaPathKey
	schemaKey.Parse(filePath)
	// TiCDC metadata checkpoint is a confirmed flush lower bound. Use it only
	// to avoid applying schema versions that TiCDC has not confirmed visible.
	if schemaKey.TableVersion > bounds.targetCheckpointTs {
		return
	}
	schemaFilePaths[schemaKey.TableVersion] = objectPath
	// Schema versions at or below the durable checkpoint are already reflected
	// in the loaded snapshot/incremental state. Keep them only as baseline meta.
	if schemaKey.TableVersion <= bounds.checkpointTs {
		return
	}
	// ddlTableVersionWatermark tracks schema versions already handled; keep the
	// path for baseline recovery, but do not schedule this schema as new work.
	if schemaKey.TableVersion <= table.ddlTableVersionWatermark {
		return
	}

	dmlkey := cloudstorage.NewSchemaFileDMLPathKey(schemaKey)
	seenDMLIdxMap[dmlkey] = 0
}

// getNewFiles returns newly created dml files in specific ranges.
func (loader *loader) getNewFiles(
	ctx context.Context,
	bounds scanBounds,
	table *tableState,
) (*tableScan, error) {
	scan := &tableScan{
		dmlFileMap:      make(map[cloudstorage.DMLPathKey]indexRange),
		schemaFilePaths: make(map[uint64]string),
		schemaFiles:     make(map[uint64]cloudstorage.SchemaFile),
	}
	seenDMLIdxMap := make(map[cloudstorage.DMLPathKey]uint64)
	stats := incrementalScanStats{}
	schemaMetaDir := path.Join(loader.storageDir, table.sourceDatabase, table.sourceTable, "meta")
	err := loader.storage.WalkDir(ctx, &storeapi.WalkOption{
		SubDir:    schemaMetaDir,
		ObjPrefix: "schema_",
	}, func(objectPath string, _ int64) error {
		stats.objectFiles++
		stats.schemaFiles++
		loader.parseSchemaFilePath(bounds, table, objectPath, scan.schemaFilePaths, seenDMLIdxMap)
		return nil
	})
	if err != nil {
		return scan, err
	}

	var activeVersion uint64
	schemaVersions := make([]uint64, 0, len(scan.schemaFilePaths))
	for version := range scan.schemaFilePaths {
		if version <= bounds.checkpointTs {
			if version > activeVersion {
				activeVersion = version
			}
			continue
		}
		schemaVersions = append(schemaVersions, version)
	}
	if activeVersion != 0 {
		schemaVersions = append(schemaVersions, activeVersion)
	}
	slices.Sort(schemaVersions)
	schemaVersions = slices.Compact(schemaVersions)

	for _, tableVersion := range schemaVersions {
		versionDir := path.Join(
			loader.storageDir,
			table.sourceDatabase,
			table.sourceTable,
			strconv.FormatUint(tableVersion, 10),
		)
		firstLevelDirs, err := loader.storage.ListDirs(ctx, versionDir)
		if err != nil {
			return scan, errors.Trace(err)
		}
		for _, dir := range firstLevelDirs {
			if isDateDir(dir) {
				scope := dmlScope{tableVersion: tableVersion}
				if activeDate := table.activeDateByDMLScope[scope]; activeDate != "" && dir < activeDate {
					stats.skippedDateDirs++
					continue
				}
				pending, err := loader.parseDMLIndexFileIfExists(
					ctx,
					bounds,
					table,
					path.Join(versionDir, dir, "meta", dmlIndexFileName),
					seenDMLIdxMap,
					&stats,
				)
				if err != nil {
					return scan, errors.Trace(err)
				}
				stats.pendingFiles += pending
				continue
			}

			partitionNum, err := strconv.ParseInt(dir, 10, 64)
			if err != nil {
				return scan, errors.Trace(err)
			}
			scope := dmlScope{tableVersion: tableVersion, partitionNum: partitionNum}
			partitionDir := path.Join(versionDir, dir)
			dateDirs, err := loader.storage.ListDirs(ctx, partitionDir)
			if err != nil {
				return scan, errors.Trace(err)
			}
			activeDate := table.activeDateByDMLScope[scope]
			for _, dateDir := range dateDirs {
				if activeDate != "" && dateDir < activeDate {
					stats.skippedDateDirs++
					continue
				}
				pending, err := loader.parseDMLIndexFileIfExists(
					ctx,
					bounds,
					table,
					path.Join(partitionDir, dateDir, "meta", dmlIndexFileName),
					seenDMLIdxMap,
					&stats,
				)
				if err != nil {
					return scan, errors.Trace(err)
				}
				stats.pendingFiles += pending
			}
		}
	}

	for key, fileIdx := range seenDMLIdxMap {
		if key.IsSchemaFileDMLPathKey() {
			scan.dmlFileMap[key] = indexRange{}
			continue
		}
		consumedIdx := table.tableDMLIdxMap[key]
		if fileIdx > consumedIdx {
			scan.dmlFileMap[key] = indexRange{start: consumedIdx + 1, end: fileIdx}
		}
	}
	if len(scan.dmlFileMap) > 0 || stats.pendingFiles > 0 {
		log.Info("increment storage scan completed",
			zap.String("table", table.tableFQN),
			zap.Int("newRanges", len(scan.dmlFileMap)),
			zap.Int("objectFiles", stats.objectFiles),
			zap.Int("schemaFiles", stats.schemaFiles),
			zap.Int("indexFiles", stats.indexFiles),
			zap.Int("skippedDateDirs", stats.skippedDateDirs),
			zap.Int("pendingFiles", stats.pendingFiles))
	}
	return scan, nil
}

func (table *tableState) markDMLPathConsumed(key cloudstorage.DMLPathKey, fileIdx uint64) {
	table.tableDMLIdxMap[key] = fileIdx
	cursorKey := dmlCursorKey(key)
	cursor := state.DMLCursor{Date: key.Date, FileIndex: fileIdx}
	table.dmlCursors[cursorKey] = cursor
	table.dirtyDMLCursors[cursorKey] = cursor
	scope := dmlScope{tableVersion: key.TableVersion, partitionNum: key.PartitionNum}
	if table.activeDateByDMLScope == nil {
		table.activeDateByDMLScope = make(map[dmlScope]string)
	}
	if key.Date > table.activeDateByDMLScope[scope] {
		table.activeDateByDMLScope[scope] = key.Date
	}
}

func isDateDir(dir string) bool {
	_, err := time.Parse("2006-01-02", dir)
	return err == nil
}

func (loader *loader) parseDMLIndexFileIfExists(
	ctx context.Context,
	bounds scanBounds,
	table *tableState,
	objectPath string,
	seenDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
	stats *incrementalScanStats,
) (int, error) {
	exists, err := loader.storage.FileExists(ctx, objectPath)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !exists {
		return 0, nil
	}
	stats.objectFiles++
	stats.indexFiles++
	return loader.parseDMLIndexFile(ctx, bounds, table, objectPath, seenDMLIdxMap)
}

func (loader *loader) parseDMLIndexFile(
	ctx context.Context,
	bounds scanBounds,
	table *tableState,
	objectPath string,
	seenDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
) (int, error) {
	filePath := strings.TrimPrefix(objectPath, loader.storageDir+"/")
	var dmlKey cloudstorage.DMLPathKey
	if err := dmlKey.ParseIndexFilePath(config.DateSeparatorDay.String(), filePath); err != nil {
		return 0, errors.Trace(err)
	}

	// Ignore index files for table versions newer than TiCDC's confirmed flush bound.
	if dmlKey.TableVersion > bounds.targetCheckpointTs {
		return 0, nil
	}

	data, err := loader.storage.ReadFile(ctx, objectPath)
	if err != nil {
		return 0, errors.Trace(err)
	}
	fileName := strings.TrimSpace(string(data))
	fileIndex, err := cloudstorage.ParseFileIndexFromFileName(fileName, storage.CSVFileExtension)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if fileIndex.EnableTableAcrossNodes {
		return 0, errors.Errorf("table-across-nodes index files are not supported: %s", filePath)
	}
	if fileIndex.Idx > seenDMLIdxMap[dmlKey] {
		seenDMLIdxMap[dmlKey] = fileIndex.Idx
	}
	consumedIdx := table.tableDMLIdxMap[dmlKey]
	if fileIndex.Idx <= consumedIdx {
		return 0, nil
	}
	return int(fileIndex.Idx - consumedIdx), nil
}

func (loader *loader) loadSchemaFiles(ctx context.Context, tbl *tableState, scan *tableScan) error {
	requiredSchemaVersions := make(map[uint64]struct{})
	needsCurrentMeta := false
	for key := range scan.dmlFileMap {
		requiredSchemaVersions[key.TableVersion] = struct{}{}
		if key.IsSchemaFileDMLPathKey() && key.TableVersion > tbl.ddlTableVersionWatermark {
			needsCurrentMeta = true
		}
	}

	var baselineVersion uint64
	if needsCurrentMeta && tbl.currentMeta == nil && tbl.ddlTableVersionWatermark != 0 {
		for version := range scan.schemaFilePaths {
			if version <= tbl.ddlTableVersionWatermark && version > baselineVersion {
				baselineVersion = version
			}
		}
		if baselineVersion == 0 {
			return errors.Errorf("applied schema file for table %s before table version %d not found",
				tbl.tableFQN, tbl.ddlTableVersionWatermark)
		}
		requiredSchemaVersions[baselineVersion] = struct{}{}
	}

	for version := range requiredSchemaVersions {
		objectPath, ok := scan.schemaFilePaths[version]
		if !ok {
			return errors.Errorf("schema file for table %s version %d not found", tbl.tableFQN, version)
		}

		schemaFile, err := loader.readSchemaFile(ctx, objectPath)
		if err != nil {
			return errors.Trace(err)
		}
		if schemaFile.TableVersion != version {
			return errors.Errorf("schema file metadata mismatch: table %s path %s", tbl.tableFQN, objectPath)
		}
		if schemaFile.Schema != tbl.sourceDatabase || schemaFile.Table != tbl.sourceTable {
			return errors.Errorf("schema file metadata mismatch: table %s path %s", tbl.tableFQN, objectPath)
		}
		scan.schemaFiles[version] = schemaFile
	}

	if baselineVersion != 0 {
		tbl.currentMeta = table.FromSchemaFile(scan.schemaFiles[baselineVersion])
	}
	return nil
}

func (loader *loader) readSchemaFile(ctx context.Context, objectPath string) (cloudstorage.SchemaFile, error) {
	var schemaFile cloudstorage.SchemaFile
	schemaContent, err := loader.storage.ReadFile(ctx, objectPath)
	if err != nil {
		return schemaFile, errors.Trace(err)
	}
	if err = json.Unmarshal(schemaContent, &schemaFile); err != nil {
		return schemaFile, errors.Trace(err)
	}
	return schemaFile, nil
}

func (scan *tableScan) schemaFile(table *tableState, tableVersion uint64) cloudstorage.SchemaFile {
	schemaFile, ok := scan.schemaFiles[tableVersion]
	if ok {
		return schemaFile
	}
	log.Panic("schema file not found",
		zap.String("table", table.tableFQN),
		zap.Any("tableVersion", tableVersion),
		zap.Any("schemaFiles", scan.schemaFiles))
	return cloudstorage.SchemaFile{}
}

func (loader *loader) syncExecDMLEvents(
	ctx context.Context,
	bounds scanBounds,
	tbl *tableState,
	schemaFile cloudstorage.SchemaFile,
	key cloudstorage.DMLPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(&cloudstorage.FileIndex{
		Idx: fileIdx,
	}, storage.CSVFileExtension, config.DefaultFileIndexWidth)
	objectPath := path.Join(loader.storageDir, filePath)

	err := loader.conn.LoadIncrement(ctx, table.FromSchemaFile(schemaFile), objectPath, bounds.checkpointTs)
	if err != nil {
		log.Error("failed to load DML file into data warehouse",
			zap.Error(err),
			zap.String("table", tbl.tableFQN),
			zap.String("filePath", objectPath),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Uint64("fileIndex", fileIdx))
		return errors.Trace(err)
	}
	return nil
}

func (loader *loader) execDDL(ctx context.Context, tbl *tableState, schemaFile cloudstorage.SchemaFile) error {
	newMeta := table.FromSchemaFile(schemaFile)
	action := model.ActionType(schemaFile.Type)
	if len(newMeta.PrimaryKeys) == 0 {
		return errors.Errorf("table %s has no primary key in schema file table version %d", tbl.tableFQN, schemaFile.TableVersion)
	}
	if len(schemaFile.Query) == 0 {
		log.Info("initializing table schema from schema file",
			zap.String("table", tbl.tableFQN), zap.Uint64("tableVersion", schemaFile.TableVersion))
		if schemaFile.TableVersion > tbl.ddlTableVersionWatermark {
			if err := loader.state.SetDDLTableVersionWatermark(ctx, tbl.tableFQN, schemaFile.TableVersion); err != nil {
				return errors.Trace(err)
			}
			tbl.ddlTableVersionWatermark = schemaFile.TableVersion
		}
		tbl.currentMeta = newMeta
		return nil
	}
	if schemaFile.TableVersion <= tbl.ddlTableVersionWatermark {
		tbl.currentMeta = newMeta
		log.Info("skip applied DDL",
			zap.String("table", tbl.tableFQN),
			zap.Uint64("tableVersion", schemaFile.TableVersion),
			zap.Uint64("ddlTableVersionWatermark", tbl.ddlTableVersionWatermark))
		return nil
	}

	return loader.applyDDL(ctx, tbl, schemaFile, newMeta, action)
}

func (loader *loader) applyDDL(
	ctx context.Context,
	tbl *tableState,
	schemaFile cloudstorage.SchemaFile,
	newMeta *table.Meta,
	action model.ActionType,
) error {
	ddls, err := snowflake.GenDDLViaTiDBDDL(loader.conn.TargetDatabase, tbl.currentMeta, newMeta, action, schemaFile.Query)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("executing DDL from schema file",
		zap.String("table", tbl.tableFQN),
		zap.Uint64("tableVersion", schemaFile.TableVersion),
		zap.String("query", schemaFile.Query),
		zap.Int("columnCount", len(schemaFile.Columns)))
	for i, ddl := range ddls {
		if err := loader.conn.ExecDDL(ctx, ddl); err != nil {
			return errors.Annotate(err, ddlExecutionErrorMessage(tbl.tableFQN, i, ddls))
		}
	}
	tbl.currentMeta = newMeta
	if err := loader.state.SetDDLTableVersionWatermark(ctx, tbl.tableFQN, schemaFile.TableVersion); err != nil {
		return errors.Trace(err)
	}
	tbl.ddlTableVersionWatermark = schemaFile.TableVersion
	metrics.AddCounter(metrics.TableVersionsCounter, float64(schemaFile.TableVersion), fmt.Sprintf("%s/%s", tbl.sourceDatabase, tbl.sourceTable))

	log.Info("DDL marked as applied in state",
		zap.String("table", tbl.tableFQN),
		zap.Uint64("tableVersion", schemaFile.TableVersion))
	return nil
}

func ddlExecutionErrorMessage(table string, failedIndex int, ddls []string) string {
	return fmt.Sprintf("execute DDL %d/%d for %s failed\nfailed DDL:\n%s\ngenerated DDLs:\n%s\n"+
		"if necessary, manually execute the remaining DDLs in data warehouse, "+
		"update the ddl_table_version_watermark of %s in state if all generated DDLs have been applied, "+
		"and restart the program",
		failedIndex+1,
		len(ddls),
		table,
		ddls[failedIndex],
		strings.Join(ddls, "\n"),
		table)
}

func (loader *loader) handleNewFiles(ctx context.Context, bounds scanBounds, table *tableState, scan *tableScan) (scanSummary, error) {
	if len(scan.dmlFileMap) == 0 {
		return scanSummary{}, nil
	}

	summary := scanSummary{activeTables: 1}
	dmlPathKeys := make([]cloudstorage.DMLPathKey, 0, len(scan.dmlFileMap))
	for k := range scan.dmlFileMap {
		dmlPathKeys = append(dmlPathKeys, k)
	}
	slices.SortStableFunc(dmlPathKeys, func(x, y cloudstorage.DMLPathKey) int {
		return cloudstorage.CompareDMLPathKey(x, y)
	})
	log.Info("new increment ranges found",
		zap.String("table", table.tableFQN),
		zap.Int("rangeCount", len(dmlPathKeys)),
		zap.Uint64("fileCount", countFilesInRanges(scan.dmlFileMap)))

	for _, key := range dmlPathKeys {
		schemaFile := scan.schemaFile(table, key.SchemaPathKey.TableVersion)
		if key.IsSchemaFileDMLPathKey() {
			if err := loader.execDDL(ctx, table, schemaFile); err != nil {
				return summary, errors.Trace(err)
			}
			continue
		}

		fileRange := scan.dmlFileMap[key]
		log.Info("processing increment range",
			zap.String("table", table.tableFQN),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Uint64("startFileIndex", fileRange.start),
			zap.Uint64("endFileIndex", fileRange.end))
		for i := fileRange.start; i <= fileRange.end; i++ {
			if err := loader.syncExecDMLEvents(ctx, bounds, table, schemaFile, key, i); err != nil {
				return summary, errors.Trace(err)
			}
			summary.loadedFiles++
			table.markDMLPathConsumed(key, i)
		}
	}
	return summary, nil
}

func countFilesInRanges(ranges map[cloudstorage.DMLPathKey]indexRange) uint64 {
	var count uint64
	for key, idxRange := range ranges {
		if key.IsSchemaFileDMLPathKey() {
			continue
		}
		if idxRange.end >= idxRange.start {
			count += idxRange.end - idxRange.start + 1
		}
	}
	return count
}
