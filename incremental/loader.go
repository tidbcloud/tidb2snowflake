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
		logIncrementalScanProgress(bounds, hasScan)
		if !hasScan {
			continue
		}
		start := time.Now()
		summary, err := loader.processTables(ctx, bounds.checkpointTs, pool)
		if err != nil {
			return errors.Trace(err)
		}
		if err := loader.state.SetCheckpointTS(ctx, bounds.targetCheckpointTs); err != nil {
			log.Error("incremental scan completed, but set target checkpoint to state failed",
				zap.Uint64("checkpointTs", bounds.checkpointTs), zap.Uint64("targetCheckpointTs", bounds.targetCheckpointTs),
				zap.Error(err))
			return errors.Trace(err)
		}
		log.Info("incremental scan completed",
			zap.Uint64("checkpointTs", bounds.checkpointTs),
			zap.Uint64("targetCheckpointTs", bounds.targetCheckpointTs),
			zap.Int("activeTables", summary.activeTables),
			zap.Uint64("loadedFiles", summary.loadedFiles),
			zap.Duration("duration", time.Since(start)))
	}
}

func logIncrementalScanProgress(bounds scanBounds, hasScan bool) {
	log.Info("incremental scan progress",
		zap.Uint64("checkpointTs", bounds.checkpointTs),
		zap.Uint64("targetCheckpointTs", bounds.targetCheckpointTs),
		zap.Bool("hasScan", hasScan))
}

// indexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type indexRange struct {
	start uint64
	end   uint64
}

type incrementalScanStats struct {
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
	tableDMLIdxMap       map[cloudstorage.DMLPathKey]uint64
	activeDateByDMLScope map[dmlScope]string
	dmlCursors           map[string]state.DMLCursor
	dirtyDMLCursors      map[string]state.DMLCursor
	currentMeta          *table.Meta
	// ddlTableVersionWatermark mirrors the persisted per-table sync lower bound.
	// Schema versions below it are skipped; the equal version is the current
	// applied schema and may still have DML with commit_ts above the watermark.
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

	targetCheckpointTs, ok, err := loader.readMetadata(ctx)
	if err != nil {
		return scanBounds{}, false, err
	}
	if !ok {
		return scanBounds{checkpointTs: checkpointTs}, false, nil
	}
	bounds := scanBounds{checkpointTs: checkpointTs, targetCheckpointTs: targetCheckpointTs}
	if targetCheckpointTs < checkpointTs {
		log.Warn("incremental metadata checkpoint is behind state checkpoint, this should not happen",
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("targetCheckpointTs", targetCheckpointTs))
		return bounds, false, nil
	}
	if targetCheckpointTs == checkpointTs {
		log.Warn("targetCheckpoint is stuck, skip the scan",
			zap.Uint64("checkpointTs", bounds.checkpointTs),
			zap.Uint64("targetCheckpointTs", bounds.targetCheckpointTs))
		return bounds, false, nil
	}
	return bounds, true, nil
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
	checkpointTs uint64,
	table *tableState,
) (scanSummary, error) {
	scan, err := loader.getNewFiles(ctx, checkpointTs, table)
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

	summary, err := loader.handleNewFiles(ctx, checkpointTs, table, scan)
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

func (loader *loader) processTables(ctx context.Context, checkpointTs uint64, pool *workerpool.Pool) (scanSummary, error) {
	summaries := make([]scanSummary, len(loader.tables))
	group := pool.NewGroup(ctx, 0)
	var first error
	for i, table := range loader.tables {
		task := workerpool.TaskFunc(func(ctx context.Context) error {
			summary, err := loader.processTable(ctx, checkpointTs, table)
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
	objectPath string,
	schemaFilePaths map[uint64]string,
	seenDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
) {
	filePath := strings.TrimPrefix(objectPath, loader.storageDir+"/")

	var schemaKey cloudstorage.SchemaPathKey
	schemaKey.Parse(filePath)
	schemaFilePaths[schemaKey.TableVersion] = objectPath

	dmlkey := cloudstorage.NewSchemaFileDMLPathKey(schemaKey)
	seenDMLIdxMap[dmlkey] = 0
}

func parseSchemaFileVersion(objectPath string) (uint64, error) {
	name := path.Base(objectPath)
	if !strings.HasPrefix(name, "schema_") || !strings.HasSuffix(name, ".json") {
		return 0, errors.Errorf("invalid schema file path %s", objectPath)
	}
	versionPart := strings.TrimSuffix(strings.TrimPrefix(name, "schema_"), ".json")
	versionText, _, ok := strings.Cut(versionPart, "_")
	if !ok {
		return 0, errors.Errorf("invalid schema file path %s", objectPath)
	}
	version, err := strconv.ParseUint(versionText, 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return version, nil
}

// getNewFiles returns newly created dml files in specific ranges.
func (loader *loader) getNewFiles(
	ctx context.Context,
	checkpointTs uint64,
	table *tableState,
) (*tableScan, error) {
	scan := &tableScan{
		dmlFileMap:      make(map[cloudstorage.DMLPathKey]indexRange),
		schemaFilePaths: make(map[uint64]string),
		schemaFiles:     make(map[uint64]cloudstorage.SchemaFile),
	}
	newDMLIdxMap := make(map[cloudstorage.DMLPathKey]uint64)
	stats := incrementalScanStats{}

	var (
		// Before a table-level watermark exists, the latest schema at or below
		// the global checkpoint is only a baseline used to rebuild currentMeta
		// before applying the first incremental DDL.
		baselineSchemaVersion uint64
		baselineSchemaPath    string
		currentSchemaFound    bool
	)
	schemaMetaDir := path.Join(loader.storageDir, table.sourceDatabase, table.sourceTable, "meta")
	err := loader.storage.WalkDir(ctx, &storeapi.WalkOption{
		SubDir:    schemaMetaDir,
		ObjPrefix: "schema_",
	}, func(objectPath string, _ int64) error {
		schemaVersion, err := parseSchemaFileVersion(objectPath)
		if err != nil {
			return errors.Trace(err)
		}

		if table.ddlTableVersionWatermark > 0 {
			// Once the table watermark exists, it is the per-table sync lower
			// bound. Older schema epochs are already crossed and must not be
			// scanned again, even if the global checkpoint is lower.
			if schemaVersion < table.ddlTableVersionWatermark {
				return nil
			}
			// The schema at the watermark is the current applied schema. Keep
			// its path so DML under this epoch can be decoded, but do not add a
			// synthetic DDL key because the DDL has already been applied.
			if schemaVersion == table.ddlTableVersionWatermark {
				scan.schemaFilePaths[schemaVersion] = objectPath
				currentSchemaFound = true
				return nil
			}
			// Newer schema files represent pending DDLs. parseSchemaFilePath
			// records the schema path and adds a synthetic DML key so replay
			// orders the DDL before DML in that schema epoch.
			loader.parseSchemaFilePath(objectPath, scan.schemaFilePaths, newDMLIdxMap)
			return nil
		}

		if schemaVersion <= checkpointTs {
			// No table watermark means the table has not crossed any incremental
			// schema epoch yet. Keep only the latest checkpoint-covered schema
			// as the snapshot baseline; older schema files are not needed.
			if schemaVersion > baselineSchemaVersion {
				baselineSchemaVersion = schemaVersion
				baselineSchemaPath = objectPath
			}
			return nil
		}

		loader.parseSchemaFilePath(objectPath, scan.schemaFilePaths, newDMLIdxMap)
		return nil
	})
	if err != nil {
		return scan, err
	}
	if table.ddlTableVersionWatermark > 0 && !currentSchemaFound {
		// The persisted watermark says this schema epoch is current. Falling
		// back to an older schema would violate the table-level lower bound.
		return scan, errors.Errorf("current schema file for table %s watermark %d not found",
			table.tableFQN, table.ddlTableVersionWatermark)
	}
	if baselineSchemaPath != "" {
		scan.schemaFilePaths[baselineSchemaVersion] = baselineSchemaPath
	}

	// schemaFilePaths already contains exactly the schema epochs this scan may
	// need:
	//   - without a table watermark: the snapshot baseline plus pending DDLs
	//   - with a table watermark: the current schema epoch plus pending DDLs
	//
	// Therefore every key in schemaFilePaths should be scanned. Older schema
	// epochs were filtered out while walking schema files, so do not re-derive
	// an active version from the global checkpoint here.
	schemaVersions := make([]uint64, 0, len(scan.schemaFilePaths))
	for version := range scan.schemaFilePaths {
		schemaVersions = append(schemaVersions, version)
	}
	slices.Sort(schemaVersions)

	// For each selected schema epoch, discover the latest CDC.index under every
	// active output stream. Non-partitioned tables write:
	//   <schema>/<table>/<tableVersion>/<date>/meta/CDC.index
	// Partitioned tables write:
	//   <schema>/<table>/<tableVersion>/<partition>/<date>/meta/CDC.index
	// The index file gives the highest data-file sequence visible for that
	// stream; later we compare it with the persisted DML cursor to build ranges.
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
				// Non-partitioned layout: the first level below tableVersion is
				// already a date directory. Skip dates older than the active
				// cursor date for this schema epoch.
				scope := dmlScope{tableVersion: tableVersion}
				if activeDate := table.activeDateByDMLScope[scope]; activeDate != "" && dir < activeDate {
					stats.skippedDateDirs++
					continue
				}
				pending, err := loader.parseDMLIndexFileIfExists(
					ctx,
					checkpointTs,
					table,
					path.Join(versionDir, dir),
					newDMLIdxMap,
					&stats,
				)
				if err != nil {
					return scan, errors.Trace(err)
				}
				stats.pendingFiles += pending
				continue
			}

			// Partitioned layout: the first level is a physical partition id,
			// and each partition has its own date directories and DML cursor.
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
				streamDir := path.Join(partitionDir, dateDir)
				pending, err := loader.parseDMLIndexFileIfExists(
					ctx,
					checkpointTs,
					table,
					streamDir,
					newDMLIdxMap,
					&stats,
				)
				if err != nil {
					return scan, errors.Trace(err)
				}
				stats.pendingFiles += pending
			}
		}
	}

	for key, fileIdx := range newDMLIdxMap {
		if key.IsSchemaFileDMLPathKey() {
			scan.dmlFileMap[key] = indexRange{}
			continue
		}
		consumedIdx := table.tableDMLIdxMap[key]
		if fileIdx > consumedIdx {
			scan.dmlFileMap[key] = indexRange{start: consumedIdx + 1, end: fileIdx}
		}
	}
	log.Info("increment storage scan completed",
		zap.String("table", table.tableFQN),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Int("newRanges", len(scan.dmlFileMap)),
		zap.Int("indexFiles", stats.indexFiles),
		zap.Int("skippedDateDirs", stats.skippedDateDirs),
		zap.Int("pendingFiles", stats.pendingFiles))
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

// parseDMLIndexFileIfExists checks the CDC.index file under one TiCDC DML
// stream directory. Missing index files are normal: a schema/date/partition
// stream may not have produced data yet, so absence means there is no new range.
// CDC.index contains the latest data file name, for example CDC000010.csv; the
// consumer expands that into the contiguous pending range after comparing with
// the persisted DML cursor.
func (loader *loader) parseDMLIndexFileIfExists(
	ctx context.Context,
	checkpointTs uint64,
	table *tableState,
	streamDir string,
	seenDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
	stats *incrementalScanStats,
) (int, error) {
	indexPath := path.Join(streamDir, "meta", dmlIndexFileName)
	exists, err := loader.storage.FileExists(ctx, indexPath)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !exists {
		return 0, nil
	}
	stats.indexFiles++
	// TiCDC's path parser expects a path relative to the incremental storage root,
	// <schema>/<table>/<tableVersion>/<date>/meta/CDC.index
	// <schema>/<table>/<tableVersion>/<partition>/<date>/meta/CDC.index
	// for example source/t/150/2026-07-01/meta/CDC.index.
	filePath := strings.TrimPrefix(indexPath, loader.storageDir+"/")
	var dmlKey cloudstorage.DMLPathKey
	if err := dmlKey.ParseIndexFilePath(config.DateSeparatorDay.String(), filePath); err != nil {
		return 0, errors.Trace(err)
	}

	data, err := loader.storage.ReadFile(ctx, indexPath)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// TiCDC writes the latest data file name into CDC.index after the data file
	// is durable. The file index tells us the upper bound of visible files.
	fileName := strings.TrimSpace(string(data))
	fileIndex, err := cloudstorage.ParseFileIndexFromFileName(fileName, storage.CSVFileExtension)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if fileIndex.EnableTableAcrossNodes {
		return 0, errors.Errorf("table-across-nodes index files are not supported: %s", filePath)
	}

	consumedIdx := table.tableDMLIdxMap[dmlKey]
	var (
		pendingStartIdx uint64
		pendingEndIdx   uint64

		pendingFilesCount int
	)
	if fileIndex.Idx > consumedIdx {
		pendingStartIdx = consumedIdx + 1
		pendingEndIdx = fileIndex.Idx
		pendingFilesCount = int(fileIndex.Idx - consumedIdx)
	}
	log.Info("increment index scanned",
		zap.String("table", table.tableFQN),
		zap.String("indexPath", indexPath),
		zap.String("latestFileName", fileName),
		zap.Uint64("latestFileIndex", fileIndex.Idx),
		zap.Uint64("consumedFileIndex", consumedIdx),
		zap.Uint64("pendingStartFileIndex", pendingStartIdx),
		zap.Uint64("pendingEndFileIndex", pendingEndIdx),
		zap.Int("pendingFilesCount", pendingFilesCount),
		zap.Uint64("checkpointTs", checkpointTs))
	if fileIndex.Idx > seenDMLIdxMap[dmlKey] {
		// A normal TiCDC layout has only one CDC.index for a DML key in a scan.
		// Keep this as a max update so duplicate discovery or future index
		// sources cannot move the visible upper bound backward before
		// getNewFiles compares it with the persisted cursor.
		seenDMLIdxMap[dmlKey] = fileIndex.Idx
	}
	return pendingFilesCount, nil
}

func (loader *loader) loadSchemaFiles(ctx context.Context, tbl *tableState, scan *tableScan) error {
	requiredSchemaVersions := make(map[uint64]struct{})
	var needsCurrentMeta bool
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
	checkpointTs uint64,
	tbl *tableState,
	schemaFile cloudstorage.SchemaFile,
	key cloudstorage.DMLPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(&cloudstorage.FileIndex{
		Idx: fileIdx,
	}, storage.CSVFileExtension, config.DefaultFileIndexWidth)
	objectPath := path.Join(loader.storageDir, filePath)

	err := loader.conn.LoadIncrement(ctx, table.FromSchemaFile(schemaFile), objectPath, checkpointTs)
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

func (loader *loader) handleNewFiles(
	ctx context.Context,
	checkpointTs uint64,
	table *tableState,
	scan *tableScan,
) (scanSummary, error) {
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
			if err := loader.syncExecDMLEvents(ctx, checkpointTs, table, schemaFile, key, i); err != nil {
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
