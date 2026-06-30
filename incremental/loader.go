package incremental

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
	tableConcurrency = workerpool.DefaultConcurrency
)

type Config struct {
	Snowflake    *snowflake.Config
	Credential   *credentials.Value
	Tables       []string
	StorageURI   *url.URL
	StorageDir   string
	ScanInterval time.Duration
}

func Load(ctx context.Context, cfg Config, store *storage.Storage, stateManager state.Manager) error {
	log.Info("starting Snowflake incremental load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.Int("tableConcurrency", tableConcurrency),
		zap.Duration("scanInterval", cfg.ScanInterval))

	conn, err := snowflake.NewConnector(
		cfg.Snowflake,
		snowflake.IncrementStageName,
		cfg.StorageURI,
		cfg.Credential,
		"",
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	loader := newLoader(cfg, store, conn, stateManager)
	if err := loader.run(ctx); err != nil {
		return errors.Trace(err)
	}

	log.Info("Snowflake incremental load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func (loader *loader) run(ctx context.Context) error {
	pool := workerpool.New(tableConcurrency)
	pool.Go(ctx)
	defer pool.Close()

	ticker := time.NewTicker(loader.scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		checkpointTs, highWatermark, hasScan, err := loader.beginScan(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !hasScan {
			continue
		}
		if err := loader.processTables(ctx, checkpointTs, highWatermark, pool); err != nil {
			return errors.Trace(err)
		}
		if err := loader.state.FinishIncrementalScan(ctx, highWatermark); err != nil {
			return errors.Trace(err)
		}
		log.Info("incremental scan completed",
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("highWatermark", highWatermark))
	}
}

// indexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type indexRange struct {
	start uint64
	end   uint64
}

type incrementalScanStats struct {
	objectFiles  int
	schemaFiles  int
	indexFiles   int
	pendingFiles int
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
	tableDMLIdxMap           map[cloudstorage.DMLPathKey]uint64
	currentMeta              *table.Meta
	ddlTableVersionWatermark uint64
	sourceDatabase           string
	sourceTable              string
	tableFQN                 string
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
		storedTable := st.Incremental.Tables[tableFQN]
		metrics.AddGauge(metrics.IncrementPendingSizeGauge, 0, tableFQN)
		table := &tableState{
			tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]uint64),
			ddlTableVersionWatermark: storedTable.DDLTableVersionWatermark,
			sourceDatabase:           sourceDatabase,
			sourceTable:              sourceTable,
			tableFQN:                 tableFQN,
		}
		for scope, idx := range storedTable.DMLFileWatermarks {
			key, err := parseDMLScopeKey(table, scope)
			if err != nil {
				log.Panic("invalid dml watermark scope",
					zap.String("table", table.tableFQN),
					zap.String("scope", scope),
					zap.Error(err))
			}
			table.tableDMLIdxMap[key] = idx
		}
		l.tables = append(l.tables, table)
	}
	return l
}

func dmlScopeKey(key cloudstorage.DMLPathKey) string {
	return fmt.Sprintf("%d/%d/%s", key.TableVersion, key.PartitionNum, key.Date)
}

func parseDMLScopeKey(table *tableState, scope string) (cloudstorage.DMLPathKey, error) {
	parts := strings.Split(scope, "/")
	if len(parts) != 3 {
		return cloudstorage.DMLPathKey{}, errors.Errorf("invalid dml scope %q", scope)
	}
	tableVersion, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return cloudstorage.DMLPathKey{}, errors.Trace(err)
	}
	partitionNum, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return cloudstorage.DMLPathKey{}, errors.Trace(err)
	}
	dmlKey := cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       table.sourceDatabase,
			Table:        table.sourceTable,
			TableVersion: tableVersion,
		},
		PartitionNum: partitionNum,
		Date:         parts[2],
	}
	return dmlKey, nil
}

func (loader *loader) beginScan(ctx context.Context) (uint64, uint64, bool, error) {
	st := loader.state.Snapshot()
	checkpointTs := st.Incremental.CheckpointTS
	if st.Incremental.Scan != nil {
		return checkpointTs, st.Incremental.Scan.HighWatermark, true, nil
	}

	highWatermark, ok, err := loader.readMetadata(ctx)
	if err != nil {
		return 0, 0, false, err
	}
	if !ok || highWatermark <= checkpointTs {
		return checkpointTs, 0, false, nil
	}
	if err := loader.state.StartIncrementalScan(ctx, highWatermark); err != nil {
		return 0, 0, false, errors.Trace(err)
	}
	return checkpointTs, highWatermark, true, nil
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
	highWatermark uint64,
	table *tableState,
	renameSchemaFilePaths map[uint64]string,
) error {
	scan, err := loader.getNewFiles(ctx, checkpointTs, highWatermark, table, renameSchemaFilePaths)
	if err != nil {
		metrics.AddCounter(metrics.ErrorCounter, 1, table.tableFQN)
		log.Error("incremental scan failed", zap.String("table", table.tableFQN), zap.Error(err))
		return errors.Trace(err)
	}
	if len(scan.dmlFileMap) == 0 {
		return nil
	}
	if err := loader.loadSchemaFiles(ctx, table, scan); err != nil {
		metrics.AddCounter(metrics.ErrorCounter, 1, table.tableFQN)
		log.Error("incremental scan failed", zap.String("table", table.tableFQN), zap.Error(err))
		return errors.Trace(err)
	}

	err = loader.handleNewFiles(ctx, highWatermark, table, scan)
	if err != nil {
		metrics.AddCounter(metrics.ErrorCounter, 1, table.tableFQN)
		log.Error("incremental load failed", zap.String("table", table.tableFQN), zap.Error(err))
	}
	return errors.Trace(err)
}

func (loader *loader) processTables(ctx context.Context, checkpointTs, highWatermark uint64, pool *workerpool.Pool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	renameSchemaFilePaths, err := loader.findRenameSchemaFilePaths(ctx, highWatermark)
	if err != nil {
		return errors.Trace(err)
	}

	futures := make([]*workerpool.Future, 0, len(loader.tables))
	for _, table := range loader.tables {
		task := workerpool.TaskFunc(func(ctx context.Context) error {
			return loader.processTable(ctx, checkpointTs, highWatermark, table, renameSchemaFilePaths[table.tableFQN])
		})
		future, err := pool.Submit(ctx, task)
		if err != nil {
			return errors.Trace(err)
		}
		futures = append(futures, future)
	}

	for _, future := range futures {
		if err := future.Wait(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (loader *loader) parseSchemaFilePath(
	highWatermark uint64,
	table *tableState,
	objectPath string,
	schemaFilePaths map[uint64]string,
	seenDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
) {
	filePath := strings.TrimPrefix(objectPath, loader.storageDir+"/")

	var schemaKey cloudstorage.SchemaPathKey
	schemaKey.Parse(filePath)
	// The scan high watermark freezes the source-visible upper bound for this round.
	if schemaKey.TableVersion > highWatermark {
		return
	}
	schemaFilePaths[schemaKey.TableVersion] = objectPath
	// ddlTableVersionWatermark tracks schema versions already handled; keep the
	// path for baseline recovery, but do not schedule this schema as new work.
	if schemaKey.TableVersion <= table.ddlTableVersionWatermark {
		return
	}

	dmlkey := cloudstorage.NewSchemaFileDMLPathKey(schemaKey)
	seenDMLIdxMap[dmlkey] = 0
}

func (loader *loader) findRenameSchemaFilePaths(ctx context.Context, highWatermark uint64) (map[string]map[uint64]string, error) {
	tablesByDatabase := make(map[string]map[string]string)
	for _, table := range loader.tables {
		if tablesByDatabase[table.sourceDatabase] == nil {
			tablesByDatabase[table.sourceDatabase] = make(map[string]string)
		}
		tablesByDatabase[table.sourceDatabase][strings.ToLower(table.sourceTable)] = table.tableFQN
	}

	result := make(map[string]map[uint64]string)
	for database, tables := range tablesByDatabase {
		tableDirs, err := loader.storage.ListDirs(ctx, path.Join(loader.storageDir, database))
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, tableDir := range tableDirs {
			if tableDir == "meta" {
				continue
			}
			schemaMetaDir := path.Join(loader.storageDir, database, tableDir, "meta")
			err := loader.storage.WalkDir(ctx, &storeapi.WalkOption{
				SubDir:    schemaMetaDir,
				ObjPrefix: "schema_",
			}, func(objectPath string, _ int64) error {
				filePath := strings.TrimPrefix(objectPath, loader.storageDir+"/")
				var schemaKey cloudstorage.SchemaPathKey
				schemaKey.Parse(filePath)
				if schemaKey.TableVersion > highWatermark {
					return nil
				}

				schemaFile, err := loader.readSchemaFile(ctx, objectPath)
				if err != nil {
					return errors.Trace(err)
				}
				if model.ActionType(schemaFile.Type) != model.ActionRenameTable {
					return nil
				}

				sourceSchema, sourceTable, ok, err := renameTableSourceForTarget(
					schemaFile.Query,
					schemaFile.Schema,
					schemaFile.Table,
				)
				if err != nil {
					return errors.Trace(err)
				}
				if !ok {
					return nil
				}
				if sourceSchema == "" {
					sourceSchema = schemaFile.Schema
				}
				if !strings.EqualFold(sourceSchema, database) {
					return nil
				}
				tableFQN, ok := tables[strings.ToLower(sourceTable)]
				if !ok {
					return nil
				}
				if result[tableFQN] == nil {
					result[tableFQN] = make(map[uint64]string)
				}
				result[tableFQN][schemaFile.TableVersion] = objectPath
				return nil
			})
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return result, nil
}

func renameTableSourceForTarget(query, targetSchema, targetTable string) (string, string, bool, error) {
	stmt, err := parser.New().ParseOneStmt(query, "", "")
	if err != nil {
		return "", "", false, errors.Trace(err)
	}
	switch stmt := stmt.(type) {
	case *ast.RenameTableStmt:
		for _, tableToTable := range stmt.TableToTables {
			if astTableMatches(tableToTable.NewTable, targetSchema, targetTable) {
				return tableToTable.OldTable.Schema.O, tableToTable.OldTable.Name.O, true, nil
			}
		}
	case *ast.AlterTableStmt:
		for _, spec := range stmt.Specs {
			if spec.Tp == ast.AlterTableRenameTable &&
				astTableMatches(spec.NewTable, targetSchema, targetTable) {
				return stmt.Table.Schema.O, stmt.Table.Name.O, true, nil
			}
		}
	default:
	}
	return "", "", false, nil
}

func astTableMatches(tableName *ast.TableName, schema, table string) bool {
	if tableName.Schema.O != "" && !strings.EqualFold(tableName.Schema.O, schema) {
		return false
	}
	return strings.EqualFold(tableName.Name.O, table)
}

// diffDMLMaps returns seen files that have not been consumed yet.
func diffDMLMaps(
	seenDMLIdxMap, consumedDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
) map[cloudstorage.DMLPathKey]indexRange {
	resMap := make(map[cloudstorage.DMLPathKey]indexRange)
	for dmlKey, idx := range seenDMLIdxMap {
		origIdx, ok := consumedDMLIdxMap[dmlKey]
		if !ok {
			resMap[dmlKey] = indexRange{
				start: 1,
				end:   idx,
			}
			continue
		}
		if idx > origIdx {
			resMap[dmlKey] = indexRange{
				start: origIdx + 1,
				end:   idx,
			}
		}
	}
	return resMap
}

// getNewFiles returns newly created dml files in specific ranges.
func (loader *loader) getNewFiles(
	ctx context.Context,
	checkpointTs uint64,
	highWatermark uint64,
	table *tableState,
	renameSchemaFilePaths map[uint64]string,
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
		loader.parseSchemaFilePath(highWatermark, table, objectPath, scan.schemaFilePaths, seenDMLIdxMap)
		return nil
	})
	if err != nil {
		return scan, err
	}
	for tableVersion, objectPath := range renameSchemaFilePaths {
		if tableVersion > highWatermark {
			continue
		}
		scan.schemaFilePaths[tableVersion] = objectPath
		if tableVersion <= table.ddlTableVersionWatermark {
			continue
		}
		schemaKey := cloudstorage.SchemaPathKey{
			Schema:       table.sourceDatabase,
			Table:        table.sourceTable,
			TableVersion: tableVersion,
		}
		seenDMLIdxMap[cloudstorage.NewSchemaFileDMLPathKey(schemaKey)] = 0
	}

	var activeVersion uint64
	schemaVersions := make([]uint64, 0, len(scan.schemaFilePaths))
	for version := range scan.schemaFilePaths {
		if version <= checkpointTs {
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
		dateDirs, err := loader.storage.ListDirs(ctx, versionDir)
		if err != nil {
			return scan, errors.Trace(err)
		}
		for _, dateDir := range dateDirs {
			indexFilePath := path.Join(versionDir, dateDir, "meta", dmlIndexFileName)
			stats.objectFiles++
			stats.indexFiles++
			pending, err := loader.parseDMLIndexFile(ctx, highWatermark, table, indexFilePath, seenDMLIdxMap)
			if err != nil {
				return scan, errors.Trace(err)
			}
			stats.pendingFiles += pending
		}
	}

	scan.dmlFileMap = diffDMLMaps(seenDMLIdxMap, table.tableDMLIdxMap)
	if len(scan.dmlFileMap) > 0 || stats.pendingFiles > 0 {
		log.Info("increment storage scan completed",
			zap.String("table", table.tableFQN),
			zap.Int("newRanges", len(scan.dmlFileMap)),
			zap.Int("objectFiles", stats.objectFiles),
			zap.Int("schemaFiles", stats.schemaFiles),
			zap.Int("indexFiles", stats.indexFiles),
			zap.Int("pendingFiles", stats.pendingFiles))
	}
	return scan, nil
}

func (loader *loader) parseDMLIndexFile(
	ctx context.Context,
	highWatermark uint64,
	table *tableState,
	objectPath string,
	seenDMLIdxMap map[cloudstorage.DMLPathKey]uint64,
) (int, error) {
	filePath := strings.TrimPrefix(objectPath, loader.storageDir+"/")
	var dmlKey cloudstorage.DMLPathKey
	if err := dmlKey.ParseIndexFilePath(config.DateSeparatorDay.String(), filePath); err != nil {
		return 0, errors.Trace(err)
	}

	// Ignore index files for table versions newer than this scan's frozen source bound.
	if dmlKey.TableVersion > highWatermark {
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
	origIdx := table.tableDMLIdxMap[dmlKey]
	if fileIndex.Idx <= origIdx {
		return 0, nil
	}
	return int(fileIndex.Idx - origIdx), nil
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
			ok, err := schemaFileRenamesTable(schemaFile, tbl)
			if err != nil {
				return errors.Trace(err)
			}
			if !ok {
				return errors.Errorf("schema file metadata mismatch: table %s path %s", tbl.tableFQN, objectPath)
			}
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

func schemaFileRenamesTable(schemaFile cloudstorage.SchemaFile, tbl *tableState) (bool, error) {
	if model.ActionType(schemaFile.Type) != model.ActionRenameTable {
		return false, nil
	}
	sourceSchema, sourceTable, ok, err := renameTableSourceForTarget(
		schemaFile.Query,
		schemaFile.Schema,
		schemaFile.Table,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !ok {
		return false, nil
	}
	if sourceSchema == "" {
		sourceSchema = tbl.sourceDatabase
	}
	return strings.EqualFold(sourceSchema, tbl.sourceDatabase) &&
		strings.EqualFold(sourceTable, tbl.sourceTable), nil
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
	highWatermark uint64,
	tbl *tableState,
	schemaFile cloudstorage.SchemaFile,
	key cloudstorage.DMLPathKey,
	fileIdx uint64,
) (bool, error) {
	filePath := key.GenerateDMLFilePath(&cloudstorage.FileIndex{
		Idx: fileIdx,
	}, storage.CSVFileExtension, config.DefaultFileIndexWidth)
	objectPath := path.Join(loader.storageDir, filePath)

	fullyConsumed, err := loader.conn.LoadIncrement(table.FromSchemaFile(schemaFile), objectPath, highWatermark)
	if err != nil {
		log.Error("failed to load DML file into data warehouse",
			zap.Error(err),
			zap.String("table", tbl.tableFQN),
			zap.String("filePath", objectPath),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Uint64("fileIndex", fileIdx))
		return false, errors.Trace(err)
	}
	if !fullyConsumed {
		log.Info("DML file partially loaded",
			zap.String("table", tbl.tableFQN),
			zap.String("filePath", objectPath),
			zap.Uint64("highWatermark", highWatermark),
			zap.Uint64("fileIndex", fileIdx))
		return false, nil
	}
	scopeKey := dmlScopeKey(key)
	if err := loader.state.SetDMLFileWatermark(ctx, tbl.tableFQN, scopeKey, fileIdx); err != nil {
		return false, errors.Trace(err)
	}
	if fileIdx > tbl.tableDMLIdxMap[key] {
		tbl.tableDMLIdxMap[key] = fileIdx
	}

	return true, nil
}

func (loader *loader) execDDL(ctx context.Context, tbl *tableState, schemaFile cloudstorage.SchemaFile) error {
	newMeta := table.FromSchemaFile(schemaFile)
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

	ddls, err := snowflake.GenDDLViaTiDBDDL(tbl.currentMeta, newMeta, model.ActionType(schemaFile.Type), schemaFile.Query)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("executing DDL from schema file",
		zap.String("table", tbl.tableFQN),
		zap.Uint64("tableVersion", schemaFile.TableVersion),
		zap.String("query", schemaFile.Query),
		zap.Int("columnCount", len(schemaFile.Columns)))
	for i, ddl := range ddls {
		if err := loader.conn.ExecDDL(ddl); err != nil {
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

func (loader *loader) handleNewFiles(ctx context.Context, highWatermark uint64, table *tableState, scan *tableScan) error {
	if len(scan.dmlFileMap) == 0 {
		return nil
	}

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
				return errors.Trace(err)
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
			fullyConsumed, err := loader.syncExecDMLEvents(ctx, highWatermark, table, schemaFile, key, i)
			if err != nil {
				return errors.Trace(err)
			}
			if !fullyConsumed {
				return nil
			}
		}
	}
	return nil
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
