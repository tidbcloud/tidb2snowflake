package incremental

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
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
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

const (
	CSVFileExtension = ".csv"
	indexFileSuffix  = ".index"
	metadataFileName = "metadata"
	tableConcurrency = 8
)

type Config struct {
	Snowflake    *snowflake.Config
	Credential   *credentials.Value
	Tables       []string
	StorageURI   *url.URL
	StorageDir   string
	ScanInterval time.Duration
	State        state.Manager
}

func Load(ctx context.Context, cfg Config, store storeapi.Storage) error {
	if cfg.StorageURI == nil {
		return errors.New("incremental storage URI is empty")
	}
	if store == nil {
		return errors.New("incremental storage is empty")
	}
	if cfg.State == nil {
		return errors.New("incremental state manager is empty")
	}

	log.Info("starting Snowflake incremental load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.Int("tableConcurrency", tableConcurrency),
		zap.Duration("scanInterval", cfg.ScanInterval))

	conn, err := snowflake.NewConnector(
		cfg.Snowflake,
		"increment_external",
		cfg.StorageURI,
		cfg.Credential,
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	loader := newLoader(ctx, cfg, store, conn)
	if err := loader.run(); err != nil {
		return errors.Trace(err)
	}

	log.Info("Snowflake incremental load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func (loader *loader) run() error {
	if len(loader.tables) == 0 {
		return nil
	}

	ticker := time.NewTicker(loader.scanInterval)
	defer ticker.Stop()
	var round uint64
	for {
		select {
		case <-loader.ctx.Done():
			return loader.ctx.Err()
		case <-ticker.C:
		}
		round++
		hasScan, err := loader.beginScan()
		if err != nil {
			return errors.Trace(err)
		}
		if !hasScan {
			continue
		}
		work, err := loader.scanVisibleFiles()
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("incremental scan range ready",
			zap.Uint64("round", round),
			zap.Uint64("checkpointTs", loader.checkpointTs),
			zap.Uint64("highWatermark", loader.highWatermark),
			zap.Int("tableWorkCount", len(work)))
		if err := loader.consumeVisibleFiles(work); err != nil {
			return errors.Trace(err)
		}
		if err := loader.finishScan(); err != nil {
			return errors.Trace(err)
		}
	}
}

type storageMetadata struct {
	CheckpointTS uint64 `json:"checkpoint-ts"`
}

type fileIndexKeyMap map[cloudstorage.FileIndexKey]uint64

// indexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type indexRange struct {
	start uint64
	end   uint64
}

type fileIndexRange map[cloudstorage.FileIndexKey]indexRange

type incrementalScanStats struct {
	objectFiles  int
	schemaFiles  int
	indexFiles   int
	pendingFiles int
	ignoredFiles int
}

type tableWork struct {
	table     *tableState
	dmlRanges map[cloudstorage.DMLPathKey]fileIndexRange
}

type loader struct {
	conn          *snowflake.Connector
	storage       storeapi.Storage
	storageDir    string
	ctx           context.Context
	fileExtension string
	scanInterval  time.Duration
	state         state.Manager
	checkpointTs  uint64
	highWatermark uint64
	tables        []*tableState
}

type tableState struct {
	tableDMLIdxMap           map[cloudstorage.DMLPathKey]fileIndexKeyMap
	tableDefMap              map[uint64]*cloudstorage.SchemaFile
	currentMeta              *table.Meta
	ddlTableVersionWatermark uint64
	sourceDatabase           string
	sourceTable              string
	tableFQN                 string
}

func newLoader(ctx context.Context, cfg Config, store storeapi.Storage, conn *snowflake.Connector) *loader {
	l := &loader{
		conn:          conn,
		storage:       store,
		storageDir:    cfg.StorageDir,
		ctx:           ctx,
		fileExtension: CSVFileExtension,
		scanInterval:  cfg.ScanInterval,
		state:         cfg.State,
		tables:        make([]*tableState, 0, len(cfg.Tables)),
	}
	st := cfg.State.Snapshot()
	l.checkpointTs = st.Incremental.CheckpointTS
	if st.Incremental.Scan != nil {
		l.highWatermark = st.Incremental.Scan.HighWatermark
	}
	for _, tableFQN := range cfg.Tables {
		sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
		storedTable := st.Incremental.Tables[tableFQN]
		metrics.AddGauge(metrics.IncrementPendingSizeGauge, 0, tableFQN)
		table := &tableState{
			tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
			tableDefMap:              make(map[uint64]*cloudstorage.SchemaFile),
			ddlTableVersionWatermark: storedTable.DDLTableVersionWatermark,
			sourceDatabase:           sourceDatabase,
			sourceTable:              sourceTable,
			tableFQN:                 tableFQN,
		}
		for scope, idx := range storedTable.DMLFileWatermarks {
			key, fileIndexKey, err := parseDMLScopeKey(table, scope)
			if err != nil {
				log.Panic("invalid dml watermark scope",
					zap.String("table", table.tableFQN),
					zap.String("scope", scope),
					zap.Error(err))
			}
			if table.tableDMLIdxMap[key] == nil {
				table.tableDMLIdxMap[key] = make(fileIndexKeyMap)
			}
			table.tableDMLIdxMap[key][fileIndexKey] = idx
		}
		l.tables = append(l.tables, table)
	}
	return l
}

func (loader *loader) objectPath(relativePath string) string {
	if loader.storageDir == "" {
		return relativePath
	}
	return path.Join(loader.storageDir, relativePath)
}

func (loader *loader) relativePath(objectPath string) string {
	if loader.storageDir == "" {
		return objectPath
	}
	return strings.TrimPrefix(objectPath, strings.TrimSuffix(loader.storageDir, "/")+"/")
}

func dmlScopeKey(key cloudstorage.DMLPathKey, fileIndexKey cloudstorage.FileIndexKey) string {
	return fmt.Sprintf("%d/%d/%s/%s", key.TableVersion, key.PartitionNum, key.Date, fileIndexKey.DispatcherID)
}

func parseDMLScopeKey(table *tableState, scope string) (cloudstorage.DMLPathKey, cloudstorage.FileIndexKey, error) {
	parts := strings.Split(scope, "/")
	if len(parts) != 4 {
		return cloudstorage.DMLPathKey{}, cloudstorage.FileIndexKey{}, errors.Errorf("invalid dml scope %q", scope)
	}
	tableVersion, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return cloudstorage.DMLPathKey{}, cloudstorage.FileIndexKey{}, errors.Trace(err)
	}
	partitionNum, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return cloudstorage.DMLPathKey{}, cloudstorage.FileIndexKey{}, errors.Trace(err)
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
	fileIndexKey := cloudstorage.FileIndexKey{
		DispatcherID:           parts[3],
		EnableTableAcrossNodes: parts[3] != "",
	}
	return dmlKey, fileIndexKey, nil
}

func (loader *loader) beginScan() (bool, error) {
	st := loader.state.Snapshot()
	loader.checkpointTs = st.Incremental.CheckpointTS
	if st.Incremental.Scan != nil {
		loader.highWatermark = st.Incremental.Scan.HighWatermark
		return true, nil
	}

	checkpointTs, ok, err := loader.readMetadata()
	if err != nil {
		return false, err
	}
	if !ok || checkpointTs <= loader.checkpointTs {
		return false, nil
	}
	if err := loader.state.StartIncrementalScan(loader.ctx, checkpointTs); err != nil {
		return false, errors.Trace(err)
	}
	loader.highWatermark = checkpointTs
	return true, nil
}

func (loader *loader) readMetadata() (uint64, bool, error) {
	metadataPath := loader.objectPath(metadataFileName)
	exists, err := loader.storage.FileExists(loader.ctx, metadataPath)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if !exists {
		return 0, false, nil
	}
	data, err := loader.storage.ReadFile(loader.ctx, metadataPath)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	var metadata storageMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return 0, false, errors.Trace(err)
	}
	return metadata.CheckpointTS, true, nil
}

func (loader *loader) finishScan() error {
	if err := loader.state.FinishIncrementalScan(loader.ctx, loader.highWatermark); err != nil {
		return errors.Trace(err)
	}
	loader.checkpointTs = loader.highWatermark
	return nil
}

func (loader *loader) scanVisibleFiles() ([]tableWork, error) {
	work := make([]tableWork, 0, len(loader.tables))
	for _, table := range loader.tables {
		dmlFileMap, err := loader.getNewFiles(table)
		if err != nil {
			metrics.AddCounter(metrics.ErrorCounter, 1, table.tableFQN)
			log.Error("incremental scan failed", zap.String("table", table.tableFQN), zap.Error(err))
			return nil, errors.Trace(err)
		}
		if len(dmlFileMap) == 0 {
			continue
		}
		work = append(work, tableWork{table: table, dmlRanges: dmlFileMap})
	}
	return work, nil
}

func (loader *loader) consumeVisibleFiles(work []tableWork) error {
	if len(work) == 0 {
		return nil
	}

	workerCount := min(tableConcurrency, len(work))
	tasks := make(chan tableWork, workerCount)
	g, ctx := errgroup.WithContext(loader.ctx)
	for range workerCount {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case task, ok := <-tasks:
					if !ok {
						return nil
					}
					if err := loader.handleNewFiles(task.table, task.dmlRanges); err != nil {
						metrics.AddCounter(metrics.ErrorCounter, 1, task.table.tableFQN)
						log.Error("incremental load failed", zap.String("table", task.table.tableFQN), zap.Error(err))
						return errors.Trace(err)
					}
				}
			}
		})
	}

	sendErr := func() error {
		defer close(tasks)
		for _, task := range work {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case tasks <- task:
			}
		}
		return nil
	}()
	waitErr := g.Wait()
	if waitErr != nil {
		return errors.Trace(waitErr)
	}
	return errors.Trace(sendErr)
}

func (loader *loader) parseSchemaFilePath(table *tableState, objectPath string) (err error) {
	filePath := loader.relativePath(objectPath)
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("parse schema file path %s: %v", filePath, r)
		}
	}()

	var schemaKey cloudstorage.SchemaPathKey
	schemaKey.Parse(filePath)
	if schemaKey.TableVersion > loader.highWatermark {
		return nil
	}
	if _, ok := table.tableDefMap[schemaKey.TableVersion]; ok {
		return nil
	}
	if schemaKey.Schema != table.sourceDatabase || schemaKey.Table != table.sourceTable {
		log.Error("schema file path not match",
			zap.String("table", table.tableFQN),
			zap.String("path", filePath))
		return nil
	}

	var tableDef cloudstorage.SchemaFile
	schemaContent, err := loader.storage.ReadFile(loader.ctx, objectPath)
	if err != nil {
		return errors.Trace(err)
	}
	if err = json.Unmarshal(schemaContent, &tableDef); err != nil {
		return errors.Trace(err)
	}
	if schemaKey.TableVersion != tableDef.TableVersion ||
		schemaKey.Schema != tableDef.Schema ||
		schemaKey.Table != tableDef.Table {
		log.Error("schema file metadata mismatch",
			zap.String("table", table.tableFQN),
			zap.Uint64("tableversionInMem", schemaKey.TableVersion),
			zap.Uint64("tableversionInFile", tableDef.TableVersion),
			zap.String("path", filePath))
		return errors.Errorf("schema file metadata mismatch")
	}

	table.tableDefMap[tableDef.TableVersion] = &tableDef
	dmlkey := cloudstorage.NewSchemaFileDMLPathKey(schemaKey)
	if _, ok := table.tableDMLIdxMap[dmlkey]; !ok {
		table.tableDMLIdxMap[dmlkey] = make(fileIndexKeyMap)
	} else {
		log.Panic("duplicate schema file found",
			zap.String("table", table.tableFQN),
			zap.String("path", filePath), zap.Any("tableDef", tableDef),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	return nil
}

// map1 - map2
func diffDMLMaps(
	map1, map2 map[cloudstorage.DMLPathKey]fileIndexKeyMap,
) map[cloudstorage.DMLPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DMLPathKey]fileIndexRange)
	for dmlKey, fileIndexMap := range map1 {
		origFileIndexMap, ok := map2[dmlKey]
		if !ok {
			resMap[dmlKey] = make(fileIndexRange)
			for fileIndexKey, idx := range fileIndexMap {
				resMap[dmlKey][fileIndexKey] = indexRange{
					start: 1,
					end:   idx,
				}
			}
			continue
		}
		for fileIndexKey, idx := range fileIndexMap {
			origIdx := origFileIndexMap[fileIndexKey]
			if idx > origIdx {
				if _, ok := resMap[dmlKey]; !ok {
					resMap[dmlKey] = make(fileIndexRange)
				}
				resMap[dmlKey][fileIndexKey] = indexRange{
					start: origIdx + 1,
					end:   idx,
				}
			}
		}
	}
	return resMap
}

// getNewFiles returns newly created dml files in specific ranges.
func (loader *loader) getNewFiles(table *tableState) (map[cloudstorage.DMLPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DMLPathKey]fileIndexRange)
	origDMLIdxMap := make(map[cloudstorage.DMLPathKey]fileIndexKeyMap, len(table.tableDMLIdxMap))
	for k, v := range table.tableDMLIdxMap {
		origDMLIdxMap[k] = make(fileIndexKeyMap, len(v))
		for fileIndexKey, idx := range v {
			origDMLIdxMap[k][fileIndexKey] = idx
		}
	}
	stats := incrementalScanStats{}
	opt := &storeapi.WalkOption{SubDir: loader.objectPath(path.Join(table.sourceDatabase, table.sourceTable))}
	err := loader.storage.WalkDir(loader.ctx, opt, func(objectPath string, _ int64) error {
		stats.objectFiles++
		if cloudstorage.IsSchemaFile(objectPath) {
			stats.schemaFiles++
			if err := loader.parseSchemaFilePath(table, objectPath); err != nil {
				return errors.Trace(err)
			}
			return nil
		}
		if strings.HasSuffix(objectPath, indexFileSuffix) {
			stats.indexFiles++
			pending, err := loader.parseDMLIndexFile(table, objectPath, origDMLIdxMap)
			if err != nil {
				return errors.Trace(err)
			}
			stats.pendingFiles += pending
			return nil
		}
		stats.ignoredFiles++
		return nil
	})
	if err != nil {
		return tableDMLMap, err
	}

	tableDMLMap = diffDMLMaps(table.tableDMLIdxMap, origDMLIdxMap)
	if len(tableDMLMap) > 0 || stats.pendingFiles > 0 {
		log.Info("increment storage scan completed",
			zap.String("table", table.tableFQN),
			zap.Int("newRanges", len(tableDMLMap)),
			zap.Int("objectFiles", stats.objectFiles),
			zap.Int("schemaFiles", stats.schemaFiles),
			zap.Int("indexFiles", stats.indexFiles),
			zap.Int("pendingFiles", stats.pendingFiles),
			zap.Int("ignoredFiles", stats.ignoredFiles))
	}
	return tableDMLMap, nil
}

func (loader *loader) parseDMLIndexFile(
	table *tableState,
	objectPath string,
	origDMLIdxMap map[cloudstorage.DMLPathKey]fileIndexKeyMap,
) (int, error) {
	filePath := loader.relativePath(objectPath)
	var dmlKey cloudstorage.DMLPathKey
	if err := dmlKey.ParseIndexFilePath(config.DateSeparatorDay.String(), filePath); err != nil {
		return 0, nil
	}
	if dmlKey.Schema != table.sourceDatabase || dmlKey.Table != table.sourceTable {
		return 0, nil
	}
	if dmlKey.TableVersion > loader.highWatermark {
		return 0, nil
	}

	data, err := loader.storage.ReadFile(loader.ctx, objectPath)
	if err != nil {
		return 0, errors.Trace(err)
	}
	fileName := strings.TrimSpace(string(data))
	fileIndex, err := cloudstorage.ParseFileIndexFromFileName(fileName, loader.fileExtension)
	if err != nil {
		return 0, errors.Trace(err)
	}
	expectedIndexPath := dmlKey.GenerateIndexFilePath(fileIndex.FileIndexKey)
	if expectedIndexPath != filePath {
		return 0, errors.Errorf("index path %s does not match index content %s", filePath, fileName)
	}
	if fileIndex.EnableTableAcrossNodes {
		return 0, errors.Errorf("table-across-nodes index files are not supported: %s", filePath)
	}
	if table.tableDMLIdxMap[dmlKey] == nil {
		table.tableDMLIdxMap[dmlKey] = make(fileIndexKeyMap)
	}
	if fileIndex.Idx > table.tableDMLIdxMap[dmlKey][fileIndex.FileIndexKey] {
		table.tableDMLIdxMap[dmlKey][fileIndex.FileIndexKey] = fileIndex.Idx
	}
	origIdx := uint64(0)
	if origDMLIdxMap[dmlKey] != nil {
		origIdx = origDMLIdxMap[dmlKey][fileIndex.FileIndexKey]
	}
	if fileIndex.Idx <= origIdx {
		return 0, nil
	}
	return int(fileIndex.Idx - origIdx), nil
}

func (table *tableState) tableDef(tableVersion uint64) cloudstorage.SchemaFile {
	if td, ok := table.tableDefMap[tableVersion]; ok {
		return *td
	}
	log.Panic("tableDef not found",
		zap.String("table", table.tableFQN),
		zap.Any("tableVersion", tableVersion),
		zap.Any("tableDefMap", table.tableDefMap))
	return cloudstorage.SchemaFile{}
}

func (loader *loader) syncExecDMLEvents(
	tbl *tableState,
	tableDef cloudstorage.SchemaFile,
	key cloudstorage.DMLPathKey,
	fileIndexKey cloudstorage.FileIndexKey,
	fileIdx uint64,
) (bool, error) {
	filePath := key.GenerateDMLFilePath(&cloudstorage.FileIndex{
		FileIndexKey: fileIndexKey,
		Idx:          fileIdx,
	}, loader.fileExtension, config.DefaultFileIndexWidth)
	objectPath := loader.objectPath(filePath)
	log.Info("loading DML file into data warehouse",
		zap.String("table", tbl.tableFQN),
		zap.String("filePath", objectPath),
		zap.Uint64("tableVersion", key.TableVersion),
		zap.Int64("partitionNum", key.PartitionNum),
		zap.String("date", key.Date),
		zap.String("dispatcherID", fileIndexKey.DispatcherID),
		zap.Uint64("fileIndex", fileIdx))
	fileConsumed, err := loader.conn.LoadIncrement(table.FromSchemaFile(tableDef), objectPath, loader.highWatermark)
	if err != nil {
		log.Error("failed to load DML file into data warehouse",
			zap.Error(err),
			zap.String("table", tbl.tableFQN),
			zap.String("filePath", objectPath),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.String("dispatcherID", fileIndexKey.DispatcherID),
			zap.Uint64("fileIndex", fileIdx))
		return false, errors.Trace(err)
	}
	if !fileConsumed {
		log.Info("DML file partially loaded",
			zap.String("table", tbl.tableFQN),
			zap.String("filePath", objectPath),
			zap.Uint64("highWatermark", loader.highWatermark),
			zap.Uint64("fileIndex", fileIdx))
		return false, nil
	}
	scopeKey := dmlScopeKey(key, fileIndexKey)
	if err := loader.state.SetDMLFileWatermark(loader.ctx, tbl.tableFQN, scopeKey, fileIdx); err != nil {
		return false, errors.Trace(err)
	}

	log.Info("DML file loaded",
		zap.String("table", tbl.tableFQN),
		zap.String("filePath", objectPath),
		zap.Uint64("tableVersion", key.TableVersion),
		zap.Int64("partitionNum", key.PartitionNum),
		zap.String("date", key.Date),
		zap.String("dispatcherID", fileIndexKey.DispatcherID),
		zap.Uint64("fileIndex", fileIdx))

	return true, nil
}

func (loader *loader) syncExecDDLEvents(tbl *tableState, tableDef cloudstorage.SchemaFile) error {
	nextMeta := table.FromSchemaFile(tableDef)
	if len(nextMeta.PrimaryKeys) == 0 {
		return errors.Errorf("table %s has no primary key in schema file table version %d", tbl.tableFQN, tableDef.TableVersion)
	}
	if len(tableDef.Query) == 0 {
		log.Info("initializing table schema from schema file",
			zap.String("table", tbl.tableFQN),
			zap.Uint64("tableVersion", tableDef.TableVersion),
			zap.Int("columnCount", len(nextMeta.Columns)))
		tbl.currentMeta = nextMeta
		return nil
	}
	if tableDef.TableVersion <= tbl.ddlTableVersionWatermark {
		tbl.currentMeta = nextMeta
		log.Info("skip applied DDL",
			zap.String("table", tbl.tableFQN),
			zap.Uint64("tableVersion", tableDef.TableVersion),
			zap.Uint64("ddlTableVersionWatermark", tbl.ddlTableVersionWatermark))
		return nil
	}

	ddls, err := snowflake.GenDDLViaMetaDiff(tbl.currentMeta, nextMeta, model.ActionType(tableDef.Type))
	if err != nil {
		return errors.Trace(err)
	}
	if len(ddls) == 0 {
		log.Info("no Snowflake DDL needed",
			zap.String("table", tbl.tableFQN),
			zap.String("ddl", tableDef.Query),
			zap.Uint64("tableVersion", tableDef.TableVersion))
	} else {
		log.Info("executing DDL from schema file",
			zap.String("table", tbl.tableFQN),
			zap.Uint64("tableVersion", tableDef.TableVersion),
			zap.String("query", tableDef.Query),
			zap.Int("columnCount", len(tableDef.Columns)))
		for _, ddl := range ddls {
			if err := loader.conn.ExecDDL(ddl); err != nil {
				return errors.Annotate(err,
					fmt.Sprintf("Please check the DDL query, "+
						"if necessary, please manually execute the DDL query in data warehouse, "+
						"update the ddl_table_version_watermark of %s in state if the DDL has been manually applied, "+
						"and restart the program",
						tbl.tableFQN))
			}
		}
	}
	tbl.currentMeta = nextMeta
	if err := loader.state.SetDDLTableVersionWatermark(loader.ctx, tbl.tableFQN, tableDef.TableVersion); err != nil {
		return errors.Trace(err)
	}
	tbl.ddlTableVersionWatermark = tableDef.TableVersion
	metrics.AddCounter(metrics.TableVersionsCounter, float64(tableDef.TableVersion), fmt.Sprintf("%s/%s", tbl.sourceDatabase, tbl.sourceTable))

	log.Info("DDL marked as applied in state",
		zap.String("table", tbl.tableFQN),
		zap.Uint64("tableVersion", tableDef.TableVersion))
	return nil
}

func (loader *loader) handleNewFiles(table *tableState, dmlFileMap map[cloudstorage.DMLPathKey]fileIndexRange) error {
	keys := make([]cloudstorage.DMLPathKey, 0, len(dmlFileMap))
	for k := range dmlFileMap {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil
	}
	slices.SortStableFunc(keys, func(x, y cloudstorage.DMLPathKey) int {
		return cloudstorage.CompareDMLPathKey(x, y)
	})
	log.Info("new increment ranges found",
		zap.String("table", table.tableFQN),
		zap.Int("rangeCount", len(keys)),
		zap.Uint64("fileCount", countFilesInRanges(dmlFileMap)))

	for _, key := range keys {
		tableDef := table.tableDef(key.SchemaPathKey.TableVersion)
		if key.IsSchemaFileDMLPathKey() {
			if err := loader.syncExecDDLEvents(table, tableDef); err != nil {
				return errors.Trace(err)
			}
			continue
		}

		fileRanges := dmlFileMap[key]
		fileIndexKeys := make([]cloudstorage.FileIndexKey, 0, len(fileRanges))
		for fileIndexKey := range fileRanges {
			fileIndexKeys = append(fileIndexKeys, fileIndexKey)
		}
		slices.SortStableFunc(fileIndexKeys, func(x, y cloudstorage.FileIndexKey) int {
			if x.DispatcherID < y.DispatcherID {
				return -1
			}
			if x.DispatcherID > y.DispatcherID {
				return 1
			}
			if !x.EnableTableAcrossNodes && y.EnableTableAcrossNodes {
				return -1
			}
			if x.EnableTableAcrossNodes && !y.EnableTableAcrossNodes {
				return 1
			}
			return 0
		})
		log.Info("processing increment range",
			zap.String("table", table.tableFQN),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partitionNum", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Int("indexScopeCount", len(fileRanges)))
		for _, fileIndexKey := range fileIndexKeys {
			fileRange := fileRanges[fileIndexKey]
			log.Info("processing increment index scope",
				zap.String("table", table.tableFQN),
				zap.String("dispatcherID", fileIndexKey.DispatcherID),
				zap.Uint64("startFileIndex", fileRange.start),
				zap.Uint64("endFileIndex", fileRange.end))
			for i := fileRange.start; i <= fileRange.end; i++ {
				fileConsumed, err := loader.syncExecDMLEvents(table, tableDef, key, fileIndexKey, i)
				if err != nil {
					return errors.Trace(err)
				}
				if !fileConsumed {
					return nil
				}
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
		for _, idxRange := range fileRange {
			if idxRange.end >= idxRange.start {
				count += idxRange.end - idxRange.start + 1
			}
		}
	}
	return count
}
