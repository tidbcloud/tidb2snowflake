package incremental

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"testing"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestCountFilesInRangesSkipsSchemaKeys(t *testing.T) {
	schemaKey := cloudstorage.SchemaPathKey{
		Schema:       "db",
		Table:        "tbl",
		TableVersion: 42,
	}
	dmlKey := cloudstorage.DMLPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  7,
		Date:          "2026-06-19",
	}
	got := countFilesInRanges(map[cloudstorage.DMLPathKey]indexRange{
		cloudstorage.NewSchemaFileDMLPathKey(schemaKey): {start: 1, end: 1},
		dmlKey: {start: 2, end: 4},
	})

	require.Equal(t, uint64(3), got)
}

func TestDDLExecutionErrorMessageIncludesGeneratedDDLs(t *testing.T) {
	ddls := []string{
		`ALTER TABLE "db.t" RENAME COLUMN "a" TO "b";`,
		`ALTER TABLE "db.t" MODIFY COLUMN "b" VARCHAR(32);`,
	}

	msg := ddlExecutionErrorMessage("db.t", 1, ddls)

	require.Contains(t, msg, "execute DDL 2/2 for db.t failed")
	require.Contains(t, msg, `failed DDL:
ALTER TABLE "db.t" MODIFY COLUMN "b" VARCHAR(32);`)
	require.Contains(t, msg, `generated DDLs:
ALTER TABLE "db.t" RENAME COLUMN "a" TO "b";
ALTER TABLE "db.t" MODIFY COLUMN "b" VARCHAR(32);`)
	require.Contains(t, msg, "ddl_table_version_watermark of db.t")
}

func TestGetNewFilesScansMetadataPrefixes(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 100)
	writeSchemaFile(t, ctx, store, "inc", 120)
	writeIndexFile(t, ctx, store, "inc", 100, "2026-06-29", 3)
	writeIndexFile(t, ctx, store, "inc", 120, "2026-06-29", 2)
	require.NoError(t, store.WriteFile(ctx, "inc/db/t/100/2026-06-29/CDC000000000000003.csv", []byte("ignored")))

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap: map[cloudstorage.DMLPathKey]uint64{
			{
				SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
				Date:          "2026-06-29",
			}: 1,
		},
		ddlTableVersionWatermark: 100,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}

	scan, err := loader.getNewFiles(ctx, 110, table)
	require.NoError(t, err)

	require.Equal(t, indexRange{start: 2, end: 3}, scan.dmlFileMap[cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
		Date:          "2026-06-29",
	}])
	require.Equal(t, indexRange{start: 1, end: 2}, scan.dmlFileMap[cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 120},
		Date:          "2026-06-29",
	}])
	require.Contains(t, scan.dmlFileMap, cloudstorage.NewSchemaFileDMLPathKey(cloudstorage.SchemaPathKey{
		Schema:       "db",
		Table:        "t",
		TableVersion: 120,
	}))
}

func TestBeginScanOnlyScansWhenTargetCheckpointAdvances(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	stateManager, err := state.Open(ctx, store, []string{"db.t"}, true)
	require.NoError(t, err)
	require.NoError(t, stateManager.SetCheckpointTS(ctx, 100))

	loader := &loader{
		storage:    store,
		storageDir: "inc",
		state:      stateManager,
	}

	require.NoError(t, writeIncrementMetadata(ctx, store, "inc", 100))
	bounds, hasScan, err := loader.beginScan(ctx)
	require.NoError(t, err)
	require.False(t, hasScan)
	require.Equal(t, scanBounds{checkpointTs: 100, targetCheckpointTs: 100}, bounds)

	require.NoError(t, writeIncrementMetadata(ctx, store, "inc", 99))
	bounds, hasScan, err = loader.beginScan(ctx)
	require.NoError(t, err)
	require.False(t, hasScan)
	require.Equal(t, scanBounds{checkpointTs: 100, targetCheckpointTs: 99}, bounds)

	require.NoError(t, writeIncrementMetadata(ctx, store, "inc", 101))
	bounds, hasScan, err = loader.beginScan(ctx)
	require.NoError(t, err)
	require.True(t, hasScan)
	require.Equal(t, scanBounds{checkpointTs: 100, targetCheckpointTs: 101}, bounds)
}

func TestGetNewFilesDoesNotScheduleSchemaAtCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 100)

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]uint64),
		ddlTableVersionWatermark: 0,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}

	scan, err := loader.getNewFiles(ctx, 100, table)
	require.NoError(t, err)

	require.Contains(t, scan.schemaFilePaths, uint64(100))
	require.NotContains(t, scan.dmlFileMap, cloudstorage.NewSchemaFileDMLPathKey(cloudstorage.SchemaPathKey{
		Schema:       "db",
		Table:        "t",
		TableVersion: 100,
	}))
}

func TestGetNewFilesKeepsOnlyLatestAppliedBaselineSchema(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 80)
	writeSchemaFile(t, ctx, store, "inc", 90)
	writeSchemaFile(t, ctx, store, "inc", 100)

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]uint64),
		ddlTableVersionWatermark: 100,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}

	scan, err := loader.getNewFiles(ctx, 100, table)
	require.NoError(t, err)

	require.NotContains(t, scan.schemaFilePaths, uint64(80))
	require.NotContains(t, scan.schemaFilePaths, uint64(90))
	require.Contains(t, scan.schemaFilePaths, uint64(100))
}

func TestGetNewFilesScansVisibleWorkBeyondTargetCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 100)
	writeSchemaFile(t, ctx, store, "inc", 150)
	writeIndexFile(t, ctx, store, "inc", 150, "2026-07-02", 2)

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]uint64),
		ddlTableVersionWatermark: 100,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}

	scan, err := loader.getNewFiles(ctx, 110, table)
	require.NoError(t, err)

	require.Contains(t, scan.dmlFileMap, cloudstorage.NewSchemaFileDMLPathKey(cloudstorage.SchemaPathKey{
		Schema:       "db",
		Table:        "t",
		TableVersion: 150,
	}))
	require.Equal(t, indexRange{start: 1, end: 2}, scan.dmlFileMap[cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 150},
		Date:          "2026-07-02",
	}])
}

func TestGetNewFilesDoesNotRescheduleAppliedDDLAfterCheckpointLag(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 120)
	writeIndexFile(t, ctx, store, "inc", 120, "2026-07-02", 1)

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]uint64),
		ddlTableVersionWatermark: 120,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}

	scan, err := loader.getNewFiles(ctx, 100, table)
	require.NoError(t, err)

	require.NotContains(t, scan.dmlFileMap, cloudstorage.NewSchemaFileDMLPathKey(cloudstorage.SchemaPathKey{
		Schema:       "db",
		Table:        "t",
		TableVersion: 120,
	}))
	require.Equal(t, indexRange{start: 1, end: 1}, scan.dmlFileMap[cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 120},
		Date:          "2026-07-02",
	}])
}

func TestGetNewFilesSkipsDatesBeforeActiveDate(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 100)
	writeIndexFile(t, ctx, store, "inc", 100, "2026-06-29", 5)
	writeIndexFile(t, ctx, store, "inc", 100, "2026-06-30", 2)

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap: map[cloudstorage.DMLPathKey]uint64{
			{
				SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
				Date:          "2026-06-29",
			}: 1,
			{
				SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
				Date:          "2026-06-30",
			}: 1,
		},
		activeDateByDMLScope: map[dmlScope]string{
			{tableVersion: 100}: "2026-06-30",
		},
		ddlTableVersionWatermark: 100,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}

	scan, err := loader.getNewFiles(ctx, 100, table)
	require.NoError(t, err)

	require.NotContains(t, scan.dmlFileMap, cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
		Date:          "2026-06-29",
	})
	require.Equal(t, indexRange{start: 2, end: 2}, scan.dmlFileMap[cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
		Date:          "2026-06-30",
	}])
}

func TestGetNewFilesScansPartitionDateDirs(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 100)
	writePartitionIndexFile(t, ctx, store, "inc", 100, 55, "2026-06-29", 5)
	writePartitionIndexFile(t, ctx, store, "inc", 100, 55, "2026-06-30", 2)

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap: map[cloudstorage.DMLPathKey]uint64{
			{
				SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
				PartitionNum:  55,
				Date:          "2026-06-29",
			}: 5,
			{
				SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
				PartitionNum:  55,
				Date:          "2026-06-30",
			}: 1,
		},
		activeDateByDMLScope: map[dmlScope]string{
			{tableVersion: 100, partitionNum: 55}: "2026-06-30",
		},
		ddlTableVersionWatermark: 100,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}

	scan, err := loader.getNewFiles(ctx, 100, table)
	require.NoError(t, err)

	require.NotContains(t, scan.dmlFileMap, cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
		PartitionNum:  55,
		Date:          "2026-06-29",
	})
	require.Equal(t, indexRange{start: 2, end: 2}, scan.dmlFileMap[cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "t", TableVersion: 100},
		PartitionNum:  55,
		Date:          "2026-06-30",
	}])
}

func writeSchemaFile(t *testing.T, ctx context.Context, store *storage.Storage, storageDir string, tableVersion uint64) {
	t.Helper()
	writeSchemaFileForTable(t, ctx, store, storageDir, "db", "t", tableVersion, "", 0)
}

func writeIncrementMetadata(ctx context.Context, store *storage.Storage, storageDir string, checkpointTs uint64) error {
	return store.WriteFile(ctx, path.Join(storageDir, metadataFileName), []byte(fmt.Sprintf(`{"checkpoint-ts":%d}`, checkpointTs)))
}

func writeSchemaFileForTable(
	t *testing.T,
	ctx context.Context,
	store *storage.Storage,
	storageDir string,
	schema string,
	table string,
	tableVersion uint64,
	query string,
	action model.ActionType,
) string {
	t.Helper()
	schemaFile := cloudstorage.SchemaFile{
		Schema:       schema,
		Table:        table,
		TableVersion: tableVersion,
		Query:        query,
		Type:         byte(action),
		TotalColumns: 1,
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "INT", Nullable: "false", IsPK: "true"},
		},
	}
	objectPath := path.Join(storageDir, schemaFile.Path(false, 0))
	require.NoError(t, store.WriteFile(ctx, objectPath, schemaFile.Marshal()))
	return objectPath
}

func writeIndexFile(t *testing.T, ctx context.Context, store *storage.Storage, storageDir string, tableVersion uint64, date string, fileIndex uint64) {
	t.Helper()
	dmlKey := cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       "db",
			Table:        "t",
			TableVersion: tableVersion,
		},
		Date: date,
	}
	fileName := path.Base(dmlKey.GenerateDMLFilePath(
		&cloudstorage.FileIndex{Idx: fileIndex},
		storage.CSVFileExtension,
		config.DefaultFileIndexWidth,
	))
	require.NoError(t, store.WriteFile(ctx, path.Join(storageDir, dmlKey.GenerateIndexFilePath(cloudstorage.FileIndexKey{})), []byte(fileName)))
}

func writePartitionIndexFile(
	t *testing.T,
	ctx context.Context,
	store *storage.Storage,
	storageDir string,
	tableVersion uint64,
	partitionNum int64,
	date string,
	fileIndex uint64,
) {
	t.Helper()
	dmlKey := cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       "db",
			Table:        "t",
			TableVersion: tableVersion,
		},
		PartitionNum: partitionNum,
		Date:         date,
	}
	fileName := path.Base(dmlKey.GenerateDMLFilePath(
		&cloudstorage.FileIndex{Idx: fileIndex},
		storage.CSVFileExtension,
		config.DefaultFileIndexWidth,
	))
	require.NoError(t, store.WriteFile(ctx, path.Join(storageDir, dmlKey.GenerateIndexFilePath(cloudstorage.FileIndexKey{})), []byte(fileName)))
}
