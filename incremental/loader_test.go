package incremental

import (
	"context"
	"net/url"
	"path"
	"testing"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestDiffDMLMaps(t *testing.T) {
	key := cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       "db",
			Table:        "tbl",
			TableVersion: 42,
		},
		PartitionNum: 7,
		Date:         "2026-06-19",
	}
	got := diffDMLMaps(
		map[cloudstorage.DMLPathKey]uint64{key: 9},
		map[cloudstorage.DMLPathKey]uint64{key: 6},
	)

	require.Equal(t, indexRange{start: 7, end: 9}, got[key])
}

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

	scan, err := loader.getNewFiles(ctx, 110, 130, table, nil)
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

func TestGetNewFilesScansRenameSchemaFile(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	writeSchemaFile(t, ctx, store, "inc", 100)
	renameSchemaPath := writeSchemaFileForTable(t, ctx, store, "inc", "db", "tt", 120,
		"RENAME TABLE `t` TO `tt`", model.ActionRenameTable)

	loader := &loader{storage: store, storageDir: "inc"}
	table := &tableState{
		tableDMLIdxMap:           make(map[cloudstorage.DMLPathKey]uint64),
		ddlTableVersionWatermark: 100,
		sourceDatabase:           "db",
		sourceTable:              "t",
		tableFQN:                 "db.t",
	}
	loader.tables = []*tableState{table}

	renameSchemaFilePaths, err := loader.findRenameSchemaFilePaths(ctx, 130)
	require.NoError(t, err)
	scan, err := loader.getNewFiles(ctx, 110, 130, table, renameSchemaFilePaths[table.tableFQN])
	require.NoError(t, err)

	require.Equal(t, renameSchemaPath, scan.schemaFilePaths[120])
	require.Contains(t, scan.dmlFileMap, cloudstorage.NewSchemaFileDMLPathKey(cloudstorage.SchemaPathKey{
		Schema:       "db",
		Table:        "t",
		TableVersion: 120,
	}))
}

func writeSchemaFile(t *testing.T, ctx context.Context, store *storage.Storage, storageDir string, tableVersion uint64) {
	t.Helper()
	writeSchemaFileForTable(t, ctx, store, storageDir, "db", "t", tableVersion, "", 0)
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
