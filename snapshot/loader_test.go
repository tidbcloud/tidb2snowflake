package snapshot

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/workerpool"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestCreateTablesRejectsTableWithoutPrimaryKey(t *testing.T) {
	ctx := context.Background()
	storageURI, store := newTestSnapshotStore(t)
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t1"), "CREATE TABLE t1 (id bigint);")

	cfg := Config{
		Tables:     []string{"db.t1"},
		StorageURI: storageURI,
		StorageDir: "snapshot",
	}
	pool := newStartedPool(ctx, t)
	err := prepareTables(ctx, cfg, store, nil, pool)

	require.Error(t, err)
	require.Contains(t, err.Error(), "has no primary key")
}

func TestSnapshotTaskForFile(t *testing.T) {
	task, ok := snapshotTaskForFile("snapshot/db.t.000001.csv.gz")
	require.True(t, ok)
	require.Equal(t, loadTask{
		targetTable: "db.t",
		filePath:    "snapshot/db.t.000001.csv.gz",
	}, task)

	_, ok = snapshotTaskForFile("snapshot/notes.csv")
	require.False(t, ok)
}

func TestLoadFilesSkipsUnconfiguredTables(t *testing.T) {
	ctx := context.Background()
	storageURI, store := newTestSnapshotStore(t)
	writeSnapshotObject(t, ctx, store, "snapshot/db.skip.000001.csv", "ignored")

	cfg := Config{
		Tables:     []string{"db.keep"},
		StorageURI: storageURI,
		StorageDir: "snapshot",
	}
	pool := newStartedPool(ctx, t)

	require.NoError(t, loadFiles(ctx, cfg, store, nil, pool))
}

func newStartedPool(ctx context.Context, t *testing.T) *workerpool.Pool {
	t.Helper()
	pool := workerpool.New(workerpool.DefaultConcurrency)
	pool.Go(ctx)
	t.Cleanup(pool.Close)
	return pool
}

func newTestSnapshotStore(t *testing.T) (*url.URL, *storage.Storage) {
	t.Helper()
	uri := &url.URL{Scheme: "file", Path: t.TempDir()}
	store, err := storage.New(context.Background(), uri)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	return uri, store
}

func writeSnapshotObject(t *testing.T, ctx context.Context, store *storage.Storage, name, data string) {
	t.Helper()
	require.NoError(t, store.WriteFile(ctx, name, []byte(data)))
}
