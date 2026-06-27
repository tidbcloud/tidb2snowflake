package snapshot

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
)

func TestLoadCopiesSchemasAndSnapshotFiles(t *testing.T) {
	ctx := context.Background()
	storageURI, store := newTestSnapshotStore(t)
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t1"), "CREATE TABLE t1 (id bigint primary key, name varchar(64));")
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t2"), "CREATE TABLE t2 (id bigint primary key);")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t1.000001.csv", "1,a\n")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t1.000002.csv.gz", "2,b\n")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t2.000001.csv", "3\n")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t3.000001.csv", "4\n")
	writeSnapshotObject(t, ctx, store, "snapshot/notes.csv", "ignored\n")

	cfg := Config{
		Tables:     []string{"db.t1", "db.t2"},
		StorageURI: storageURI,
		StorageDir: "snapshot",
	}
	conn := &fakeSnapshotConnector{}
	tables, err := prepareSnapshotTables(ctx, cfg, store, conn)
	require.NoError(t, err)
	err = loadSnapshotFiles(ctx, cfg, store, tables, conn)

	require.NoError(t, err)
	require.Equal(t, 2, conn.schemaCount())
	require.ElementsMatch(t, []string{"t1", "t2"}, conn.schemaTables())
	require.ElementsMatch(t, []string{
		"db.t1:snapshot/db.t1.000001.csv",
		"db.t1:snapshot/db.t1.000002.csv.gz",
		"db.t2:snapshot/db.t2.000001.csv",
	}, conn.loadedFiles())
}

func TestLoadReturnsSnapshotFileError(t *testing.T) {
	ctx := context.Background()
	storageURI, store := newTestSnapshotStore(t)
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t1"), "CREATE TABLE t1 (id bigint primary key);")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t1.000001.csv", "1\n")

	loadErr := errors.New("copy failed")
	cfg := Config{
		Tables:     []string{"db.t1"},
		StorageURI: storageURI,
		StorageDir: "snapshot",
	}
	conn := &fakeSnapshotConnector{loadErr: loadErr}
	tables, err := prepareSnapshotTables(ctx, cfg, store, conn)
	require.NoError(t, err)
	err = loadSnapshotFiles(ctx, cfg, store, tables, conn)

	require.Error(t, err)
	require.Contains(t, err.Error(), loadErr.Error())
}

func TestPrepareSnapshotTablesRejectsTableWithoutPrimaryKey(t *testing.T) {
	ctx := context.Background()
	storageURI, store := newTestSnapshotStore(t)
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t1"), "CREATE TABLE t1 (id bigint);")

	cfg := Config{
		Tables:     []string{"db.t1"},
		StorageURI: storageURI,
		StorageDir: "snapshot",
	}
	_, err := prepareSnapshotTables(ctx, cfg, store, &fakeSnapshotConnector{})

	require.Error(t, err)
	require.Contains(t, err.Error(), "has no primary key")
}

type fakeSnapshotConnector struct {
	mu           sync.Mutex
	schemas      int
	copiedTables []string
	loaded       []string
	loadErr      error
}

func (c *fakeSnapshotConnector) CopyTableSchema(schema *table.Meta) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas++
	c.copiedTables = append(c.copiedTables, schema.Table)
	return nil
}

func (c *fakeSnapshotConnector) LoadSnapshot(targetTable, filePath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == 0 {
		return errors.New("schema was not copied before snapshot data")
	}
	if c.loadErr != nil {
		return c.loadErr
	}
	c.loaded = append(c.loaded, targetTable+":"+filePath)
	return nil
}

func (c *fakeSnapshotConnector) schemaCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.schemas
}

func (c *fakeSnapshotConnector) schemaTables() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	tables := make([]string, len(c.copiedTables))
	copy(tables, c.copiedTables)
	return tables
}

func (c *fakeSnapshotConnector) loadedFiles() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	files := make([]string, len(c.loaded))
	copy(files, c.loaded)
	return files
}

func newTestSnapshotStore(t *testing.T) (*url.URL, storeapi.Storage) {
	t.Helper()
	uri := &url.URL{Scheme: "file", Path: t.TempDir()}
	store, err := util.GetExternalStorageWithDefaultTimeout(context.Background(), uri.String())
	require.NoError(t, err)
	t.Cleanup(store.Close)
	return uri, store
}

func writeSnapshotObject(t *testing.T, ctx context.Context, store storeapi.Storage, name, data string) {
	t.Helper()
	require.NoError(t, store.WriteFile(ctx, name, []byte(data)))
}
