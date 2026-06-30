package snapshot

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestLoadCreatesTablesAndLoadsSnapshotFiles(t *testing.T) {
	ctx := context.Background()
	storageURI, store := newTestSnapshotStore(t)
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t1"), "CREATE TABLE t1 (id bigint primary key, name varchar(64));")
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t2"), "CREATE TABLE t2 (id bigint primary key);")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t1.000001.csv", "1,a\n")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t1.000002.csv.gz", "2,b\n")
	writeSnapshotObject(t, ctx, store, "snapshot/db.t2.000001.csv", "3\n")
	writeSnapshotObject(t, ctx, store, "snapshot/notes.csv", "ignored\n")

	cfg := Config{
		Tables:     []string{"db.t1", "db.t2"},
		StorageURI: storageURI,
		StorageDir: "snapshot",
	}
	conn := &fakeSnapshotConnector{}
	require.NoError(t, createTables(ctx, cfg, store, conn))
	err := loadFiles(ctx, cfg, store, conn)

	require.NoError(t, err)
	require.Equal(t, 2, conn.schemaCount())
	require.ElementsMatch(t, []string{"db.t1", "db.t2"}, conn.createdTables())
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
	require.NoError(t, createTables(ctx, cfg, store, conn))
	err := loadFiles(ctx, cfg, store, conn)

	require.Error(t, err)
	require.Contains(t, err.Error(), loadErr.Error())
}

func TestCreateTablesRejectsTableWithoutPrimaryKey(t *testing.T) {
	ctx := context.Background()
	storageURI, store := newTestSnapshotStore(t)
	writeSnapshotObject(t, ctx, store, "snapshot/"+table.SchemaFilePath("db", "t1"), "CREATE TABLE t1 (id bigint);")

	cfg := Config{
		Tables:     []string{"db.t1"},
		StorageURI: storageURI,
		StorageDir: "snapshot",
	}
	err := createTables(ctx, cfg, store, &fakeSnapshotConnector{})

	require.Error(t, err)
	require.Contains(t, err.Error(), "has no primary key")
}

type fakeSnapshotConnector struct {
	mu      sync.Mutex
	schemas int
	created []string
	loaded  []string
	loadErr error
}

func (c *fakeSnapshotConnector) CreateTable(schema *table.Meta) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas++
	c.created = append(c.created, schema.SnowflakeTableName())
	return nil
}

func (c *fakeSnapshotConnector) LoadSnapshot(targetTable, filePath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == 0 {
		return errors.New("table was not created before snapshot data")
	}
	if !contains(c.created, targetTable) {
		return errors.New("target table was not created")
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

func (c *fakeSnapshotConnector) createdTables() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	tables := make([]string, len(c.created))
	copy(tables, c.created)
	return tables
}

func (c *fakeSnapshotConnector) loadedFiles() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	files := make([]string, len(c.loaded))
	copy(files, c.loaded)
	return files
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
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
