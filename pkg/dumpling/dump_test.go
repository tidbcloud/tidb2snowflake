package dumpling

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestBuildConfigSnowflakeSnapshotDump(t *testing.T) {
	storageURI, err := url.Parse("local:///tmp/tidb2snowflake/snapshot")
	require.NoError(t, err)

	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
	require.NoError(t, err)

	cfg, err := buildConfig(store, &tidb.Config{
		Host: "127.0.0.1",
		Port: 4000,
		User: "root",
		Pass: "secret",
	}, Config{
		Concurrency:  8,
		StorageURI:   storageURI,
		SnapshotTSO:  "449023000000000000",
		Tables:       []string{"db1.t1", "db2.t2"},
		Compression:  "gzip",
		ReadTimeout:  15 * time.Second,
		CSVNullValue: "\\N",
	})
	require.NoError(t, err)

	require.Equal(t, "127.0.0.1", cfg.Host)
	require.Equal(t, 4000, cfg.Port)
	require.Equal(t, "root", cfg.User)
	require.Equal(t, "secret", cfg.Password)
	require.Equal(t, 8, cfg.Threads)
	require.Equal(t, "csv", cfg.FileType)
	require.True(t, cfg.NoHeader)
	require.Equal(t, ",", cfg.CsvSeparator)
	require.Equal(t, "\"", cfg.CsvDelimiter)
	require.Equal(t, "\\N", cfg.CsvNullValue)
	require.False(t, cfg.EscapeBackslash)
	require.Equal(t, export.CSVDialectSnowflake, cfg.CsvOutputDialect)
	require.True(t, cfg.TransactionalConsistency)
	require.Equal(t, "449023000000000000", cfg.Snapshot)
	require.Equal(t, uint64(export.UnspecifiedSize), cfg.FileSize)
	require.True(t, cfg.SpecifiedTables)
	require.Equal(t, storageURI.String(), cfg.OutputDirPath)
	require.NotNil(t, cfg.ExtStorage)
}

func TestLoadTSOFromMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/metadata", []byte(`Started dump at: 2026-06-11 10:35:45
SHOW MASTER STATUS:
	Log: tidb-binlog
	Pos: 466924115091783691
	GTID:

Finished dump at: 2026-06-11 10:35:48
`)))

	tso, err := LoadTSOFromMetadata(ctx, store)
	require.NoError(t, err)
	require.Equal(t, uint64(466924115091783691), tso)
}

func TestLoadTSOFromMetadataRequiresMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/db1.t1.000001.csv", []byte("1\n")))

	_, err = LoadTSOFromMetadata(ctx, store)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot/metadata is missing")
}

func TestTSOFromMetadata(t *testing.T) {
	tso := tsoFromMetadata([]byte("Started dump at: x\n\tPos: 466924115091783691\n"))
	require.Equal(t, "466924115091783691", tso)
}
