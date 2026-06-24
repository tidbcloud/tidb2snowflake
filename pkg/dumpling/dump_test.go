package dumpling

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tidb/dumpling/export"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
)

func TestBuildConfig_SnowflakeSnapshotDump(t *testing.T) {
	storageURI, err := url.Parse("local:///tmp/tidb2snowflake/snapshot")
	require.NoError(t, err)

	cfg, err := BuildConfig(context.Background(), &tidb.Config{
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
		FileSize:     "5GiB",
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
	require.True(t, cfg.SpecifiedTables)
	require.Equal(t, storageURI.String(), cfg.OutputDirPath)
	require.NotNil(t, cfg.ExtStorage)
}
