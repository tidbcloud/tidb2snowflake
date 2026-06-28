package cmd

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
)

func baseOption() *Option {
	opt := NewOption()
	opt.StoragePath = "s3://bucket/path"
	opt.AWSAccessKey = "AKIA"
	opt.AWSSecretKey = "secret"
	opt.Tables = []string{"db1.t1", "db2.t2"}
	return opt
}

func TestNewOptionDefaults(t *testing.T) {
	opt := NewOption()

	require.Equal(t, "127.0.0.1", opt.TiDBHost)
	require.Equal(t, 4000, opt.TiDBPort)
	require.Equal(t, "root", opt.TiDBUser)
	require.Equal(t, 8, opt.SnapshotConcurrency)
	require.Equal(t, "COMPUTE_WH", opt.SnowflakeWarehouse)
	require.Equal(t, 60*time.Second, opt.ChangefeedFlushInterval)
	require.Equal(t, 64, opt.ChangefeedFileSizeMiB)
	require.Equal(t, snapshotCompressionNone, opt.SnapshotCompression)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)
	require.Equal(t, runModeFull, opt.Mode)
	require.Empty(t, opt.SnapshotTSO)
}

func TestValidateConfig(t *testing.T) {
	opt := &Option{
		AWSAccessKey:            "AKIA",
		AWSSecretKey:            "secret",
		SnowflakeDatabase:       "SNOW",
		SnowflakeSchema:         "PUBLIC",
		Tables:                  []string{"db1.t1"},
		Mode:                    "SNAPSHOT-ONLY",
		SourceMode:              "OP",
		SnapshotCompression:     "GZIP",
		ChangefeedFlushInterval: -1,
		ChangefeedFileSizeMiB:   -1,
		SnapshotConcurrency:     -1,
	}
	err := opt.validate()
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", opt.TiDBHost)
	require.Equal(t, 4000, opt.TiDBPort)
	require.Equal(t, "root", opt.TiDBUser)
	require.Equal(t, "COMPUTE_WH", opt.SnowflakeWarehouse)
	require.Equal(t, 60*time.Second, opt.ChangefeedFlushInterval)
	require.Equal(t, 64, opt.ChangefeedFileSizeMiB)
	require.Equal(t, runModeSnapshotOnly, opt.Mode)
	require.Equal(t, sourceModeOP, opt.SourceMode)
	require.Equal(t, snapshotCompressionGzip, opt.SnapshotCompression)
	require.Equal(t, 8, opt.SnapshotConcurrency)

	opt = &Option{
		SourceMode:          sourceModeTiDBCloud,
		SnapshotConcurrency: -1,
		AWSAccessKey:        "AKIA",
		AWSSecretKey:        "secret",
		SnowflakeDatabase:   "SNOW",
		SnowflakeSchema:     "PUBLIC",
		Tables:              []string{"db1.t1"},
	}
	err = opt.validate()
	require.NoError(t, err)
	require.Equal(t, runModeFull, opt.Mode)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)
	require.Equal(t, snapshotCompressionNone, opt.SnapshotCompression)
	require.Equal(t, -1, opt.SnapshotConcurrency)

	opt = &Option{
		Mode:                "unknown",
		SourceMode:          "unknown",
		SnapshotCompression: "unknown",
		AWSAccessKey:        "AKIA",
		AWSSecretKey:        "secret",
		SnowflakeDatabase:   "SNOW",
		SnowflakeSchema:     "PUBLIC",
		Tables:              []string{"db1.t1"},
	}
	err = opt.validate()
	require.NoError(t, err)
	require.Equal(t, runModeFull, opt.Mode)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)
	require.Equal(t, snapshotCompressionNone, opt.SnapshotCompression)
	require.Equal(t, 0, opt.SnapshotConcurrency)

	opt = validationOption()
	err = opt.validate()
	require.NoError(t, err)
	require.Equal(t, runModeFull, opt.Mode)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)

	opt = validationOption()
	opt.SnapshotTSO = "0"
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--snapshot-tso must be greater than 0")

	opt = validationOption()
	opt.SourceMode = sourceModeOP
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--ticdc.address is required when --source.mode=op")

	opt = validationOption()
	opt.Mode = runModeIncrementalOnly
	opt.SnowflakeDatabase = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--snowflake.database and --snowflake.schema are required")

	opt = validationOption()
	opt.AWSAccessKey = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--aws.access-key and --aws.secret-key are required")

	opt = validationOption()
	opt.Tables = nil
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no tables specified")
}

func TestMarkSnapshotFinishedInitializesIncrementalCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)
	require.NoError(t, manager.SetSnapshotTSO(ctx, 466924115091783691))

	require.NoError(t, manager.MarkSnapshotFinished(ctx))
	st := manager.Snapshot()
	require.True(t, st.Snapshot.Finished)
	require.Equal(t, uint64(466924115091783691), st.Incremental.CheckpointTS)
}

func newCmdTestStateManager(t *testing.T, ctx context.Context, store storeapi.Storage) state.Manager {
	t.Helper()
	manager, err := state.Open(ctx, store, []string{"db1.t1", "db2.t2"})
	require.NoError(t, err)
	return manager
}

func validationOption() *Option {
	opt := baseOption()
	opt.TiDBHost = "127.0.0.1"
	opt.TiDBPort = 4000
	opt.TiDBUser = "root"
	opt.SnowflakeDatabase = "SNOW"
	opt.SnowflakeSchema = "PUBLIC"
	return opt
}
