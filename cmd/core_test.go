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
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
)

func baseOption() *Option {
	opt := NewOption()
	opt.StoragePath = "s3://bucket/path"
	opt.AWSAccessKey = "AKIA"
	opt.AWSSecretKey = "secret"
	opt.SnowflakeAccountID = "org-account"
	opt.SnowflakeUser = "sf-user"
	opt.SnowflakePass = "sf-pass"
	opt.SnowflakeDatabase = "SNOW"
	opt.Tables = []string{"db1.t1", "db2.t2"}
	return opt
}

func TestNewOptionDefaults(t *testing.T) {
	opt := NewOption()

	require.Equal(t, "127.0.0.1", opt.TiDBHost)
	require.Equal(t, 4000, opt.TiDBPort)
	require.Equal(t, "root", opt.TiDBUser)
	require.Equal(t, cloudapi.DefaultHost, opt.TiDBCloudHost)
	require.Equal(t, 8, opt.SnapshotConcurrency)
	require.Equal(t, "COMPUTE_WH", opt.SnowflakeWarehouse)
	require.Equal(t, 60*time.Second, opt.ChangefeedFlushInterval)
	require.Equal(t, 64, opt.ChangefeedFileSizeMiB)
	require.Equal(t, defaultIncrementScanInterval, opt.IncrementScanInterval)
	require.Equal(t, snapshotCompressionNone, opt.SnapshotCompression)
	require.Equal(t, snapshotCSVNullValue, opt.SnapshotCSVNullValue)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)
	require.Equal(t, runModeAll, opt.Mode)
	require.Empty(t, opt.SnapshotTSO)
	require.Equal(t, 2, opt.ChangefeedRCU)
}

func TestValidateConfig(t *testing.T) {
	opt := &Option{
		StoragePath:             "s3://bucket/path",
		AWSAccessKey:            "AKIA",
		AWSSecretKey:            "secret",
		SnowflakeAccountID:      "org-account",
		SnowflakeUser:           "sf-user",
		SnowflakePass:           "sf-pass",
		SnowflakeDatabase:       "SNOW",
		Tables:                  []string{"db1.t1"},
		Mode:                    "SNAPSHOT-ONLY",
		SourceMode:              "OP",
		SnapshotCompression:     "GZIP",
		ChangefeedFlushInterval: -1,
		ChangefeedFileSizeMiB:   -1,
		IncrementScanInterval:   -1,
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
	require.Equal(t, defaultIncrementScanInterval, opt.IncrementScanInterval)
	require.Equal(t, runModeSnapshotOnly, opt.Mode)
	require.Equal(t, sourceModeOP, opt.SourceMode)
	require.Equal(t, snapshotCompressionGzip, opt.SnapshotCompression)
	require.Equal(t, 8, opt.SnapshotConcurrency)
	require.Equal(t, snapshotCSVNullValue, opt.SnapshotCSVNullValue)

	opt = &Option{
		StoragePath:         "s3://bucket/path",
		SourceMode:          sourceModeTiDBCloud,
		SnapshotConcurrency: -1,
		AWSAccessKey:        "AKIA",
		AWSSecretKey:        "secret",
		SnowflakeAccountID:  "org-account",
		SnowflakeUser:       "sf-user",
		SnowflakePass:       "sf-pass",
		SnowflakeDatabase:   "SNOW",
		Tables:              []string{"db1.t1"},
	}
	err = opt.validate()
	require.NoError(t, err)
	require.Equal(t, runModeAll, opt.Mode)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)
	require.Equal(t, snapshotCompressionNone, opt.SnapshotCompression)
	require.Equal(t, -1, opt.SnapshotConcurrency)

	opt = &Option{
		StoragePath:         "s3://bucket/path",
		Mode:                "unknown",
		SourceMode:          "unknown",
		SnapshotCompression: "unknown",
		AWSAccessKey:        "AKIA",
		AWSSecretKey:        "secret",
		SnowflakeAccountID:  "org-account",
		SnowflakeUser:       "sf-user",
		SnowflakePass:       "sf-pass",
		SnowflakeDatabase:   "SNOW",
		Tables:              []string{"db1.t1"},
	}
	err = opt.validate()
	require.NoError(t, err)
	require.Equal(t, runModeAll, opt.Mode)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)
	require.Equal(t, snapshotCompressionNone, opt.SnapshotCompression)
	require.Equal(t, 0, opt.SnapshotConcurrency)

	opt = validationOption()
	err = opt.validate()
	require.NoError(t, err)
	require.Equal(t, runModeAll, opt.Mode)
	require.Equal(t, sourceModeTiDBCloud, opt.SourceMode)

	opt = validationOption()
	opt.SnapshotTSO = "0"
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso must be greater than 0")

	opt = validationOption()
	opt.SourceMode = sourceModeOP
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "ticdc.address is required when source=op")

	opt = validationOption()
	opt.Mode = runModeIncrementalOnly
	opt.SnowflakeDatabase = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "snowflake.database is required")

	opt = validationOption()
	opt.AWSAccessKey = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage.access-key and storage.secret-access-key are required")

	opt = validationOption()
	opt.Tables = nil
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "tables is required")

	opt = validationOption()
	opt.StoragePath = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage.uri is required")

	opt = validationOption()
	opt.SnowflakeAccountID = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "snowflake.account-id is required")

	opt = validationOption()
	opt.SnowflakeUser = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "snowflake.user is required")

	opt = validationOption()
	opt.SnowflakePass = ""
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "snowflake.password is required")

	opt = validationOption()
	opt.ChangefeedRCU = -1
	err = opt.validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "changefeed.rcu must be greater than 0")
}

func TestValidateRejectsColumnSelectorsForOP(t *testing.T) {
	opt := baseOption()
	opt.SourceMode = sourceModeOP
	opt.Mode = runModeSnapshotOnly
	opt.ColumnSelectors = []cloudapi.ColumnSelector{{
		Matcher: []string{"db1.t1"},
		Columns: []string{"*", "!customer_email"},
	}}

	err := opt.validate()
	require.ErrorContains(t, err, "column-selectors is currently supported only when source=tidbcloud")
}

func TestMarkSnapshotFinishedSetsCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)

	err = manager.MarkSnapshotFinished(ctx, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checkpoint_ts is empty")

	require.NoError(t, manager.MarkSnapshotFinished(ctx, 466924115091783691))
	st := manager.Snapshot()
	require.True(t, st.SnapshotFinished)
	require.Equal(t, uint64(466924115091783691), st.CheckpointTS)
}

func newCmdTestStateManager(t *testing.T, ctx context.Context, store storeapi.Storage) state.Manager {
	t.Helper()
	manager, err := state.Open(ctx, store, []string{"db1.t1", "db2.t2"}, false)
	require.NoError(t, err)
	return manager
}

func validationOption() *Option {
	opt := baseOption()
	opt.TiDBHost = "127.0.0.1"
	opt.TiDBPort = 4000
	opt.TiDBUser = "root"
	return opt
}
