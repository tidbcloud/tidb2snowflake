package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnowflakeCmdOnlyExposesConfigFlag(t *testing.T) {
	cmd := NewSnowflakeCmd()

	require.NotNil(t, cmd.Flags().Lookup("config"))
	require.Nil(t, cmd.Flags().Lookup("source.mode"))
	require.Nil(t, cmd.Flags().Lookup("tidb.host"))
	require.Nil(t, cmd.Flags().Lookup("ticdc.address"))
	require.Nil(t, cmd.Flags().Lookup("snowflake.database"))
	require.Nil(t, cmd.Flags().Lookup("storage"))
	require.Nil(t, cmd.Flags().Lookup("table"))
	require.Nil(t, cmd.Flags().Lookup("aws.access-key"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.public-key"))
}

func TestSnowflakeCmdLoadsTiDBCloudConfig(t *testing.T) {
	t.Setenv("TIDBCLOUD_CLUSTER_ID", "cluster-from-env")
	t.Setenv("TIDBCLOUD_PUBLIC_KEY", "public-from-env")
	t.Setenv("TIDBCLOUD_PRIVATE_KEY", "private-from-env")
	t.Setenv("TIDBCLOUD_HOST", "api.env.example.com")

	var captured *Option
	cmd := newSnowflakeCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
mode = "full"
source = "tidbcloud"
tables = ["db1.t1", "db2.t2"]

[storage]
uri = "s3://bucket/path?region=us-west-2"
access-key = "AKIA"
secret-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
pass = "sf-pass"
database = "SNOW"

[tidbcloud]
cluster-id = "cluster-from-config"
public-key = "public-from-config"
private-key = "private-from-config"
host = " api.config.example.com "
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "s3://bucket/path?region=us-west-2", captured.StoragePath)
	require.Equal(t, []string{"db1.t1", "db2.t2"}, captured.Tables)
	require.Equal(t, sourceModeTiDBCloud, captured.SourceMode)
	require.Equal(t, runModeFull, captured.Mode)
	require.Equal(t, "COMPUTE_WH", captured.SnowflakeWarehouse)
	require.Equal(t, "cluster-from-config", captured.TiDBCloudClusterID)
	require.Equal(t, "public-from-config", captured.TiDBCloudPublicKey)
	require.Equal(t, "private-from-config", captured.TiDBCloudPrivateKey)
	require.Equal(t, "api.config.example.com", captured.TiDBCloudHost)
}

func TestSnowflakeCmdLoadsOPConfig(t *testing.T) {
	var captured *Option
	cmd := newSnowflakeCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
mode = "full"
source = "op"
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-key = "secret"

[tidb]
host = "tidb.example.com"
port = 4400
user = "tidb-user"
pass = "tidb-pass"
tls = true
ssl-ca = "/tmp/ca.pem"

[ticdc]
address = "http://127.0.0.1:8300"

[snowflake]
account-id = "org-account"
user = "sf-user"
pass = "sf-pass"
database = "SNOW"
warehouse = "WH"

[snapshot]
tso = "466924115091783691"
compression = "gzip"
concurrency = 16

[changefeed]
flush-interval = "10s"
file-size = 128

[increment]
scan-interval = "5s"
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, sourceModeOP, captured.SourceMode)
	require.Equal(t, "tidb.example.com", captured.TiDBHost)
	require.Equal(t, 4400, captured.TiDBPort)
	require.Equal(t, "tidb-user", captured.TiDBUser)
	require.Equal(t, "tidb-pass", captured.TiDBPass)
	require.True(t, captured.TiDBTLS)
	require.Equal(t, "/tmp/ca.pem", captured.TiDBSSLCA)
	require.Equal(t, "http://127.0.0.1:8300", captured.TiCDCAddress)
	require.Equal(t, "WH", captured.SnowflakeWarehouse)
	require.Equal(t, "466924115091783691", captured.SnapshotTSO)
	require.Equal(t, snapshotCompressionGzip, captured.SnapshotCompression)
	require.Equal(t, 16, captured.SnapshotConcurrency)
	require.Equal(t, 10*time.Second, captured.ChangefeedFlushInterval)
	require.Equal(t, 128, captured.ChangefeedFileSizeMiB)
	require.Equal(t, 5*time.Second, captured.IncrementScanInterval)
}

func TestSnowflakeCmdRejectsOldBusinessFlags(t *testing.T) {
	cmd := newSnowflakeCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called when old flags are passed")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--table", "db1.t1"})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown flag: --table")
}

func TestSnowflakeCmdRequiresConfig(t *testing.T) {
	cmd := newSnowflakeCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called without config")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--config is required")
}

func TestSnowflakeCmdRejectsUnknownConfigKey(t *testing.T) {
	cmd := newSnowflakeCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called for invalid config")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
tables = ["db1.t1"]
unknown = "value"

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
pass = "sf-pass"
database = "SNOW"
`)})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown configuration options")
	require.Contains(t, err.Error(), "unknown")
}

func TestSnowflakeCmdRejectsInvalidDuration(t *testing.T) {
	cmd := newSnowflakeCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called for invalid config")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
pass = "sf-pass"
database = "SNOW"

[changefeed]
flush-interval = "bad"
`)})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse changefeed.flush-interval")
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
