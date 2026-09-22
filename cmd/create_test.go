package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
)

func TestCreateCmdOnlyExposesConfigFlag(t *testing.T) {
	cmd := NewCreateCmd()

	require.Contains(t, cmd.Use, "create")
	configFlag := cmd.Flags().Lookup("config")
	require.NotNil(t, configFlag)
	require.Equal(t, "c", configFlag.Shorthand)
	require.Nil(t, cmd.Flags().Lookup("source.mode"))
	require.Nil(t, cmd.Flags().Lookup("tidb.host"))
	require.Nil(t, cmd.Flags().Lookup("ticdc.address"))
	require.Nil(t, cmd.Flags().Lookup("snowflake.database"))
	require.Nil(t, cmd.Flags().Lookup("storage"))
	require.Nil(t, cmd.Flags().Lookup("table"))
	require.Nil(t, cmd.Flags().Lookup("aws.access-key"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.public-key"))
}

func TestCreateCmdLoadsTiDBCloudConfig(t *testing.T) {
	t.Setenv("TIDBCLOUD_CLUSTER_ID", "cluster-from-env")
	t.Setenv("TIDBCLOUD_PUBLIC_KEY", "public-from-env")
	t.Setenv("TIDBCLOUD_PRIVATE_KEY", "private-from-env")
	t.Setenv("TIDBCLOUD_HOST", "api.env.example.com")

	var captured *Option
	cmd := newCreateCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
mode = "all"
source = "tidbcloud"
tables = ["db1.t1", "db2.t2"]

[[column-selectors]]
matcher = ["db1.t1"]
columns = ["*", "!customer_email"]

[storage]
uri = "s3://bucket/path?region=us-west-2"
access-key = "AKIA"
secret-access-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
password = "sf-pass"
database = "SNOW"

[tidbcloud]
cluster-id = "cluster-from-config"
public-key = "public-from-config"
private-key = "private-from-config"
host = " api.config.example.com "

[changefeed]
rcu = 8
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "s3://bucket/path?region=us-west-2", captured.StoragePath)
	require.Equal(t, []string{"db1.t1", "db2.t2"}, captured.Tables)
	require.Equal(t, sourceModeTiDBCloud, captured.SourceMode)
	require.Equal(t, runModeAll, captured.Mode)
	require.Equal(t, "COMPUTE_WH", captured.SnowflakeWarehouse)
	require.Equal(t, "cluster-from-config", captured.TiDBCloudClusterID)
	require.Equal(t, "public-from-config", captured.TiDBCloudPublicKey)
	require.Equal(t, "private-from-config", captured.TiDBCloudPrivateKey)
	require.Equal(t, "api.config.example.com", captured.TiDBCloudHost)
	require.Equal(t, 8, captured.ChangefeedRCU)
	require.Equal(t, []cloudapi.ColumnSelector{{
		Matcher: []string{"db1.t1"},
		Columns: []string{"*", "!customer_email"},
	}}, captured.ColumnSelectors)
}

func TestCreateCmdLoadsConfigWithShorthand(t *testing.T) {
	var captured *Option
	cmd := newCreateCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"-c", writeConfigFile(t, `
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-access-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
password = "sf-pass"
database = "SNOW"
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "s3://bucket/path", captured.StoragePath)
	require.Equal(t, 2, captured.ChangefeedRCU)
}

func TestCreateCmdDefaultsEmptyTiDBCloudHost(t *testing.T) {
	var captured *Option
	cmd := newCreateCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-access-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
password = "sf-pass"
database = "SNOW"

[tidbcloud]
host = "   "
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, cloudapi.DefaultHost, captured.TiDBCloudHost)
}

func TestCreateCmdLoadsExplicitZeroChangefeedRCU(t *testing.T) {
	var captured *Option
	cmd := newCreateCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-access-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
password = "sf-pass"
database = "SNOW"

[changefeed]
rcu = 0
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, 2, captured.ChangefeedRCU)
}

func TestCreateCmdLoadsOPConfig(t *testing.T) {
	var captured *Option
	cmd := newCreateCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
mode = "all"
source = "op"
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-access-key = "secret"

[tidb]
host = "tidb.example.com"
port = 4400
user = "tidb-user"
password = "tidb-pass"
tls = true
ssl-ca = "/tmp/ca.pem"

[ticdc]
address = "http://127.0.0.1:8300"

[snowflake]
account-id = "org-account"
user = "sf-user"
password = "sf-pass"
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

func TestCreateCmdRejectsOldBusinessFlags(t *testing.T) {
	cmd := newCreateCmdWithRun(func(context.Context, *Option) error {
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

func TestCreateCmdRequiresConfig(t *testing.T) {
	cmd := newCreateCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called without config")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--config is required")
}

func TestCreateCmdRejectsUnknownConfigKey(t *testing.T) {
	cmd := newCreateCmdWithRun(func(context.Context, *Option) error {
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
secret-access-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
password = "sf-pass"
database = "SNOW"
`)})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown configuration options")
	require.Contains(t, err.Error(), "unknown")
}

func TestCreateCmdRejectsInvalidDuration(t *testing.T) {
	cmd := newCreateCmdWithRun(func(context.Context, *Option) error {
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
secret-access-key = "secret"

[snowflake]
account-id = "org-account"
user = "sf-user"
password = "sf-pass"
database = "SNOW"

[changefeed]
flush-interval = "bad"
`)})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse changefeed.flush-interval")
}

func TestDeleteCmdOnlyExposesConfigFlag(t *testing.T) {
	cmd := NewDeleteCmd()

	require.Contains(t, cmd.Use, "delete")
	configFlag := cmd.Flags().Lookup("config")
	require.NotNil(t, configFlag)
	require.Equal(t, "c", configFlag.Shorthand)
	require.Nil(t, cmd.Flags().Lookup("source.mode"))
	require.Nil(t, cmd.Flags().Lookup("tidb.host"))
	require.Nil(t, cmd.Flags().Lookup("ticdc.address"))
	require.Nil(t, cmd.Flags().Lookup("snowflake.database"))
	require.Nil(t, cmd.Flags().Lookup("storage"))
	require.Nil(t, cmd.Flags().Lookup("table"))
	require.Nil(t, cmd.Flags().Lookup("aws.access-key"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.public-key"))
}

func TestDeleteCmdLoadsConfig(t *testing.T) {
	var captured *Option
	cmd := newDeleteCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validateDelete())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", writeConfigFile(t, `
source = "tidbcloud"

[storage]
uri = "s3://bucket/path?region=us-west-2"
access-key = "AKIA"
secret-access-key = "secret"

[tidbcloud]
cluster-id = "cluster-from-config"
public-key = "public-from-config"
private-key = "private-from-config"
host = " api.config.example.com "
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "s3://bucket/path?region=us-west-2", captured.StoragePath)
	require.Equal(t, "AKIA", captured.AWSAccessKey)
	require.Equal(t, "secret", captured.AWSSecretKey)
	require.Equal(t, "cluster-from-config", captured.TiDBCloudClusterID)
	require.Equal(t, "public-from-config", captured.TiDBCloudPublicKey)
	require.Equal(t, "private-from-config", captured.TiDBCloudPrivateKey)
	require.Equal(t, "api.config.example.com", captured.TiDBCloudHost)
}

func TestDeleteCmdLoadsConfigWithShorthand(t *testing.T) {
	var captured *Option
	cmd := newDeleteCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validateDelete())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"-c", writeConfigFile(t, `
source = "tidbcloud"

[storage]
uri = "s3://bucket/path"
access-key = "AKIA"
secret-access-key = "secret"

[tidbcloud]
cluster-id = "cluster-from-config"
public-key = "public-from-config"
private-key = "private-from-config"
`)})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "s3://bucket/path", captured.StoragePath)
}

func TestDeleteCmdRequiresConfig(t *testing.T) {
	cmd := newDeleteCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called without config")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--config is required")
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
