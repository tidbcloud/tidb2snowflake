package cmd

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnowflakeCmdExposesSourceModeAndOPFlags(t *testing.T) {
	cmd := NewSnowflakeCmd()

	sourceModeFlag := cmd.Flags().Lookup("source.mode")
	require.NotNil(t, sourceModeFlag)
	require.Equal(t, sourceModeTiDBCloud, sourceModeFlag.DefValue)

	ticdcAddressFlag := cmd.Flags().Lookup("ticdc.address")
	require.NotNil(t, ticdcAddressFlag)
	require.Empty(t, ticdcAddressFlag.DefValue)

	snapshotConcurrencyFlag := cmd.Flags().Lookup("snapshot.concurrency")
	require.NotNil(t, snapshotConcurrencyFlag)
	require.Equal(t, "8", snapshotConcurrencyFlag.DefValue)

	incrementScanIntervalFlag := cmd.Flags().Lookup("increment.scan-interval")
	require.NotNil(t, incrementScanIntervalFlag)
	require.Equal(t, "1m0s", incrementScanIntervalFlag.DefValue)
}

func TestSnowflakeCmdDoesNotExposeTiDBCloudCredentialFlags(t *testing.T) {
	cmd := NewSnowflakeCmd()

	require.Nil(t, cmd.Flags().Lookup("tidbcloud.cluster-id"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.public-key"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.private-key"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.host"))
}

func TestSnowflakeCmdReadsTiDBCloudEnvironment(t *testing.T) {
	t.Setenv(envTiDBCloudClusterID, "cluster-from-env")
	t.Setenv(envTiDBCloudPublicKey, "public-from-env")
	t.Setenv(envTiDBCloudPrivateKey, "private-from-env")
	t.Setenv(envTiDBCloudHost, " api.env.example.com ")

	var captured *Option
	cmd := newSnowflakeCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validate())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--aws.access-key", "AKIA",
		"--aws.secret-key", "secret",
		"--snowflake.database", "SNOW",
		"--storage", "s3://bucket/path",
		"--table", "db1.t1",
	})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "cluster-from-env", captured.TiDBCloudClusterID)
	require.Equal(t, "public-from-env", captured.TiDBCloudPublicKey)
	require.Equal(t, "private-from-env", captured.TiDBCloudPrivateKey)
	require.Equal(t, "api.env.example.com", captured.TiDBCloudHost)
}

func TestSnowflakeCmdRejectsTiDBCloudCredentialFlags(t *testing.T) {
	cmd := newSnowflakeCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called when TiDB Cloud flags are passed")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--aws.access-key", "AKIA",
		"--aws.secret-key", "secret",
		"--snowflake.database", "SNOW",
		"--storage", "s3://bucket/path",
		"--table", "db1.t1",
		"--tidbcloud.public-key", "public-from-flag",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown flag: --tidbcloud.public-key")
}
