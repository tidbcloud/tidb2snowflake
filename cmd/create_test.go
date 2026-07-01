package cmd

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateCmdExposesSourceModeAndOPFlags(t *testing.T) {
	cmd := NewCreateCmd()

	require.Equal(t, "create", cmd.Use)

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

func TestCreateCmdDoesNotExposeTiDBCloudCredentialFlags(t *testing.T) {
	cmd := NewCreateCmd()

	require.Nil(t, cmd.Flags().Lookup("tidbcloud.cluster-id"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.public-key"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.private-key"))
	require.Nil(t, cmd.Flags().Lookup("tidbcloud.host"))
}

func TestCreateCmdReadsTiDBCloudEnvironment(t *testing.T) {
	t.Setenv(envTiDBCloudClusterID, "cluster-from-env")
	t.Setenv(envTiDBCloudPublicKey, "public-from-env")
	t.Setenv(envTiDBCloudPrivateKey, "private-from-env")
	t.Setenv(envTiDBCloudHost, " api.env.example.com ")

	var captured *Option
	cmd := newCreateCmdWithRun(func(_ context.Context, opt *Option) error {
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
		"--snowflake.schema", "PUBLIC",
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

func TestCreateCmdRejectsTiDBCloudCredentialFlags(t *testing.T) {
	cmd := newCreateCmdWithRun(func(context.Context, *Option) error {
		t.Fatal("run should not be called when TiDB Cloud flags are passed")
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--aws.access-key", "AKIA",
		"--aws.secret-key", "secret",
		"--snowflake.database", "SNOW",
		"--snowflake.schema", "PUBLIC",
		"--storage", "s3://bucket/path",
		"--table", "db1.t1",
		"--tidbcloud.public-key", "public-from-flag",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown flag: --tidbcloud.public-key")
}

func TestDeleteCmdExposesTaskDeleteFlags(t *testing.T) {
	cmd := NewDeleteCmd()

	require.Equal(t, "delete", cmd.Use)
	require.NotNil(t, cmd.Flags().Lookup("source.mode"))
	require.NotNil(t, cmd.Flags().Lookup("ticdc.address"))
	require.NotNil(t, cmd.Flags().Lookup("storage"))
	require.NotNil(t, cmd.Flags().Lookup("aws.access-key"))
	require.NotNil(t, cmd.Flags().Lookup("aws.secret-key"))
	require.Nil(t, cmd.Flags().Lookup("table"))
	require.Nil(t, cmd.Flags().Lookup("snowflake.database"))
}

func TestDeleteCmdReadsTiDBCloudEnvironment(t *testing.T) {
	t.Setenv(envTiDBCloudClusterID, "cluster-from-env")
	t.Setenv(envTiDBCloudPublicKey, "public-from-env")
	t.Setenv(envTiDBCloudPrivateKey, "private-from-env")
	t.Setenv(envTiDBCloudHost, " api.env.example.com ")

	var captured *Option
	cmd := newDeleteCmdWithRun(func(_ context.Context, opt *Option) error {
		require.NoError(t, opt.validateDelete())
		captured = opt
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--aws.access-key", "AKIA",
		"--aws.secret-key", "secret",
		"--storage", "s3://bucket/path",
	})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "cluster-from-env", captured.TiDBCloudClusterID)
	require.Equal(t, "public-from-env", captured.TiDBCloudPublicKey)
	require.Equal(t, "private-from-env", captured.TiDBCloudPrivateKey)
	require.Equal(t, "api.env.example.com", captured.TiDBCloudHost)
}
