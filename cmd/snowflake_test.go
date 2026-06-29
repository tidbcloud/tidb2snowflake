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
	require.Equal(t, "tidbcloud", sourceModeFlag.DefValue)

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

func TestSnowflakeCmdReadsTiDBCloudFlags(t *testing.T) {
	var captured *Config
	cmd := newSnowflakeCmdWithRun(func(_ context.Context, cfg *Config) error {
		captured = cfg
		return nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--tidbcloud.cluster-id", "cluster-from-flag",
		"--tidbcloud.public-key", "public-from-flag",
		"--tidbcloud.private-key", "private-from-flag",
		"--tidbcloud.host", "api.flag.example.com",
		"--aws.access-key", "AKIA",
		"--aws.secret-key", "secret",
		"--snowflake.database", "SNOW",
		"--snowflake.schema", "PUBLIC",
		"--storage", "s3://bucket/path",
		"--table", "db1.t1",
	})

	require.NoError(t, cmd.Execute())
	require.NotNil(t, captured)
	require.Equal(t, "cluster-from-flag", captured.TiDBCloud.ClusterID)
	require.Equal(t, "public-from-flag", captured.TiDBCloud.PublicKey)
	require.Equal(t, "private-from-flag", captured.TiDBCloud.PrivateKey)
	require.Equal(t, "api.flag.example.com", captured.TiDBCloud.Host)
}
