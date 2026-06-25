package cmd

import (
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
}
