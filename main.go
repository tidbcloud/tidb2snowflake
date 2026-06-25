package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/tidbcloud/tidb2snowflake/cmd"
	"github.com/tidbcloud/tidb2snowflake/version"
)

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "tidb2snowflake",
		Short: "Replicate TiDB snapshot and incremental data into Snowflake",
		Long: "tidb2snowflake loads TiDB snapshot and incremental object-storage files into Snowflake.\n" +
			"It can create TiDB Cloud export/changefeed jobs through OpenAPI, or use OP\n" +
			"Dumpling/TiCDC output for self-managed TiDB deployments.",
		SilenceUsage: true,
	}

	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(cmd.NewSnowflakeCmd())
	return rootCmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version of tidb2snowflake",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(version.NewVersion().String())
		},
	}
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
