package main

import (
	"fmt"
	"os"

	"github.com/pingcap-inc/tidb2snowflake/cmd"
	"github.com/pingcap-inc/tidb2snowflake/version"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "tidb2snowflake",
		Short: "Replicate data from TiDB Cloud to Snowflake via TiDB Cloud OpenAPI",
		Long: "tidb2snowflake orchestrates a TiDB Cloud Serverless/Essential cluster to\n" +
			"replicate snapshot and incremental data into Snowflake. Snapshot export and\n" +
			"incremental changefeed are driven through TiDB Cloud OpenAPI; this tool loads\n" +
			"the resulting object-storage files into Snowflake.",
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
