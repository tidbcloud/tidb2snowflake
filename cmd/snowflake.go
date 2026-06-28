package cmd

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewSnowflakeCmd builds the `snowflake` subcommand.
func NewSnowflakeCmd() *cobra.Command {
	var (
		logFile  string
		logLevel string
	)

	opt := NewOption()
	cmd := &cobra.Command{
		Use:   "snowflake",
		Short: "Replicate snapshot and incremental data from TiDB to Snowflake",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := logger.InitLogger(&logger.Config{Level: logLevel, File: logFile}); err != nil {
				return errors.Trace(err)
			}
			ctx := context.Background()
			if err := Run(ctx, opt); err != nil {
				log.Error("replication failed", zap.Error(err))
				return err
			}
			log.Info("replication finished")
			return nil
		},
	}

	f := cmd.Flags()
	// run mode
	f.StringVar(&opt.Mode, "mode", opt.Mode, "replication mode: full, snapshot-only, incremental-only")
	f.StringVar(&opt.SourceMode, "source.mode", opt.SourceMode, "source deployment mode: tidbcloud or op")

	// TiDB connection (used to read source table schema)
	f.StringVar(&opt.TiDBHost, "tidb.host", opt.TiDBHost, "TiDB host")
	f.IntVarP(&opt.TiDBPort, "tidb.port", "P", opt.TiDBPort, "TiDB port")
	f.StringVarP(&opt.TiDBUser, "tidb.user", "u", opt.TiDBUser, "TiDB user")
	f.StringVarP(&opt.TiDBPass, "tidb.pass", "p", opt.TiDBPass, "TiDB password")
	f.BoolVar(&opt.TiDBTLS, "tidb.tls", opt.TiDBTLS, "enable TLS for TiDB connection")
	f.StringVar(&opt.TiDBSSLCA, "tidb.ssl-ca", opt.TiDBSSLCA, "TiDB SSL CA path")

	// TiDB Cloud OpenAPI
	f.StringVar(&opt.TiDBCloudClusterID, "tidbcloud.cluster-id", opt.TiDBCloudClusterID, "TiDB Cloud Serverless cluster ID")
	f.StringVar(&opt.TiDBCloudPublicKey, "tidbcloud.public-key", opt.TiDBCloudPublicKey, "TiDB Cloud API key public part")
	f.StringVar(&opt.TiDBCloudPrivateKey, "tidbcloud.private-key", opt.TiDBCloudPrivateKey, "TiDB Cloud API key private part")
	f.StringVar(&opt.TiDBCloudHost, "tidbcloud.host", opt.TiDBCloudHost, "TiDB Cloud OpenAPI host (default serverless.tidbapi.com)")

	// OP deployment services
	f.StringVar(&opt.TiCDCAddress, "ticdc.address", opt.TiCDCAddress, "TiCDC OpenAPI base address for --source.mode=op, e.g. http://127.0.0.1:8300")
	f.IntVar(&opt.SnapshotConcurrency, "snapshot.concurrency", opt.SnapshotConcurrency, "Dumpling snapshot dump concurrency for --source.mode=op")

	// Snowflake
	f.StringVar(&opt.SnowflakeAccountID, "snowflake.account-id", opt.SnowflakeAccountID, "Snowflake account id: <organization>-<account>")
	f.StringVar(&opt.SnowflakeWarehouse, "snowflake.warehouse", opt.SnowflakeWarehouse, "Snowflake warehouse")
	f.StringVar(&opt.SnowflakeUser, "snowflake.user", opt.SnowflakeUser, "Snowflake user")
	f.StringVar(&opt.SnowflakePass, "snowflake.pass", opt.SnowflakePass, "Snowflake password")
	f.StringVar(&opt.SnowflakeDatabase, "snowflake.database", opt.SnowflakeDatabase, "Snowflake database")
	f.StringVar(&opt.SnowflakeSchema, "snowflake.schema", opt.SnowflakeSchema, "Snowflake schema")

	// tables and storage
	f.StringArrayVarP(&opt.Tables, "table", "t", opt.Tables, "fully qualified table name, repeatable, e.g. -t db1.t1 -t db2.t2")
	f.StringVarP(&opt.StoragePath, "storage", "s", opt.StoragePath, "object storage path, e.g. s3://<bucket>/<path>")
	f.StringVar(&opt.AWSAccessKey, "aws.access-key", opt.AWSAccessKey, "AWS access key for the storage bucket")
	f.StringVar(&opt.AWSSecretKey, "aws.secret-key", opt.AWSSecretKey, "AWS secret key for the storage bucket")

	// consistency / changefeed tuning
	f.StringVar(&opt.SnapshotTSO, "snapshot-tso", opt.SnapshotTSO, "pin the snapshot to a specific TiDB TSO (optional; default: chosen at export time)")
	f.StringVar(&opt.SnapshotCompression, "snapshot.compression", opt.SnapshotCompression, "snapshot export compression: none or gzip")
	f.DurationVar(&opt.ChangefeedFlushInterval, "changefeed.flush-interval", opt.ChangefeedFlushInterval, "changefeed flush interval")
	f.IntVar(&opt.ChangefeedFileSizeMiB, "changefeed.file-size", opt.ChangefeedFileSizeMiB, "changefeed file size in MiB")

	// logging
	f.StringVar(&logFile, "log.file", "", "log file path")
	f.StringVar(&logLevel, "log.level", "info", "log level")

	_ = cmd.MarkFlagRequired("storage")
	_ = cmd.MarkFlagRequired("table")

	return cmd
}
