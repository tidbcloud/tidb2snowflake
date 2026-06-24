package cmd

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"go.uber.org/zap"
)

// NewSnowflakeCmd builds the `snowflake` subcommand.
func NewSnowflakeCmd() *cobra.Command {
	cfg := &Config{
		TiDB:      &tidb.Config{},
		Snowflake: &snowflake.Config{},
	}
	var (
		logFile  string
		logLevel string
	)

	cmd := &cobra.Command{
		Use:   "snowflake",
		Short: "Replicate snapshot and incremental data from TiDB to Snowflake",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := logutil.InitLogger(&logutil.Config{Level: logLevel, File: logFile}); err != nil {
				return errors.Trace(err)
			}
			if err := validateConfig(cfg); err != nil {
				return err
			}
			ctx := context.Background()
			if err := Replicate(ctx, cfg); err != nil {
				log.Error("replication failed", zap.Error(err))
				return err
			}
			log.Info("replication finished")
			return nil
		},
	}

	f := cmd.Flags()
	// run mode
	f.Var(enumflag.New(&cfg.Mode, "mode", RunModeIds, enumflag.EnumCaseInsensitive), "mode",
		"replication mode: full, snapshot-only, incremental-only")
	f.Var(enumflag.New(&cfg.SourceMode, "source.mode", SourceModeIds, enumflag.EnumCaseInsensitive), "source.mode",
		"source deployment mode: tidbcloud or op")

	// TiDB connection (used to read source table schema)
	f.StringVar(&cfg.TiDB.Host, "tidb.host", "127.0.0.1", "TiDB host")
	f.IntVarP(&cfg.TiDB.Port, "tidb.port", "P", 4000, "TiDB port")
	f.StringVarP(&cfg.TiDB.User, "tidb.user", "u", "root", "TiDB user")
	f.StringVarP(&cfg.TiDB.Pass, "tidb.pass", "p", "", "TiDB password")
	f.BoolVar(&cfg.TiDB.TLS, "tidb.tls", false, "enable TLS for TiDB connection")
	f.StringVar(&cfg.TiDB.SSLCA, "tidb.ssl-ca", "", "TiDB SSL CA path")

	// TiDB Cloud OpenAPI
	f.StringVar(&cfg.TiDBCloud.ClusterID, "tidbcloud.cluster-id", "", "TiDB Cloud Serverless cluster ID")
	f.StringVar(&cfg.TiDBCloud.PublicKey, "tidbcloud.public-key", "", "TiDB Cloud API key public part")
	f.StringVar(&cfg.TiDBCloud.PrivateKey, "tidbcloud.private-key", "", "TiDB Cloud API key private part")
	f.StringVar(&cfg.TiDBCloud.Host, "tidbcloud.host", "", "TiDB Cloud OpenAPI host (default serverless.tidbapi.com)")

	// OP deployment services
	f.StringVar(&cfg.OP.TiCDCAddress, "ticdc.address", "", "TiCDC OpenAPI base address for --source.mode=op, e.g. http://127.0.0.1:8300")
	f.IntVar(&cfg.OP.SnapshotConcurrency, "snapshot.concurrency", 8, "Dumpling snapshot dump concurrency for --source.mode=op")

	// Snowflake
	f.StringVar(&cfg.Snowflake.AccountId, "snowflake.account-id", "", "Snowflake account id: <organization>-<account>")
	f.StringVar(&cfg.Snowflake.Warehouse, "snowflake.warehouse", "COMPUTE_WH", "Snowflake warehouse")
	f.StringVar(&cfg.Snowflake.User, "snowflake.user", "", "Snowflake user")
	f.StringVar(&cfg.Snowflake.Pass, "snowflake.pass", "", "Snowflake password")
	f.StringVar(&cfg.Snowflake.Database, "snowflake.database", "", "Snowflake database")
	f.StringVar(&cfg.Snowflake.Schema, "snowflake.schema", "", "Snowflake schema")

	// tables and storage
	f.StringArrayVarP(&cfg.Tables, "table", "t", nil, "fully qualified table name, repeatable, e.g. -t db1.t1 -t db2.t2")
	f.StringVarP(&cfg.StoragePath, "storage", "s", "", "object storage path, e.g. s3://<bucket>/<path>")
	f.StringVar(&cfg.AWSAccessKey, "aws.access-key", "", "AWS access key for the storage bucket")
	f.StringVar(&cfg.AWSSecretKey, "aws.secret-key", "", "AWS secret key for the storage bucket")

	// consistency / changefeed tuning
	f.StringVar(&cfg.SnapshotTSO, "snapshot-tso", "", "pin the snapshot to a specific TiDB TSO (optional; default: chosen at export time)")
	f.StringVar(&cfg.SnapshotLoadMode, "snapshot.load-mode", SnapshotLoadModeBulk, "snapshot load mode: bulk or per-file")
	f.StringVar(&cfg.SnapshotCompression, "snapshot.compression", SnapshotCompressionNone, "snapshot export compression: none or gzip")
	f.DurationVar(&cfg.ChangefeedFlushInterval, "changefeed.flush-interval", 60*time.Second, "changefeed flush interval")
	f.IntVar(&cfg.ChangefeedFileSizeMiB, "changefeed.file-size", 64, "changefeed file size in MiB")
	f.DurationVar(&cfg.PollInterval, "poll-interval", 10*time.Second, "interval to poll export/changefeed status")

	// logging
	f.StringVar(&logFile, "log.file", "", "log file path")
	f.StringVar(&logLevel, "log.level", "info", "log level")

	_ = cmd.MarkFlagRequired("storage")
	_ = cmd.MarkFlagRequired("table")

	return cmd
}

func validateConfig(cfg *Config) error {
	if cfg.AWSAccessKey == "" || cfg.AWSSecretKey == "" {
		return errors.New("--aws.access-key and --aws.secret-key are required")
	}
	switch sourceMode(cfg) {
	case SourceModeTiDBCloud, SourceModeOP:
	default:
		return errors.Errorf("--source.mode must be %q or %q", sourceModeString(SourceModeTiDBCloud), sourceModeString(SourceModeOP))
	}
	if sourceMode(cfg) == SourceModeOP && cfg.Mode != RunModeSnapshotOnly && cfg.OP.TiCDCAddress == "" {
		return errors.New("--ticdc.address is required when --source.mode=op")
	}
	switch snapshotLoadMode(cfg) {
	case SnapshotLoadModeBulk, SnapshotLoadModePerFile:
	default:
		return errors.Errorf("--snapshot.load-mode must be %q or %q", SnapshotLoadModeBulk, SnapshotLoadModePerFile)
	}
	switch snapshotCompression(cfg) {
	case SnapshotCompressionNone, SnapshotCompressionGzip:
	default:
		return errors.Errorf("--snapshot.compression must be %q or %q", SnapshotCompressionNone, SnapshotCompressionGzip)
	}
	// TiDB Cloud API credentials are only required when an export/changefeed has
	// to be created (i.e. the snapshot/increment data does not already exist), so
	// they are validated lazily during the run rather than here. This lets a user
	// load a pre-created snapshot/changefeed without API keys.
	if cfg.Mode != RunModeIncrementalOnly {
		if cfg.Snowflake.Database == "" || cfg.Snowflake.Schema == "" {
			return errors.New("--snowflake.database and --snowflake.schema are required")
		}
	}
	return nil
}
