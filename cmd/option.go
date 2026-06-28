package cmd

import (
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/source"
	"go.uber.org/zap"
)

const (
	runModeFull            = "full"
	runModeSnapshotOnly    = "snapshot-only"
	runModeIncrementalOnly = "incremental-only"
)

const (
	sourceModeTiDBCloud = "tidbcloud"
	sourceModeOP        = "op"
)

const (
	snapshotCompressionNone = "none"
	snapshotCompressionGzip = "gzip"
)

// Option contains the raw values accepted from the command line.
type Option struct {
	StoragePath  string
	AWSAccessKey string
	AWSSecretKey string

	TiDBHost  string
	TiDBPort  int
	TiDBUser  string
	TiDBPass  string
	TiDBTLS   bool
	TiDBSSLCA string

	TiDBCloudClusterID  string
	TiDBCloudPublicKey  string
	TiDBCloudPrivateKey string
	TiDBCloudHost       string

	TiCDCAddress        string
	SnapshotConcurrency int

	SnowflakeAccountID string
	SnowflakeWarehouse string
	SnowflakeUser      string
	SnowflakePass      string
	SnowflakeDatabase  string
	SnowflakeSchema    string

	Tables []string

	SnapshotTSO uint64

	ChangefeedFlushInterval time.Duration
	ChangefeedFileSizeMiB   int
	SnapshotCompression     string

	SourceMode string

	Mode string
}

// NewOption returns an Option initialized with CLI defaults.
func NewOption() *Option {
	return &Option{
		TiDBHost:                "127.0.0.1",
		TiDBPort:                4000,
		TiDBUser:                "root",
		SnapshotConcurrency:     8,
		SnowflakeWarehouse:      "COMPUTE_WH",
		ChangefeedFlushInterval: 60 * time.Second,
		ChangefeedFileSizeMiB:   64,
		SnapshotCompression:     snapshotCompressionNone,
		SourceMode:              sourceModeTiDBCloud,
		Mode:                    runModeFull,
	}
}

func (opt *Option) tidbConfig() *tidb.Config {
	return &tidb.Config{
		Host:  opt.TiDBHost,
		Port:  opt.TiDBPort,
		User:  opt.TiDBUser,
		Pass:  opt.TiDBPass,
		TLS:   opt.TiDBTLS,
		SSLCA: opt.TiDBSSLCA,
	}
}

func (opt *Option) snowflakeConfig() *snowflake.Config {
	return &snowflake.Config{
		AccountId: opt.SnowflakeAccountID,
		Warehouse: opt.SnowflakeWarehouse,
		User:      opt.SnowflakeUser,
		Pass:      opt.SnowflakePass,
		Database:  opt.SnowflakeDatabase,
		Schema:    opt.SnowflakeSchema,
	}
}

func (opt *Option) PrepareRequest(
	cred *credentials.Value,
	storageURI *url.URL,
) source.Request {
	return source.Request{
		PrepareSnapshot:   opt.Mode != runModeIncrementalOnly,
		PrepareChangefeed: opt.Mode != runModeSnapshotOnly,
		UseOPSource:       opt.SourceMode == sourceModeOP,
		SnapshotTSO:       opt.SnapshotTSO,
		TiDB:              opt.tidbConfig(),

		TiDBCloudClusterID:  opt.TiDBCloudClusterID,
		TiDBCloudPublicKey:  opt.TiDBCloudPublicKey,
		TiDBCloudPrivateKey: opt.TiDBCloudPrivateKey,
		TiDBCloudHost:       opt.TiDBCloudHost,

		TiCDCAddress:        opt.TiCDCAddress,
		SnapshotConcurrency: opt.SnapshotConcurrency,

		Tables: opt.Tables,

		ChangefeedFlushInterval: opt.ChangefeedFlushInterval,
		ChangefeedFileSizeMiB:   opt.ChangefeedFileSizeMiB,

		SnapshotCompression: opt.SnapshotCompression,

		Credential:  cred,
		StoragePath: opt.StoragePath,
		StorageURI:  storageURI,
	}
}

func (opt *Option) adjust() {
	defaults := NewOption()

	opt.SourceMode = strings.ToLower(strings.TrimSpace(opt.SourceMode))
	switch opt.SourceMode {
	case sourceModeTiDBCloud, sourceModeOP:
	default:
		opt.SourceMode = defaults.SourceMode
	}

	if opt.TiDBHost == "" {
		opt.TiDBHost = defaults.TiDBHost
	}
	if opt.TiDBPort <= 0 {
		opt.TiDBPort = defaults.TiDBPort
	}
	if opt.TiDBUser == "" {
		opt.TiDBUser = defaults.TiDBUser
	}
	if opt.SnowflakeWarehouse == "" {
		opt.SnowflakeWarehouse = defaults.SnowflakeWarehouse
	}
	if opt.ChangefeedFlushInterval <= 0 {
		opt.ChangefeedFlushInterval = defaults.ChangefeedFlushInterval
	}
	if opt.ChangefeedFileSizeMiB <= 0 {
		opt.ChangefeedFileSizeMiB = defaults.ChangefeedFileSizeMiB
	}

	opt.Mode = strings.ToLower(strings.TrimSpace(opt.Mode))
	switch opt.Mode {
	case runModeFull, runModeSnapshotOnly, runModeIncrementalOnly:
	default:
		opt.Mode = defaults.Mode
	}

	switch opt.SourceMode {
	case sourceModeOP:
		if opt.SnapshotConcurrency <= 0 {
			opt.SnapshotConcurrency = defaults.SnapshotConcurrency
		}
	}

	opt.SnapshotCompression = strings.ToLower(strings.TrimSpace(opt.SnapshotCompression))
	switch opt.SnapshotCompression {
	case snapshotCompressionNone, snapshotCompressionGzip:
	default:
		opt.SnapshotCompression = defaults.SnapshotCompression
	}
}

func (opt *Option) validate() error {
	opt.adjust()

	if opt.SourceMode == sourceModeOP {
		if opt.Mode != runModeSnapshotOnly && opt.TiCDCAddress == "" {
			return errors.New("--ticdc.address is required when --source.mode=op")
		}
	}

	if opt.AWSAccessKey == "" || opt.AWSSecretKey == "" {
		return errors.New("--aws.access-key and --aws.secret-key are required")
	}

	if opt.SnowflakeDatabase == "" || opt.SnowflakeSchema == "" {
		return errors.New("--snowflake.database and --snowflake.schema are required")
	}

	if len(opt.Tables) == 0 {
		return errors.New("no tables specified")
	}
	// TiDB Cloud API credentials are only required when an export/changefeed has
	// to be created (i.e. the snapshot/increment data does not already exist), so
	// they are validated lazily during the run rather than here. This lets a user
	// load a pre-created snapshot/changefeed without API keys.
	opt.logSummary()
	return nil
}

func (opt *Option) logSummary() {
	log.Info("replication options validated",
		zap.Int("tableCount", len(opt.Tables)),
		zap.String("mode", opt.Mode),
		zap.String("sourceMode", opt.SourceMode),
		zap.String("snapshotCompression", opt.SnapshotCompression),
		zap.Duration("changefeedFlushInterval", opt.ChangefeedFlushInterval),
		zap.Int("changefeedFileSizeMiB", opt.ChangefeedFileSizeMiB),
		zap.Bool("tidbCloudConfigured", opt.TiDBCloudClusterID != ""),
		zap.String("ticdcAddress", opt.TiCDCAddress),
		zap.String("snowflakeDatabase", opt.SnowflakeDatabase),
		zap.String("snowflakeSchema", opt.SnowflakeSchema))
}
