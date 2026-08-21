package cmd

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/incremental"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/snapshot"
	"github.com/tidbcloud/tidb2snowflake/source"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

const (
	runModeAll             = "all"
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
	snapshotCSVNullValue    = "\\N"
)

const (
	defaultChangefeedRCU         = 2
	defaultIncrementScanInterval = time.Minute
)

// Option contains the raw values accepted from the config file.
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
	SnowflakeUser      string
	SnowflakePass      string
	SnowflakeWarehouse string
	SnowflakeDatabase  string

	Tables []string

	SnapshotTSO string

	ChangefeedFlushInterval time.Duration
	ChangefeedFileSizeMiB   int
	ChangefeedRCU           int
	IncrementScanInterval   time.Duration
	SnapshotCompression     string
	SnapshotCSVNullValue    string
	ColumnSelectors         []cloudapi.ColumnSelector

	SourceMode string

	Mode string
}

// NewOption returns an Option initialized with CLI defaults.
func NewOption() *Option {
	return &Option{
		TiDBHost:                "127.0.0.1",
		TiDBPort:                4000,
		TiDBUser:                "root",
		TiDBCloudHost:           cloudapi.DefaultHost,
		SnapshotConcurrency:     8,
		SnowflakeWarehouse:      "COMPUTE_WH",
		ChangefeedFlushInterval: 60 * time.Second,
		ChangefeedFileSizeMiB:   64,
		ChangefeedRCU:           defaultChangefeedRCU,
		IncrementScanInterval:   defaultIncrementScanInterval,
		SnapshotCompression:     snapshotCompressionNone,
		SnapshotCSVNullValue:    snapshotCSVNullValue,
		SourceMode:              sourceModeTiDBCloud,
		Mode:                    runModeAll,
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
	}
}

func (opt *Option) prepareRequest(
	cred *aws.Credentials,
	snapshotURI *url.URL,
	incrementURI *url.URL,
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

		TiCDCAddress:         opt.TiCDCAddress,
		SnapshotConcurrency:  opt.SnapshotConcurrency,
		SnapshotCSVNullValue: opt.SnapshotCSVNullValue,

		Tables: opt.Tables,

		ChangefeedFlushInterval: opt.ChangefeedFlushInterval,
		ChangefeedFileSizeMiB:   opt.ChangefeedFileSizeMiB,
		ChangefeedRCU:           opt.ChangefeedRCU,

		SnapshotCompression: opt.SnapshotCompression,
		ColumnSelectors:     opt.ColumnSelectors,

		Credential:   cred,
		SnapshotURI:  snapshotURI,
		IncrementURI: incrementURI,
	}
}

type loadRequest struct {
	LoadSnapshot    bool
	LoadIncremental bool

	Snowflake   *snowflake.Config
	Credential  *aws.Credentials
	StorageURI  *url.URL
	Snapshot    snapshot.Config
	Incremental incremental.Config
}

func (opt *Option) loadRequest(
	cred *aws.Credentials,
	storageURI *url.URL,
) loadRequest {
	return loadRequest{
		LoadSnapshot:    opt.Mode != runModeIncrementalOnly,
		LoadIncremental: opt.Mode != runModeSnapshotOnly,
		Snowflake:       opt.snowflakeConfig(),
		Credential:      cred,
		StorageURI:      storageURI,
		Snapshot: snapshot.Config{
			Tables:      opt.Tables,
			StorageDir:  storage.SnapshotDirName,
			Compression: opt.SnapshotCompression,
		},
		Incremental: incremental.Config{
			Tables:       opt.Tables,
			StorageDir:   storage.IncrementDirName,
			ScanInterval: opt.IncrementScanInterval,
		},
	}
}

func (req loadRequest) tableCount() int {
	if req.LoadSnapshot {
		return len(req.Snapshot.Tables)
	}
	return len(req.Incremental.Tables)
}

func (opt *Option) adjust() {
	defaults := NewOption()

	opt.TiDBCloudClusterID = strings.TrimSpace(opt.TiDBCloudClusterID)
	opt.TiDBCloudPublicKey = strings.TrimSpace(opt.TiDBCloudPublicKey)
	opt.TiDBCloudPrivateKey = strings.TrimSpace(opt.TiDBCloudPrivateKey)
	opt.TiDBCloudHost = strings.TrimSpace(opt.TiDBCloudHost)
	if opt.TiDBCloudHost == "" {
		opt.TiDBCloudHost = defaults.TiDBCloudHost
	}

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
	if opt.IncrementScanInterval <= 0 {
		opt.IncrementScanInterval = defaults.IncrementScanInterval
	}

	opt.Mode = strings.ToLower(strings.TrimSpace(opt.Mode))
	switch opt.Mode {
	case runModeAll, runModeSnapshotOnly, runModeIncrementalOnly:
	default:
		opt.Mode = defaults.Mode
	}

	switch opt.SourceMode {
	case sourceModeOP:
		if opt.SnapshotConcurrency <= 0 {
			opt.SnapshotConcurrency = defaults.SnapshotConcurrency
		}
		if opt.SnapshotCSVNullValue == "" {
			opt.SnapshotCSVNullValue = defaults.SnapshotCSVNullValue
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
		if len(opt.ColumnSelectors) > 0 {
			return errors.New("column-selectors is currently supported only when source=tidbcloud")
		}
		if opt.Mode != runModeSnapshotOnly && opt.TiCDCAddress == "" {
			return errors.New("ticdc.address is required when source=op")
		}
	}

	if opt.StoragePath == "" {
		return errors.New("storage.uri is required")
	}
	if opt.AWSAccessKey == "" || opt.AWSSecretKey == "" {
		return errors.New("storage.access-key and storage.secret-access-key are required")
	}

	if opt.SnowflakeAccountID == "" {
		return errors.New("snowflake.account-id is required")
	}
	if opt.SnowflakeUser == "" {
		return errors.New("snowflake.user is required")
	}
	if opt.SnowflakePass == "" {
		return errors.New("snowflake.password is required")
	}

	if opt.SnowflakeDatabase == "" {
		return errors.New("snowflake.database is required")
	}

	if len(opt.Tables) == 0 {
		return errors.New("tables is required")
	}
	for i, selector := range opt.ColumnSelectors {
		if len(selector.Matcher) == 0 {
			return errors.Errorf("column-selectors[%d].matcher is required", i)
		}
		if len(selector.Columns) == 0 {
			return errors.Errorf("column-selectors[%d].columns is required", i)
		}
	}
	if opt.ChangefeedRCU < 0 {
		return errors.New("changefeed.rcu must be greater than 0")
	}
	if opt.SnapshotTSO != "" {
		tso, err := strconv.ParseUint(opt.SnapshotTSO, 10, 64)
		if err != nil {
			return errors.Annotate(err, "parse snapshot.tso")
		}
		if tso == 0 {
			return errors.New("snapshot.tso must be greater than 0")
		}
	}
	// TiDB Cloud API credentials are only required when an export/changefeed has
	// to be created (i.e. the snapshot/increment data does not already exist), so
	// they are validated lazily during the run rather than here. This lets a user
	// load a pre-created snapshot/changefeed without API keys.
	opt.logSummary()
	return nil
}

func (opt *Option) logSummary() {
	storage := opt.StoragePath
	if uri, err := url.Parse(opt.StoragePath); err == nil {
		uri.RawQuery = ""
		storage = uri.String()
	}

	log.Info("replication options validated",
		zap.Int("tableCount", len(opt.Tables)),
		zap.Int("columnSelectorCount", len(opt.ColumnSelectors)),
		zap.String("mode", opt.Mode),
		zap.String("sourceMode", opt.SourceMode),
		zap.String("storage", storage),
		zap.String("snapshotCompression", opt.SnapshotCompression),
		zap.Duration("changefeedFlushInterval", opt.ChangefeedFlushInterval),
		zap.Int("changefeedFileSizeMiB", opt.ChangefeedFileSizeMiB),
		zap.Int("changefeedRCU", opt.ChangefeedRCU),
		zap.Duration("incrementScanInterval", opt.IncrementScanInterval),
		zap.Bool("tidbCloudConfigured", opt.TiDBCloudClusterID != ""),
		zap.String("ticdcAddress", opt.TiCDCAddress),
		zap.String("snowflakeDatabase", opt.SnowflakeDatabase))
}
