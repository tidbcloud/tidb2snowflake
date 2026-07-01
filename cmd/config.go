package cmd

import (
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/logger"
)

type configFile struct {
	Mode   string   `toml:"mode"`
	Source string   `toml:"source"`
	Tables []string `toml:"tables"`

	Storage    storageConfig    `toml:"storage"`
	TiDB       tidbConfig       `toml:"tidb"`
	TiCDC      ticdcConfig      `toml:"ticdc"`
	Snowflake  snowflakeConfig  `toml:"snowflake"`
	Snapshot   snapshotConfig   `toml:"snapshot"`
	Changefeed changefeedConfig `toml:"changefeed"`
	Increment  incrementConfig  `toml:"increment"`
	Log        logConfig        `toml:"log"`
}

type storageConfig struct {
	URI       string `toml:"uri"`
	AccessKey string `toml:"access-key"`
	SecretKey string `toml:"secret-key"`
}

type tidbConfig struct {
	Host  string `toml:"host"`
	Port  int    `toml:"port"`
	User  string `toml:"user"`
	Pass  string `toml:"pass"`
	TLS   bool   `toml:"tls"`
	SSLCA string `toml:"ssl-ca"`
}

type ticdcConfig struct {
	Address string `toml:"address"`
}

type snowflakeConfig struct {
	AccountID string `toml:"account-id"`
	User      string `toml:"user"`
	Pass      string `toml:"pass"`
	Warehouse string `toml:"warehouse"`
	Database  string `toml:"database"`
}

type snapshotConfig struct {
	TSO         string `toml:"tso"`
	Compression string `toml:"compression"`
	Concurrency int    `toml:"concurrency"`
}

type changefeedConfig struct {
	FlushInterval string `toml:"flush-interval"`
	FileSizeMiB   int    `toml:"file-size"`
}

type incrementConfig struct {
	ScanInterval string `toml:"scan-interval"`
}

type logConfig struct {
	Level string `toml:"level"`
	File  string `toml:"file"`
}

func loadConfig(path string) (*Option, *logger.Config, error) {
	var cfg configFile
	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		var b strings.Builder
		for i, item := range undecoded {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(item.String())
		}
		return nil, nil, errors.Errorf("config file %s contained unknown configuration options: %s", path, b.String())
	}

	opt := NewOption()
	opt.Mode = cfg.Mode
	opt.SourceMode = cfg.Source
	opt.Tables = cfg.Tables

	opt.StoragePath = cfg.Storage.URI
	opt.AWSAccessKey = cfg.Storage.AccessKey
	opt.AWSSecretKey = cfg.Storage.SecretKey

	opt.TiDBHost = cfg.TiDB.Host
	opt.TiDBPort = cfg.TiDB.Port
	opt.TiDBUser = cfg.TiDB.User
	opt.TiDBPass = cfg.TiDB.Pass
	opt.TiDBTLS = cfg.TiDB.TLS
	opt.TiDBSSLCA = cfg.TiDB.SSLCA

	opt.TiCDCAddress = cfg.TiCDC.Address

	opt.SnowflakeAccountID = cfg.Snowflake.AccountID
	opt.SnowflakeUser = cfg.Snowflake.User
	opt.SnowflakePass = cfg.Snowflake.Pass
	opt.SnowflakeWarehouse = cfg.Snowflake.Warehouse
	opt.SnowflakeDatabase = cfg.Snowflake.Database

	opt.SnapshotTSO = cfg.Snapshot.TSO
	opt.SnapshotCompression = cfg.Snapshot.Compression
	opt.SnapshotConcurrency = cfg.Snapshot.Concurrency

	flushInterval, err := parseConfigDuration(cfg.Changefeed.FlushInterval, "changefeed.flush-interval")
	if err != nil {
		return nil, nil, err
	}
	opt.ChangefeedFlushInterval = flushInterval
	opt.ChangefeedFileSizeMiB = cfg.Changefeed.FileSizeMiB

	scanInterval, err := parseConfigDuration(cfg.Increment.ScanInterval, "increment.scan-interval")
	if err != nil {
		return nil, nil, err
	}
	opt.IncrementScanInterval = scanInterval

	logCfg := &logger.Config{Level: cfg.Log.Level, File: cfg.Log.File}
	if strings.TrimSpace(logCfg.Level) == "" {
		logCfg.Level = "info"
	}
	return opt, logCfg, nil
}

func parseConfigDuration(value, field string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, errors.Annotatef(err, "parse %s", field)
	}
	return duration, nil
}
