package dumpling

import (
	"context"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"go.uber.org/zap"
)

type Config struct {
	Concurrency  int
	StorageURI   *url.URL
	SnapshotTSO  string
	Tables       []string
	Compression  string
	ReadTimeout  time.Duration
	FileSize     string
	CSVNullValue string
	OnProgress   func(dumpedRows, totalRows int64)
}

func BuildConfig(ctx context.Context, tidbCfg *tidb.Config, cfg Config) (*export.Config, error) {
	if tidbCfg == nil {
		return nil, errors.New("tidb config is required")
	}
	if cfg.StorageURI == nil {
		return nil, errors.New("storage uri is required")
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 8
	}
	fileSize := cfg.FileSize
	if fileSize == "" {
		fileSize = "5GiB"
	}
	csvNullValue := cfg.CSVNullValue
	if csvNullValue == "" {
		csvNullValue = "\\N"
	}
	compression := cfg.Compression
	if compression == "" || compression == "none" {
		compression = "no-compression"
	}

	conf := export.DefaultConfig()
	conf.Logger = log.L()
	conf.User = tidbCfg.User
	conf.Password = tidbCfg.Pass
	conf.Host = tidbCfg.Host
	conf.Port = tidbCfg.Port
	conf.Threads = concurrency
	conf.NoHeader = true
	conf.FileType = "csv"
	conf.CsvSeparator = ","
	conf.CsvDelimiter = "\""
	conf.CsvNullValue = csvNullValue
	conf.EscapeBackslash = false
	conf.TransactionalConsistency = true
	conf.CsvOutputDialect = export.CSVDialectSnowflake
	conf.Rows = 1
	conf.OutputDirPath = cfg.StorageURI.String()
	conf.ReadTimeout = cfg.ReadTimeout
	conf.Security.CAPath = tidbCfg.SSLCA
	if cfg.SnapshotTSO != "" && cfg.SnapshotTSO != "0" {
		conf.Snapshot = cfg.SnapshotTSO
	}

	compressType, err := compressedio.ParseCompressType(compression)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.CompressType = compressType

	parsedFileSize, err := export.ParseFileSize(fileSize)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.FileSize = parsedFileSize

	conf.SpecifiedTables = true
	tables, err := export.GetConfTables(cfg.Tables)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.Tables = tables

	externalStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, cfg.StorageURI.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.ExtStorage = externalStorage

	return conf, nil
}

func Run(ctx context.Context, tidbCfg *tidb.Config, cfg Config) error {
	dumpConfig, err := BuildConfig(ctx, tidbCfg, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := tidb.OpenDB(tidbCfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	dumper, err := export.NewDumper(ctx, dumpConfig)
	if err != nil {
		return errors.Annotate(err, "create dumpling instance")
	}
	defer func() {
		_ = dumper.Close()
	}()

	done := make(chan struct{})
	if cfg.OnProgress != nil {
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					status := dumper.GetStatus()
					cfg.OnProgress(int64(status.FinishedRows), int64(status.EstimateTotalRows))
				}
			}
		}()
	}

	err = dumper.Dump()
	close(done)
	if err != nil {
		return errors.Annotate(err, "dump tables from TiDB")
	}
	status := dumper.GetStatus()
	log.Info("snapshot dumped from TiDB", zap.Any("status", status))
	return nil
}
