package dumpling

import (
	"context"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

type Config struct {
	Concurrency  int
	StorageURI   *url.URL
	SnapshotTSO  string
	Tables       []string
	Compression  string
	ReadTimeout  time.Duration
	CSVNullValue string
	OnProgress   func(dumpedRows, totalRows int64)
}

func buildConfig(tidbCfg *tidb.Config, cfg Config) (*export.Config, error) {
	conf := export.DefaultConfig()
	conf.Logger = log.L()
	conf.User = tidbCfg.User
	conf.Password = tidbCfg.Pass
	conf.Host = tidbCfg.Host
	conf.Port = tidbCfg.Port
	conf.Security.CAPath = tidbCfg.SSLCA

	conf.Threads = cfg.Concurrency
	conf.NoHeader = true
	conf.FileType = "csv"
	conf.CsvSeparator = ","
	conf.CsvDelimiter = "\""
	conf.CsvNullValue = cfg.CSVNullValue
	conf.EscapeBackslash = false
	conf.TransactionalConsistency = true
	conf.CsvOutputDialect = export.CSVDialectSnowflake
	conf.Rows = 1

	if cfg.StorageURI == nil {
		return nil, errors.New("storage uri is required")
	}
	conf.OutputDirPath = cfg.StorageURI.String()
	conf.ReadTimeout = cfg.ReadTimeout
	if cfg.SnapshotTSO != "" {
		if _, err := strconv.ParseUint(cfg.SnapshotTSO, 10, 64); err != nil {
			return nil, errors.Annotate(err, "parse snapshot tso")
		}
		conf.Snapshot = cfg.SnapshotTSO
	}

	compressType, err := compressedio.ParseCompressType(dumplingCompression(cfg.Compression))
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.CompressType = compressType

	conf.SpecifiedTables = true
	tables, err := export.GetConfTables(cfg.Tables)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.Tables = tables
	return conf, nil
}

func dumplingCompression(compression string) string {
	switch compression {
	case "none":
		return "no-compression"
	case "gzip":
		return "gzip"
	default:
		log.Panic("unknown snapshot compression", zap.String("compression", compression))
		return ""
	}
}

func Run(ctx context.Context, tidbCfg *tidb.Config, cfg Config) error {
	dumpConfig, err := buildConfig(tidbCfg, cfg)
	if err != nil {
		return errors.Trace(err)
	}

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
				case <-ctx.Done():
					return
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

func LoadTSOFromMetadata(ctx context.Context, store storeapi.Storage) (uint64, error) {
	metadataPath := path.Join(storage.SnapshotDirName, "metadata")
	exists, err := store.FileExists(ctx, metadataPath)
	if err != nil {
		return 0, errors.Annotatef(err, "check snapshot metadata %s", metadataPath)
	}
	if !exists {
		return 0, errors.Errorf("%s is missing", metadataPath)
	}
	data, err := store.ReadFile(ctx, metadataPath)
	if err != nil {
		return 0, errors.Annotatef(err, "read snapshot metadata %s", metadataPath)
	}
	tso := tsoFromMetadata(data)
	return strconv.ParseUint(tso, 10, 64)
}

func tsoFromMetadata(data []byte) string {
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Pos:") {
			continue
		}
		return strings.TrimSpace(strings.TrimPrefix(line, "Pos:"))
	}
	log.Panic("metadata Pos is missing")
	return ""
}
