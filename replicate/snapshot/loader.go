package snapshot

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const concurrency = 8
const csvFileExtension = ".csv"

type Config struct {
	Snowflake   *snowflake.Config
	Credential  *credentials.Value
	Tables      []string
	StorageURI  *url.URL
	Compression string
}

type snapshotConnector interface {
	CopyTableSchema(*table.Meta) error
	LoadSnapshot(targetTable, filePath string) error
}

type snapshotTable struct {
	tableFQN    string
	targetTable string
}

type snapshotFile struct {
	table snapshotTable
	path  string
}

func Load(ctx context.Context, cfg Config) error {
	if cfg.StorageURI == nil {
		return errors.New("snapshot storage URI is empty")
	}

	log.Info("starting Snowflake snapshot load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.Int("concurrency", concurrency))

	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, cfg.StorageURI.String())
	if err != nil {
		return errors.Trace(err)
	}
	defer store.Close()

	conn, err := snowflake.NewConnector(
		cfg.Snowflake,
		"snapshot_external",
		cfg.StorageURI,
		cfg.Credential,
		snowflake.WithStageFileCompression(cfg.Compression),
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	tables, err := prepareSnapshotTables(ctx, cfg, store, conn)
	if err != nil {
		return errors.Trace(err)
	}

	if err := loadSnapshotFiles(ctx, store, tables, conn); err != nil {
		return errors.Trace(err)
	}

	log.Info("Snowflake snapshot load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func prepareSnapshotTables(
	ctx context.Context,
	cfg Config,
	store storeapi.Storage,
	conn snapshotConnector,
) (map[string]snapshotTable, error) {
	tables := make(map[string]snapshotTable, len(cfg.Tables))

	for _, tableFQN := range cfg.Tables {
		sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
		schemaFilePath := table.SchemaFilePath(sourceDatabase, sourceTable)
		schemaSQL, err := store.ReadFile(ctx, schemaFilePath)
		if err != nil {
			return nil, errors.Annotatef(err, "read snapshot schema file %s", schemaFilePath)
		}
		tableSchema := table.BuildSchema(sourceDatabase, sourceTable, string(schemaSQL))
		if err := conn.CopyTableSchema(tableSchema); err != nil {
			return nil, errors.Trace(err)
		}

		prefix := fmt.Sprintf("%s.%s.", sourceDatabase, sourceTable)
		tables[prefix] = snapshotTable{
			tableFQN:    tableFQN,
			targetTable: sourceTable,
		}
	}
	return tables, nil
}

func loadSnapshotFiles(ctx context.Context, store storeapi.Storage, tables map[string]snapshotTable, conn snapshotConnector) error {
	tasks := make(chan snapshotFile, concurrency)
	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			return runSnapshotWorker(ctx, tasks, conn)
		})
	}

	g.Go(func() error {
		defer close(tasks)
		return scanSnapshotFiles(ctx, store, tables, tasks)
	})

	return errors.Trace(g.Wait())
}

func runSnapshotWorker(ctx context.Context, tasks <-chan snapshotFile, conn snapshotConnector) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-tasks:
			if !ok {
				return nil
			}
			if err := conn.LoadSnapshot(task.table.targetTable, task.path); err != nil {
				metrics.AddCounter(metrics.ErrorCounter, 1, task.table.tableFQN)
				log.Error("snapshot load failed",
					zap.String("table", task.table.tableFQN),
					zap.String("file", task.path),
					zap.Error(err))
				return errors.Trace(err)
			}
		}
	}
}

func scanSnapshotFiles(
	ctx context.Context,
	store storeapi.Storage,
	tables map[string]snapshotTable,
	tasks chan<- snapshotFile,
) error {
	return store.WalkDir(ctx, &storeapi.WalkOption{}, func(filePath string, _ int64) error {
		task, ok := snapshotTaskForFile(tables, filePath)
		if !ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tasks <- task:
			return nil
		}
	})
}

func snapshotTaskForFile(tables map[string]snapshotTable, filePath string) (snapshotFile, bool) {
	name := path.Base(filePath)
	if !strings.Contains(name, csvFileExtension) {
		return snapshotFile{}, false
	}
	for prefix, table := range tables {
		if strings.HasPrefix(name, prefix) {
			return snapshotFile{table: table, path: filePath}, true
		}
	}
	return snapshotFile{}, false
}
