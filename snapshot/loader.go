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
	StorageDir  string
	Compression string
}

type snapshotConnector interface {
	CopyTableSchema(*table.Meta) error
	LoadSnapshot(targetTable, filePath string) error
}

type snapshotFile struct {
	tableFQN    string
	targetTable string
	path        string
}

func Load(ctx context.Context, cfg Config, store storeapi.Storage) error {
	if cfg.StorageURI == nil {
		return errors.New("snapshot storage URI is empty")
	}
	if store == nil {
		return errors.New("snapshot storage is empty")
	}

	log.Info("starting Snowflake snapshot load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.Int("concurrency", concurrency))

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

	if err := loadSnapshotFiles(ctx, cfg, store, tables, conn); err != nil {
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
) (map[string]snapshotFile, error) {
	tables := make(map[string]snapshotFile, len(cfg.Tables))

	for _, tableFQN := range cfg.Tables {
		sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
		schemaFilePath := path.Join(cfg.StorageDir, table.SchemaFilePath(sourceDatabase, sourceTable))
		schemaSQL, err := store.ReadFile(ctx, schemaFilePath)
		if err != nil {
			return nil, errors.Annotatef(err, "read snapshot schema file %s", schemaFilePath)
		}
		tableSchema := table.BuildSchema(sourceDatabase, sourceTable, string(schemaSQL))
		if len(tableSchema.PrimaryKeys) == 0 {
			return nil, errors.Errorf("table %s has no primary key", tableFQN)
		}
		if err := conn.CopyTableSchema(tableSchema); err != nil {
			return nil, errors.Trace(err)
		}

		prefix := fmt.Sprintf("%s.%s.", sourceDatabase, sourceTable)
		tables[prefix] = snapshotFile{
			tableFQN:    tableFQN,
			targetTable: tableSchema.SnowflakeTableName(),
		}
	}
	return tables, nil
}

func loadSnapshotFiles(ctx context.Context, cfg Config, store storeapi.Storage, tables map[string]snapshotFile, conn snapshotConnector) error {
	tasks := make(chan snapshotFile, concurrency)
	g, ctx := errgroup.WithContext(ctx)

	for range concurrency {
		g.Go(func() error {
			return runSnapshotWorker(ctx, tasks, conn)
		})
	}

	scanErr := scanSnapshotFiles(ctx, cfg, store, tables, tasks)
	close(tasks)
	waitErr := g.Wait()
	if waitErr != nil {
		return errors.Trace(waitErr)
	}
	return errors.Trace(scanErr)
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
			if err := conn.LoadSnapshot(task.targetTable, task.path); err != nil {
				metrics.AddCounter(metrics.ErrorCounter, 1, task.tableFQN)
				log.Error("snapshot load failed",
					zap.String("table", task.tableFQN),
					zap.String("file", task.path),
					zap.Error(err))
				return errors.Trace(err)
			}
		}
	}
}

func scanSnapshotFiles(
	ctx context.Context,
	cfg Config,
	store storeapi.Storage,
	tables map[string]snapshotFile,
	tasks chan<- snapshotFile,
) error {
	return store.WalkDir(ctx, &storeapi.WalkOption{SubDir: cfg.StorageDir}, func(filePath string, _ int64) error {
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

func snapshotTaskForFile(tables map[string]snapshotFile, filePath string) (snapshotFile, bool) {
	name := path.Base(filePath)
	if !strings.Contains(name, csvFileExtension) {
		return snapshotFile{}, false
	}
	for prefix, table := range tables {
		if strings.HasPrefix(name, prefix) {
			table.path = filePath
			return table, true
		}
	}
	return snapshotFile{}, false
}
