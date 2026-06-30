package snapshot

import (
	"context"
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
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const concurrency = 8

type Config struct {
	Snowflake   *snowflake.Config
	Credential  *credentials.Value
	Tables      []string
	StorageURI  *url.URL
	StorageDir  string
	Compression string
}

type snapshotConnector interface {
	CreateTable(*table.Meta) error
	LoadSnapshot(targetTable, filePath string) error
}

type loadTask struct {
	targetTable string
	filePath    string
}

func Load(ctx context.Context, cfg Config, store *storage.Storage) error {
	log.Info("starting Snowflake snapshot load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.Int("concurrency", concurrency))

	conn, err := snowflake.NewConnector(
		cfg.Snowflake,
		snowflake.SnapshotStageName,
		cfg.StorageURI,
		cfg.Credential,
		cfg.Compression,
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	if err := createTables(ctx, cfg, store, conn); err != nil {
		return errors.Trace(err)
	}

	if err := loadFiles(ctx, cfg, store, conn); err != nil {
		return errors.Trace(err)
	}

	log.Info("Snowflake snapshot load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func createTables(
	ctx context.Context,
	cfg Config,
	store *storage.Storage,
	conn snapshotConnector,
) error {
	for _, tableFQN := range cfg.Tables {
		sourceDatabase, sourceTable, ok := strings.Cut(tableFQN, ".")
		if !ok {
			return errors.Errorf("invalid source database and table name, %s", tableFQN)
		}
		schemaFilePath := path.Join(cfg.StorageDir, table.SchemaFilePath(sourceDatabase, sourceTable))
		schemaSQL, err := store.ReadFile(ctx, schemaFilePath)
		if err != nil {
			return errors.Annotatef(err, "read snapshot schema file %s", schemaFilePath)
		}
		tableSchema := table.BuildSchema(sourceDatabase, sourceTable, string(schemaSQL))
		if len(tableSchema.PrimaryKeys) == 0 {
			return errors.Errorf("table %s has no primary key", tableFQN)
		}
		if err := conn.CreateTable(tableSchema); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func loadFiles(ctx context.Context, cfg Config, store *storage.Storage, conn snapshotConnector) error {
	tasks := make(chan loadTask, concurrency)
	g, ctx := errgroup.WithContext(ctx)

	for range concurrency {
		g.Go(func() error {
			return runWorker(ctx, tasks, conn)
		})
	}

	err := scanFiles(ctx, cfg, store, tasks)
	close(tasks)
	if err != nil {
		return errors.Trace(err)
	}
	return g.Wait()
}

func runWorker(ctx context.Context, tasks <-chan loadTask, conn snapshotConnector) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-tasks:
			if !ok {
				return nil
			}
			if err := conn.LoadSnapshot(task.targetTable, task.filePath); err != nil {
				metrics.AddCounter(metrics.ErrorCounter, 1, task.targetTable)
				log.Error("snapshot load failed",
					zap.String("table", task.targetTable),
					zap.String("file", task.filePath),
					zap.Error(err))
				return errors.Trace(err)
			}
		}
	}
}

func scanFiles(
	ctx context.Context,
	cfg Config,
	store *storage.Storage,
	tasks chan<- loadTask,
) error {
	return store.WalkDir(ctx, &storeapi.WalkOption{SubDir: cfg.StorageDir}, func(filePath string, _ int64) error {
		task, ok := snapshotTaskForFile(filePath)
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

func snapshotTaskForFile(filePath string) (loadTask, bool) {
	targetTable, ok := targetTableFromFile(path.Base(filePath))
	if !ok {
		return loadTask{}, false
	}
	return loadTask{
		targetTable: targetTable,
		filePath:    filePath,
	}, true
}

func targetTableFromFile(name string) (string, bool) {
	name = strings.TrimSuffix(name, ".gz")
	if !strings.HasSuffix(name, storage.CSVFileExtension) {
		return "", false
	}

	stem := strings.TrimSuffix(name, storage.CSVFileExtension)
	schema, rest, ok := strings.Cut(stem, ".")
	if !ok {
		return "", false
	}
	table, _, _ := strings.Cut(rest, ".")
	if schema == "" || table == "" {
		return "", false
	}
	return schema + "." + table, true
}
