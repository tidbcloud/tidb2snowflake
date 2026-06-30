package snapshot

import (
	"context"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/workerpool"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

type Config struct {
	Credential  *credentials.Value
	Tables      []string
	StorageURI  *url.URL
	StorageDir  string
	Compression string
}

type loadTask struct {
	targetTable string
	filePath    string
}

const maxInFlightSnapshotFiles = workerpool.DefaultConcurrency * 4

func Load(ctx context.Context, cfg Config, store *storage.Storage, pool *workerpool.Pool, conn *snowflake.Connector) error {
	log.Info("starting Snowflake snapshot load phase",
		zap.Int("tableCount", len(cfg.Tables)))

	if err := conn.CreateStage(ctx, snowflake.SnapshotStageName, cfg.StorageURI, cfg.Credential); err != nil {
		return errors.Trace(err)
	}
	defer conn.DropStage(context.WithoutCancel(ctx), snowflake.SnapshotStageName)

	if err := prepareTables(ctx, cfg, store, conn, pool); err != nil {
		return errors.Trace(err)
	}

	if err := loadFiles(ctx, cfg, store, conn, pool); err != nil {
		return errors.Trace(err)
	}

	log.Info("Snowflake snapshot load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func prepareTables(
	ctx context.Context,
	cfg Config,
	store *storage.Storage,
	conn *snowflake.Connector,
	pool *workerpool.Pool,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	futures := make([]*workerpool.Future, 0, len(cfg.Tables))
	var first error
	for _, tableFQN := range cfg.Tables {
		future, err := pool.Submit(ctx, workerpool.TaskFunc(func(ctx context.Context) error {
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
			if err := conn.CreateTable(ctx, tableSchema); err != nil {
				return errors.Trace(err)
			}
			return nil
		}))
		if err != nil {
			first = err
			cancel()
			break
		}
		futures = append(futures, future)
	}

	for _, future := range futures {
		if err := future.Wait(); err != nil && first == nil {
			first = err
			cancel()
		}
	}
	if first != nil {
		log.Error("snapshot prepare tables failed", zap.Duration("duration", time.Since(start)), zap.Error(first))
		return errors.Trace(first)
	}
	log.Info("snapshot prepare tables finished", zap.Duration("duration", time.Since(start)))
	return nil
}

func loadFiles(ctx context.Context, cfg Config, store *storage.Storage, conn *snowflake.Connector, pool *workerpool.Pool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	futures := make([]*workerpool.Future, 0, maxInFlightSnapshotFiles)
	configuredTables := make(map[string]struct{}, len(cfg.Tables))
	for _, table := range cfg.Tables {
		configuredTables[table] = struct{}{}
	}
	var first error
	fileCount := 0
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: cfg.StorageDir}, func(filePath string, _ int64) error {
		task, ok := snapshotTaskForFile(filePath)
		if !ok {
			return nil
		}
		if _, ok := configuredTables[task.targetTable]; !ok {
			return nil
		}
		future, err := pool.Submit(ctx, workerpool.TaskFunc(func(ctx context.Context) error {
			if err := conn.LoadSnapshot(ctx, task.targetTable, task.filePath, cfg.Compression); err != nil {
				metrics.AddCounter(metrics.ErrorCounter, 1, task.targetTable)
				log.Error("snapshot load failed",
					zap.String("table", task.targetTable),
					zap.String("file", task.filePath),
					zap.Error(err))
				return errors.Trace(err)
			}
			return nil
		}))
		if err != nil {
			return errors.Trace(err)
		}
		futures = append(futures, future)
		fileCount++
		if len(futures) >= maxInFlightSnapshotFiles {
			for _, future := range futures {
				if err := future.Wait(); err != nil && first == nil {
					first = err
					cancel()
				}
			}
			futures = futures[:0]
			if first != nil {
				return errors.Trace(first)
			}
		}
		return nil
	})
	if err != nil && first == nil {
		first = err
		cancel()
	}

	for _, future := range futures {
		if err := future.Wait(); err != nil && first == nil {
			first = err
			cancel()
		}
	}
	if first != nil {
		log.Error("snapshot load files failed", zap.Duration("duration", time.Since(start)), zap.Error(first))
		return errors.Trace(first)
	}
	log.Info("snapshot load files finished", zap.Int("fileCount", fileCount), zap.Duration("duration", time.Since(start)))
	return nil
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
