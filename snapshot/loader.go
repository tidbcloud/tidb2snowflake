package snapshot

import (
	"context"
	"net/url"
	"path"
	"strings"
	"time"

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
	Tables      []string
	StorageDir  string
	Compression string
}

type loadTask struct {
	sourceDatabase string
	sourceTable    string
	filePath       string
}

const maxInFlightSnapshotFiles = workerpool.DefaultConcurrency * 4

func Load(ctx context.Context, cfg Config, store *storage.Storage, pool *workerpool.Pool, conn *snowflake.Connector) error {
	log.Info("starting Snowflake snapshot load phase",
		zap.Int("tableCount", len(cfg.Tables)))

	if err := prepareSchemas(ctx, cfg, store, conn); err != nil {
		return errors.Trace(err)
	}

	if err := prepareTables(ctx, cfg, store, conn, pool); err != nil {
		return errors.Trace(err)
	}

	if err := loadFiles(ctx, cfg, store, conn, pool); err != nil {
		return errors.Trace(err)
	}

	log.Info("Snowflake snapshot load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func prepareSchemas(ctx context.Context, cfg Config, store *storage.Storage, conn *snowflake.Connector) error {
	configured := configuredSourceDatabases(cfg.Tables)
	if len(configured) == 0 {
		return nil
	}

	start := time.Now()
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: cfg.StorageDir}, func(filePath string, _ int64) error {
		database, ok := sourceDatabaseFromSchemaCreateFile(path.Base(filePath))
		if !ok {
			return nil
		}
		if _, ok := configured[database]; !ok {
			return nil
		}
		if err := conn.CreateSchema(ctx, database); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		log.Error("snapshot prepare schemas failed", zap.Duration("duration", time.Since(start)), zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("snapshot prepare schemas finished", zap.Duration("duration", time.Since(start)))
	return nil
}

func prepareTables(
	ctx context.Context,
	cfg Config,
	store *storage.Storage,
	conn *snowflake.Connector,
	pool *workerpool.Pool,
) error {
	start := time.Now()
	var first error

	group := pool.NewGroup(ctx, 0)
	for _, tableFQN := range cfg.Tables {
		if err := group.Submit(workerpool.TaskFunc(func(ctx context.Context) error {
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
		})); err != nil {
			first = err
			break
		}
	}

	if err := group.Wait(); err != nil && first == nil {
		first = err
	}
	if first != nil {
		log.Error("snapshot prepare tables failed", zap.Duration("duration", time.Since(start)), zap.Error(first))
		return errors.Trace(first)
	}

	log.Info("snapshot prepare tables finished", zap.Duration("duration", time.Since(start)))
	return nil
}

func configuredSourceDatabases(tables []string) map[string]struct{} {
	sourceDatabases := make(map[string]struct{})
	for _, tableFQN := range tables {
		sourceDatabase, _, ok := strings.Cut(tableFQN, ".")
		if ok && sourceDatabase != "" {
			sourceDatabases[sourceDatabase] = struct{}{}
		}
	}
	return sourceDatabases
}

func loadFiles(ctx context.Context, cfg Config, store *storage.Storage, conn *snowflake.Connector, pool *workerpool.Pool) error {
	start := time.Now()

	configuredTables := make(map[string]struct{}, len(cfg.Tables))
	for _, table := range cfg.Tables {
		configuredTables[table] = struct{}{}
	}

	var (
		first     error
		fileCount int
	)

	group := pool.NewGroup(ctx, maxInFlightSnapshotFiles)
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: cfg.StorageDir}, func(filePath string, _ int64) error {
		task, ok := snapshotTaskForFile(filePath)
		if !ok {
			return nil
		}
		tableFQN := task.tableFQN()
		if _, ok := configuredTables[tableFQN]; !ok {
			return nil
		}
		if err := group.Submit(workerpool.TaskFunc(func(ctx context.Context) error {
			if err := conn.LoadSnapshot(ctx, task.sourceDatabase, task.sourceTable, task.filePath, cfg.Compression); err != nil {
				metrics.AddCounter(metrics.ErrorCounter, 1, tableFQN)
				log.Error("snapshot load failed",
					zap.String("table", tableFQN),
					zap.String("file", task.filePath),
					zap.Error(err))
				return errors.Trace(err)
			}
			return nil
		})); err != nil {
			return errors.Trace(err)
		}
		fileCount++
		return nil
	})
	if err != nil && first == nil {
		first = err
		group.Cancel()
	}

	if err := group.Wait(); err != nil && first == nil {
		first = err
	}
	if first != nil {
		log.Error("snapshot load files failed", zap.Duration("duration", time.Since(start)), zap.Error(first))
		return errors.Trace(first)
	}
	log.Info("snapshot load files finished", zap.Int("fileCount", fileCount), zap.Duration("duration", time.Since(start)))
	return nil
}

func snapshotTaskForFile(filePath string) (loadTask, bool) {
	sourceDatabase, sourceTable, ok := sourceTableFromFile(path.Base(filePath))
	if !ok {
		return loadTask{}, false
	}
	return loadTask{
		sourceDatabase: sourceDatabase,
		sourceTable:    sourceTable,
		filePath:       filePath,
	}, true
}

func (task loadTask) tableFQN() string {
	return task.sourceDatabase + "." + task.sourceTable
}

func sourceDatabaseFromSchemaCreateFile(name string) (string, bool) {
	if !strings.HasSuffix(name, table.SchemaCreateFileSuffix) {
		return "", false
	}
	escaped := strings.TrimSuffix(name, table.SchemaCreateFileSuffix)
	if escaped == "" {
		return "", false
	}
	database, err := url.PathUnescape(escaped)
	if err != nil || database == "" {
		return "", false
	}
	return database, true
}

func sourceTableFromFile(name string) (string, string, bool) {
	name = strings.TrimSuffix(name, ".gz")
	if !strings.HasSuffix(name, storage.CSVFileExtension) {
		return "", "", false
	}

	stem := strings.TrimSuffix(name, storage.CSVFileExtension)
	schema, rest, ok := strings.Cut(stem, ".")
	if !ok {
		return "", "", false
	}
	table, _, _ := strings.Cut(rest, ".")
	if schema == "" || table == "" {
		return "", "", false
	}
	sourceDatabase, err := url.PathUnescape(schema)
	if err != nil || sourceDatabase == "" {
		return "", "", false
	}
	sourceTable, err := url.PathUnescape(table)
	if err != nil || sourceTable == "" {
		return "", "", false
	}
	return sourceDatabase, sourceTable, true
}
