package incremental

import (
	"context"
	"fmt"
	"net/url"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const concurrency = 8

type Config struct {
	Snowflake    *snowflake.Config
	Credential   *credentials.Value
	Tables       []string
	StorageURI   *url.URL
	ScanInterval time.Duration
}

type tableSession struct {
	tableFQN string
	sync     func() error
	close    func()
}

func Load(ctx context.Context, cfg Config) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting Snowflake incremental load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.Int("concurrency", concurrency),
		zap.Duration("scanInterval", cfg.ScanInterval))

	sessions := make([]tableSession, 0, len(cfg.Tables))
	defer closeSessions(sessions)
	for _, tableFQN := range cfg.Tables {
		session, err := newTableSession(ctx, cfg, tableFQN)
		if err != nil {
			metrics.AddCounter(metrics.ErrorCounter, 1, tableFQN)
			log.Error("incremental session setup failed", zap.String("table", tableFQN), zap.Error(err))
			return errors.Trace(err)
		}
		sessions = append(sessions, session)
	}
	if err := runSessions(ctx, cfg.ScanInterval, sessions); err != nil {
		return errors.Trace(err)
	}

	log.Info("Snowflake incremental load phase finished", zap.Int("tableCount", len(cfg.Tables)))
	return nil
}

func runSessions(ctx context.Context, scanInterval time.Duration, sessions []tableSession) error {
	if len(sessions) == 0 {
		return nil
	}

	workerCount := concurrency
	if workerCount > len(sessions) {
		workerCount = len(sessions)
	}
	g, ctx := errgroup.WithContext(ctx)
	for workerID := range workerCount {
		workerID := workerID
		g.Go(func() error {
			ticker := time.NewTicker(scanInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
				for i := workerID; i < len(sessions); i += workerCount {
					session := sessions[i]
					if err := session.sync(); err != nil {
						metrics.AddCounter(metrics.ErrorCounter, 1, session.tableFQN)
						log.Error("incremental load failed", zap.String("table", session.tableFQN), zap.Error(err))
						return err
					}
				}
			}
		})
	}
	if err := g.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func newTableSession(ctx context.Context, cfg Config, tableFQN string) (tableSession, error) {
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	stageName := fmt.Sprintf("increment_external_%s_%s", sourceDatabase, sourceTable)

	log.Info("creating incremental session for table",
		zap.String("table", tableFQN),
		zap.String("stage", stageName),
		zap.Duration("scanInterval", cfg.ScanInterval))
	conn, err := snowflake.NewConnector(
		cfg.Snowflake,
		stageName,
		cfg.StorageURI,
		cfg.Credential,
	)
	if err != nil {
		return tableSession{}, errors.Trace(err)
	}
	metrics.AddGauge(metrics.IncrementPendingSizeGauge, 0, tableFQN)
	logger := log.L().With(zap.String("table", tableFQN))
	session, err := NewIncrementReplicateSession(ctx, conn, CSVFileExtension, cfg.StorageURI, tableFQN, logger)
	if err != nil {
		conn.Close()
		return tableSession{}, errors.Trace(err)
	}
	return tableSession{
		tableFQN: tableFQN,
		sync: func() error {
			dmlFileMap, err := session.getNewFiles()
			if err != nil {
				return errors.Trace(err)
			}
			if err = session.handleNewFiles(dmlFileMap); err != nil {
				return errors.Trace(err)
			}
			return nil
		},
		close: conn.Close,
	}, nil
}

func closeSessions(sessions []tableSession) {
	for _, session := range sessions {
		if session.close != nil {
			session.close()
		}
	}
}
