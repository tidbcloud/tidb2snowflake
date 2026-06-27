package op

import (
	"context"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/dumpling"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/ticdc"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/source/snapshot"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

type Config struct {
	TiDB                    *tidb.Config
	TiCDCAddress            string
	SnapshotConcurrency     int
	Tables                  []string
	ChangefeedFlushInterval time.Duration
	ChangefeedFileSizeMiB   int
	SnapshotCompression     string

	SnapshotURI  *url.URL
	IncrementURI *url.URL
}

type Runner struct {
	cfg    Config
	client *ticdc.Client

	store storeapi.Storage
	state state.Manager
}

func NewRunner(cfg Config, store storeapi.Storage, state state.Manager) *Runner {
	return &Runner{
		cfg:   cfg,
		store: store,
		state: state,
	}
}

func (r *Runner) EnsureSnapshot(ctx context.Context) error {
	if r.state.Snapshot().Snapshot.Finished {
		if err := snapshot.LoadExistingTSO(ctx, r.store, r.state); err != nil {
			return errors.Trace(err)
		}
		log.Info("snapshot already marked finished in state, skipping OP Dumpling snapshot dump")
		return nil
	}

	snapshotExists, err := storage.DirHasObjects(ctx, r.store, storage.SnapshotDirName)
	if err != nil {
		return errors.Annotate(err, "check snapshot directory")
	}
	if snapshotExists {
		if err := snapshot.LoadTSOFromMetadata(ctx, r.store, r.state); err != nil {
			return errors.Trace(err)
		}
		log.Info("snapshot data already exists in storage, skipping OP Dumpling snapshot dump",
			zap.String("dir", storage.SnapshotDirName))
		return nil
	}

	log.Info("dumping OP TiDB snapshot with Dumpling",
		zap.String("snapshotTSO", r.state.Snapshot().Snapshot.TSO),
		zap.Int("concurrency", r.cfg.SnapshotConcurrency))
	if err := dumpling.Run(ctx, r.cfg.TiDB, dumpling.Config{
		Concurrency:  r.cfg.SnapshotConcurrency,
		StorageURI:   r.cfg.SnapshotURI,
		SnapshotTSO:  r.state.Snapshot().Snapshot.TSO,
		Tables:       r.cfg.Tables,
		Compression:  r.cfg.SnapshotCompression,
		CSVNullValue: "\\N",
		OnProgress: func(dumpedRows, totalRows int64) {
			log.Info("OP Dumpling snapshot dump progress",
				zap.Int64("dumpedRows", dumpedRows),
				zap.Int64("estimatedTotalRows", totalRows))
		},
	}); err != nil {
		return errors.Trace(err)
	}
	if err := snapshot.LoadTSOFromMetadata(ctx, r.store, r.state); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *Runner) EnsureChangefeed(ctx context.Context) error {
	if id := r.state.Snapshot().TaskInfo.ChangefeedID; id != "" {
		log.Info("waiting for existing OP TiCDC changefeed from state",
			zap.String("changefeedID", id))
		changefeedID, err := r.waitChangefeed(ctx, id)
		if err != nil {
			return errors.Annotate(err, "wait OP TiCDC changefeed")
		}
		return errors.Trace(r.state.UpdateChangefeedID(ctx, changefeedID))
	}

	exists, err := storage.DirHasObjects(ctx, r.store, storage.IncrementDirName)
	if err != nil {
		return errors.Annotatef(err, "check %s directory", storage.IncrementDirName)
	}
	if exists {
		log.Info("changefeed data already exists in storage, skipping OP TiCDC changefeed creation",
			zap.String("dir", storage.IncrementDirName))
		return nil
	}

	changefeedID, err := r.createChangefeed(ctx)
	if err != nil {
		return errors.Annotate(err, "create OP TiCDC changefeed")
	}
	if changefeedID == "" {
		return errors.New("create OP TiCDC changefeed returned empty id")
	}
	if err := r.state.UpdateChangefeedID(ctx, changefeedID); err != nil {
		return errors.Trace(err)
	}
	log.Info("OP TiCDC changefeed created",
		zap.String("changefeedID", changefeedID),
		zap.String("snapshotTSO", r.state.Snapshot().Snapshot.TSO))

	waitedChangefeedID, err := r.waitChangefeed(ctx, changefeedID)
	if err != nil {
		return errors.Annotate(err, "wait OP TiCDC changefeed")
	}
	if err := r.state.UpdateChangefeedID(ctx, waitedChangefeedID); err != nil {
		return errors.Trace(err)
	}
	log.Info("OP TiCDC changefeed ready", zap.String("changefeedID", changefeedID))
	return nil
}

func (r *Runner) createChangefeed(ctx context.Context) (string, error) {
	client, err := r.ticdcClient()
	if err != nil {
		return "", errors.Trace(err)
	}
	startTSO, err := snapshot.ParseTSO(r.state.Snapshot().Snapshot.TSO)
	if err != nil {
		return "", errors.Trace(err)
	}
	req, err := ticdc.BuildChangefeedConfig(ticdc.ChangefeedConfigOptions{
		Tables:        r.cfg.Tables,
		StorageURI:    r.cfg.IncrementURI,
		StartTSO:      startTSO,
		FlushInterval: r.cfg.ChangefeedFlushInterval,
		FileSizeMiB:   r.cfg.ChangefeedFileSizeMiB,
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	cf, err := client.CreateChangefeed(ctx, req)
	if err != nil {
		return "", errors.Trace(err)
	}
	return ticdc.ChangefeedID(cf), nil
}

func (r *Runner) waitChangefeed(ctx context.Context, id string) (string, error) {
	client, err := r.ticdcClient()
	if err != nil {
		return "", err
	}
	cf, err := client.WaitChangefeed(ctx, id)
	if err != nil {
		return "", err
	}
	return ticdc.ChangefeedID(cf), nil
}

func (r *Runner) ticdcClient() (*ticdc.Client, error) {
	if r.client != nil {
		return r.client, nil
	}
	c, err := ticdc.NewClient(r.cfg.TiCDCAddress)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.client = c
	return c, nil
}
