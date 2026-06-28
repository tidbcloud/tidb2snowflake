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
	SnapshotTSO             string

	SnapshotURI  *url.URL
	IncrementURI *url.URL
}

type Runner struct {
	cfg    Config
	client *ticdc.Client

	store        storeapi.Storage
	stateManager state.Manager
}

func NewRunner(cfg Config, store storeapi.Storage, state state.Manager) *Runner {
	return &Runner{
		cfg:          cfg,
		store:        store,
		stateManager: state,
	}
}

func (r *Runner) EnsureSnapshot(ctx context.Context) error {
	stateSnapshot := r.stateManager.Snapshot()
	if stateSnapshot.Snapshot.TSO != 0 {
		log.Info("snapshot TSO already exists in state, skipping OP Dumpling snapshot dump",
			zap.Uint64("snapshotTSO", stateSnapshot.Snapshot.TSO))
		return nil
	}

	exist, err := storage.DirHasObjects(ctx, r.store, storage.SnapshotDirName)
	if err != nil {
		return errors.Annotate(err, "check snapshot directory")
	}

	if !exist {
		log.Info("dumping OP TiDB snapshot with Dumpling")
		if err := dumpling.Run(ctx, r.store, r.cfg.TiDB, dumpling.Config{
			Concurrency:  r.cfg.SnapshotConcurrency,
			StorageURI:   r.cfg.SnapshotURI,
			SnapshotTSO:  r.cfg.SnapshotTSO,
			Tables:       r.cfg.Tables,
			Compression:  r.cfg.SnapshotCompression,
			CSVNullValue: "\\N",
			OnProgress: func(dumpedRows, totalRows int64) {
				log.Info("snapshot dumpling progress",
					zap.Int64("dumpedRows", dumpedRows),
					zap.Int64("estimatedTotalRows", totalRows))
			},
		}); err != nil {
			return errors.Trace(err)
		}
	}

	snapshotTSO, err := dumpling.LoadTSOFromMetadata(ctx, r.store)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("snapshot data already exists in storage, skipping OP Dumpling snapshot dump",
		zap.String("dir", storage.SnapshotDirName),
		zap.Uint64("snapshotTSO", snapshotTSO))
	return r.stateManager.SetSnapshotTSO(ctx, snapshotTSO)
}

func (r *Runner) EnsureChangefeed(ctx context.Context) error {
	if id := r.stateManager.Snapshot().TaskInfo.ChangefeedID; id != "" {
		err := r.waitChangefeed(ctx, id)
		if err != nil {
			return errors.Annotate(err, "wait OP TiCDC changefeed")
		}
		return nil
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
	if err := r.stateManager.SetChangefeedID(ctx, changefeedID); err != nil {
		return errors.Trace(err)
	}
	log.Info("OP TiCDC changefeed created",
		zap.String("changefeedID", changefeedID),
		zap.Uint64("snapshotTSO", r.stateManager.Snapshot().Snapshot.TSO))

	err = r.waitChangefeed(ctx, changefeedID)
	if err != nil {
		return errors.Annotate(err, "wait OP TiCDC changefeed")
	}
	log.Info("OP TiCDC changefeed ready", zap.String("changefeedID", changefeedID))
	return nil
}

func (r *Runner) createChangefeed(ctx context.Context) (string, error) {
	snapshotTSO := r.stateManager.Snapshot().Snapshot.TSO
	if snapshotTSO == 0 {
		return "", errors.New("snapshot.tso is required to create OP TiCDC changefeed")
	}

	client, err := r.ticdcClient()
	if err != nil {
		return "", errors.Trace(err)
	}

	req, err := ticdc.BuildChangefeedConfig(ticdc.ChangefeedConfigOptions{
		Tables:        r.cfg.Tables,
		StorageURI:    r.cfg.IncrementURI,
		StartTSO:      snapshotTSO,
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

func (r *Runner) waitChangefeed(ctx context.Context, id string) error {
	client, err := r.ticdcClient()
	if err != nil {
		return err
	}
	_, err = client.WaitChangefeed(ctx, id)
	if err != nil {
		return err
	}
	return nil
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
