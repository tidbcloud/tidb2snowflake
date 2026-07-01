package op

import (
	"context"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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
	SnapshotCSVNullValue    string
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

	store        *storage.Storage
	stateManager state.Manager
}

func NewRunner(cfg Config, store *storage.Storage, state state.Manager) *Runner {
	return &Runner{
		cfg:          cfg,
		store:        store,
		stateManager: state,
	}
}

func (r *Runner) EnsureSnapshot(ctx context.Context) error {
	stateSnapshot := r.stateManager.Snapshot()
	if stateSnapshot.SnapshotFinished && stateSnapshot.CheckpointTS != 0 {
		log.Info("snapshot already finished in state, skipping OP Dumpling snapshot dump",
			zap.Uint64("checkpointTS", stateSnapshot.CheckpointTS))
		return nil
	}

	metadataPath := path.Join(storage.SnapshotDirName, "metadata")
	metadataExists, err := r.store.FileExists(ctx, metadataPath)
	if err != nil {
		return errors.Annotatef(err, "check snapshot metadata %s", metadataPath)
	}
	if metadataExists {
		snapshotTSO, err := dumpling.LoadTSOFromMetadata(ctx, r.store)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("snapshot metadata already exists in storage, skipping OP Dumpling snapshot dump",
			zap.String("metadata", metadataPath),
			zap.Uint64("snapshotTSO", snapshotTSO))
		return nil
	}

	exist, err := r.store.DirHasObjects(ctx, storage.SnapshotDirName)
	if err != nil {
		return errors.Annotate(err, "check snapshot directory")
	}
	if exist {
		return errors.Errorf("snapshot directory is not complete, metadata missing: %s", metadataPath)
	}

	log.Info("dumping OP TiDB snapshot with Dumpling")
	if err := dumpling.Run(ctx, r.cfg.TiDB, dumpling.Config{
		Concurrency:  r.cfg.SnapshotConcurrency,
		StorageURI:   r.cfg.SnapshotURI,
		SnapshotTSO:  r.cfg.SnapshotTSO,
		Tables:       r.cfg.Tables,
		Compression:  r.cfg.SnapshotCompression,
		CSVNullValue: r.cfg.SnapshotCSVNullValue,
		OnProgress: func(dumpedRows, totalRows int64) {
			log.Info("snapshot dumpling progress",
				zap.Int64("dumpedRows", dumpedRows),
				zap.Int64("estimatedTotalRows", totalRows))
		},
	}); err != nil {
		return errors.Trace(err)
	}

	snapshotTSO, err := dumpling.LoadTSOFromMetadata(ctx, r.store)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("OP Dumpling snapshot dump finished, snapshot metadata loaded",
		zap.String("metadata", path.Join(storage.SnapshotDirName, "metadata")),
		zap.Uint64("snapshotTSO", snapshotTSO))
	return nil
}

func (r *Runner) EnsureChangefeed(ctx context.Context) error {
	if id := r.stateManager.Snapshot().TaskInfo.ChangefeedID; id != "" {
		err := r.getChangefeed(ctx, id)
		if err != nil {
			return errors.Annotate(err, "get OP TiCDC changefeed")
		}
		log.Info("OP TiCDC changefeed exists, skipping creation", zap.String("changefeedID", id))
		return nil
	}

	changefeedID, startTSO, err := r.createChangefeed(ctx)
	if err != nil {
		return errors.Annotate(err, "create OP TiCDC changefeed")
	}
	if err := r.stateManager.SetChangefeedID(ctx, changefeedID); err != nil {
		return errors.Trace(err)
	}
	log.Info("OP TiCDC changefeed created",
		zap.String("changefeedID", changefeedID),
		zap.Uint64("startTSO", startTSO))

	return nil
}

func (r *Runner) createChangefeed(ctx context.Context) (string, uint64, error) {
	startTSO, err := r.changefeedStartTSO(ctx)
	if err != nil {
		return "", 0, errors.Trace(err)
	}

	client, err := r.ticdcClient()
	if err != nil {
		return "", 0, errors.Trace(err)
	}

	req, err := ticdc.BuildChangefeedConfig(ticdc.ChangefeedConfigOptions{
		Tables:        r.cfg.Tables,
		StorageURI:    r.cfg.IncrementURI,
		StartTSO:      startTSO,
		FlushInterval: r.cfg.ChangefeedFlushInterval,
		FileSizeMiB:   r.cfg.ChangefeedFileSizeMiB,
	})
	if err != nil {
		return "", 0, errors.Trace(err)
	}
	cf, err := client.CreateChangefeed(ctx, req)
	if err != nil {
		return "", 0, errors.Trace(err)
	}
	return cf.ID, startTSO, nil
}

func (r *Runner) changefeedStartTSO(ctx context.Context) (uint64, error) {
	if checkpointTS := r.stateManager.Snapshot().CheckpointTS; checkpointTS != 0 {
		return checkpointTS, nil
	}
	metadataPath := path.Join(storage.SnapshotDirName, "metadata")
	metadataExists, err := r.store.FileExists(ctx, metadataPath)
	if err != nil {
		return 0, errors.Annotatef(err, "check snapshot metadata %s", metadataPath)
	}
	if metadataExists {
		return dumpling.LoadTSOFromMetadata(ctx, r.store)
	}
	if r.cfg.SnapshotTSO != "" {
		tso, err := strconv.ParseUint(r.cfg.SnapshotTSO, 10, 64)
		if err != nil {
			return 0, errors.Annotate(err, "parse snapshot tso")
		}
		return tso, nil
	}
	return 0, nil
}

func (r *Runner) getChangefeed(ctx context.Context, id string) error {
	client, err := r.ticdcClient()
	if err != nil {
		return err
	}
	_, err = client.GetChangefeed(ctx, id)
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
