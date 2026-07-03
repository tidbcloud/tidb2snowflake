package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/incremental"
	"github.com/tidbcloud/tidb2snowflake/pkg/dumpling"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/workerpool"
	"github.com/tidbcloud/tidb2snowflake/snapshot"
	"github.com/tidbcloud/tidb2snowflake/source"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

// prepare snapshot and incremental data if necessary,
// load snapshot data into the snowflake, and periodically merge incremental data into it.
func run(ctx context.Context, opt *Option) error {
	if err := opt.validate(); err != nil {
		return err
	}
	cred := &aws.Credentials{
		AccessKeyID:     opt.AWSAccessKey,
		SecretAccessKey: opt.AWSSecretKey,
	}

	storageURI, err := storage.GetS3URIWithCredentials(opt.StoragePath, cred)
	if err != nil {
		return errors.Trace(err)
	}

	sourceStorage, err := storage.New(ctx, storageURI)
	if err != nil {
		return errors.Trace(err)
	}
	defer sourceStorage.Close()

	stateManager, err := state.Open(ctx, sourceStorage, opt.Tables, opt.Mode == runModeIncrementalOnly)
	if err != nil {
		return errors.Trace(err)
	}

	snapshotURI := storageURI.JoinPath(storage.SnapshotDirName)
	incrementURI := storageURI.JoinPath(storage.IncrementDirName)
	if err := source.Prepare(ctx, opt.prepareRequest(cred, snapshotURI, incrementURI), sourceStorage, stateManager); err != nil {
		return errors.Trace(err)
	}

	// ---- Load object-storage files into Snowflake ----
	return loadIntoSnowflake(ctx, opt.loadRequest(cred, storageURI), sourceStorage, stateManager)
}

func loadIntoSnowflake(
	ctx context.Context,
	req loadRequest,
	store *storage.Storage,
	stateManager state.Manager,
) error {
	tableCount := req.tableCount()
	metrics.TableNumGauge.Add(float64(tableCount))
	log.Info("starting Snowflake load phase", zap.Int("tableCount", tableCount))

	stateSnapshot := stateManager.Snapshot()
	loadSnapshot := req.LoadSnapshot && !stateSnapshot.SnapshotFinished
	if req.LoadIncremental && !loadSnapshot && !stateSnapshot.SnapshotFinished {
		return errors.New("snapshot is not finished; run all mode before incremental")
	}
	if !loadSnapshot && !req.LoadIncremental {
		return nil
	}

	conn, err := snowflake.NewConnector(req.Snowflake)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	if err := conn.CreateStage(ctx, req.StorageURI, req.Credential); err != nil {
		return errors.Trace(err)
	}
	defer conn.DropStage(ctx)

	pool := workerpool.New(workerpool.DefaultConcurrency)
	pool.Go(ctx)
	defer pool.Close()

	if loadSnapshot {
		if err := snapshot.Load(ctx, req.Snapshot, store, pool, conn); err != nil {
			return errors.Trace(err)
		}
		snapshotTSO, err := dumpling.LoadTSOFromMetadata(ctx, store)
		if err != nil {
			return errors.Trace(err)
		}
		if err := stateManager.MarkSnapshotFinished(ctx, snapshotTSO); err != nil {
			return errors.Trace(err)
		}
	}
	if req.LoadIncremental {
		if err := incremental.Load(ctx, req.Incremental, store, stateManager, pool, conn); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
