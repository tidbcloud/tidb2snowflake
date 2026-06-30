package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/incremental"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
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
	cred := &credentials.Value{
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

	stateManager, err := state.Open(ctx, sourceStorage, opt.Tables)
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

	if req.LoadSnapshot && !stateManager.Snapshot().Snapshot.Finished {
		if err := snapshot.Load(ctx, req.Snapshot, store); err != nil {
			return errors.Trace(err)
		}
		if err := stateManager.MarkSnapshotFinished(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	if req.LoadIncremental {
		if err := incremental.Load(ctx, req.Incremental, store, stateManager); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
