package cmd

import (
	"context"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/incremental"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/snapshot"
	"github.com/tidbcloud/tidb2snowflake/source"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

// prepare snapshot and incremental data if necessary,
// load snapshot data into the snowflake, and periodically merge incremental data into it.
func Run(ctx context.Context, opt *Option) error {
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

	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
	if err != nil {
		return errors.Annotate(err, "open storage")
	}
	defer store.Close()

	stateManager, err := state.Open(ctx, store, opt.Tables)
	if err != nil {
		return errors.Trace(err)
	}

	if err := source.Prepare(ctx, opt.PrepareRequest(cred, storageURI), store, stateManager); err != nil {
		return errors.Trace(err)
	}

	// ---- Load object-storage files into Snowflake ----
	return loadIntoSnowflake(ctx, opt, opt.snowflakeConfig(), cred, storageURI, store, stateManager)
}

func loadIntoSnowflake(
	ctx context.Context,
	opt *Option,
	snowflakeCfg *snowflake.Config,
	cred *credentials.Value,
	storageURI *url.URL,
	store storeapi.Storage,
	stateManager state.Manager,
) error {
	metrics.TableNumGauge.Add(float64(len(opt.Tables)))
	log.Info("starting Snowflake load phase", zap.Int("tableCount", len(opt.Tables)))

	if opt.Mode != runModeIncrementalOnly && !stateManager.Snapshot().Snapshot.Finished {
		if err := snapshot.Load(ctx, snapshot.Config{
			Snowflake:   snowflakeCfg,
			Credential:  cred,
			Tables:      opt.Tables,
			StorageURI:  storageURI,
			StorageDir:  storage.SnapshotDirName,
			Compression: opt.SnapshotCompression,
		}, store); err != nil {
			return errors.Trace(err)
		}
		if err := stateManager.MarkSnapshotFinished(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	if opt.Mode != runModeSnapshotOnly {
		if err := incremental.Load(ctx, incremental.Config{
			Snowflake:    snowflakeCfg,
			Credential:   cred,
			Tables:       opt.Tables,
			StorageURI:   storageURI,
			StorageDir:   storage.IncrementDirName,
			ScanInterval: opt.ChangefeedFlushInterval / 5,
			State:        stateManager,
		}, store); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
