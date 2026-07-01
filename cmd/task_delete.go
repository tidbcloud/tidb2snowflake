package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/ticdc"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

type changefeedDeleter interface {
	DeleteChangefeed(context.Context, string) error
}

func runDelete(ctx context.Context, opt *Option) error {
	if err := opt.validateDelete(); err != nil {
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

	stateManager, err := state.OpenExisting(ctx, sourceStorage)
	if err != nil {
		return errors.Trace(err)
	}

	deleter, err := opt.newChangefeedDeleter()
	if err != nil {
		return errors.Trace(err)
	}
	return deleteChangefeedFromState(ctx, stateManager, deleter)
}

func deleteChangefeedFromState(ctx context.Context, stateManager state.Manager, deleter changefeedDeleter) error {
	changefeedID := stateManager.Snapshot().TaskInfo.ChangefeedID
	if changefeedID == "" {
		return errors.New("no changefeed id found in state")
	}
	if err := deleter.DeleteChangefeed(ctx, changefeedID); err != nil {
		return errors.Annotate(err, "delete changefeed")
	}
	if err := stateManager.ClearChangefeedID(ctx); err != nil {
		return errors.Trace(err)
	}
	log.Info("changefeed deleted", zap.String("changefeedID", changefeedID))
	return nil
}

func (opt *Option) newChangefeedDeleter() (changefeedDeleter, error) {
	switch opt.SourceMode {
	case sourceModeOP:
		client, err := ticdc.NewClient(opt.TiCDCAddress)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return opChangefeedDeleter{client: client}, nil
	case sourceModeTiDBCloud:
		var opts []cloudapi.Option
		if opt.TiDBCloudHost != "" {
			opts = append(opts, cloudapi.WithHost(opt.TiDBCloudHost))
		}
		client, err := cloudapi.NewClient(opt.TiDBCloudPublicKey, opt.TiDBCloudPrivateKey, opts...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return tidbCloudChangefeedDeleter{client: client, clusterID: opt.TiDBCloudClusterID}, nil
	default:
		return nil, errors.Errorf("unsupported source mode %q", opt.SourceMode)
	}
}

type opChangefeedDeleter struct {
	client *ticdc.Client
}

func (d opChangefeedDeleter) DeleteChangefeed(ctx context.Context, changefeedID string) error {
	return d.client.DeleteChangefeed(ctx, changefeedID)
}

type tidbCloudChangefeedDeleter struct {
	client    *cloudapi.Client
	clusterID string
}

func (d tidbCloudChangefeedDeleter) DeleteChangefeed(ctx context.Context, changefeedID string) error {
	return d.client.DeleteChangefeed(ctx, d.clusterID, changefeedID)
}
