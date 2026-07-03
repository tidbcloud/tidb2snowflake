package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/spf13/cobra"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/ticdc"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

// NewDeleteCmd builds the `delete` subcommand.
func NewDeleteCmd() *cobra.Command {
	return newDeleteCmdWithRun(runDelete)
}

func newDeleteCmdWithRun(run func(context.Context, *Option) error) *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "delete --config config.toml",
		Short: "Delete the changefeed recorded in tidb2snowflake state",
		RunE: func(c *cobra.Command, _ []string) error {
			if configPath == "" {
				err := errors.New("--config is required")
				c.PrintErrf("config error: %v\n", err)
				return err
			}
			opt, logConfig, err := loadConfig(configPath)
			if err != nil {
				c.PrintErrf("load config failed: %v\n", err)
				return err
			}
			if err := logger.InitLogger(logConfig); err != nil {
				c.PrintErrf("init logger failed: %v\n", err)
				return errors.Trace(err)
			}
			ctx := context.Background()
			if err := run(ctx, opt); err != nil {
				log.Error("delete changefeed failed", zap.Error(err))
				return err
			}
			log.Info("delete changefeed finished")
			return nil
		},
	}

	f := cmd.Flags()
	f.StringVar(&configPath, "config", "", "TOML config file path")

	return cmd
}

func runDelete(ctx context.Context, opt *Option) error {
	if err := opt.validateDelete(); err != nil {
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

	stateManager, err := state.OpenExisting(ctx, sourceStorage)
	if err != nil {
		return errors.Trace(err)
	}
	changefeedID := stateManager.Snapshot().TaskInfo.ChangefeedID
	if changefeedID == "" {
		return errors.New("state task_info.changefeed_id is empty")
	}

	switch opt.SourceMode {
	case sourceModeOP:
		err = deleteOPChangefeed(ctx, opt, changefeedID)
	default:
		err = deleteTiDBCloudChangefeed(ctx, opt, changefeedID)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if err := stateManager.ClearChangefeedID(ctx); err != nil {
		return errors.Trace(err)
	}
	log.Info("changefeed deleted and state cleared",
		zap.String("sourceMode", opt.SourceMode),
		zap.String("changefeedID", changefeedID))
	return nil
}

func (opt *Option) validateDelete() error {
	opt.adjust()
	if opt.StoragePath == "" {
		return errors.New("storage.uri is required")
	}
	if opt.AWSAccessKey == "" || opt.AWSSecretKey == "" {
		return errors.New("storage.access-key and storage.secret-access-key are required")
	}
	if opt.SourceMode == sourceModeOP {
		if opt.TiCDCAddress == "" {
			return errors.New("ticdc.address is required when source=op")
		}
		return nil
	}
	if opt.TiDBCloudClusterID == "" {
		return errors.New("tidbcloud.cluster-id is required")
	}
	if opt.TiDBCloudPublicKey == "" {
		return errors.New("tidbcloud.public-key is required")
	}
	if opt.TiDBCloudPrivateKey == "" {
		return errors.New("tidbcloud.private-key is required")
	}
	return nil
}

func deleteTiDBCloudChangefeed(ctx context.Context, opt *Option, changefeedID string) error {
	var clientOptions []cloudapi.Option
	if opt.TiDBCloudHost != "" {
		clientOptions = append(clientOptions, cloudapi.WithHost(opt.TiDBCloudHost))
	}
	client, err := cloudapi.NewClient(opt.TiDBCloudPublicKey, opt.TiDBCloudPrivateKey, clientOptions...)
	if err != nil {
		return errors.Trace(err)
	}
	return client.DeleteChangefeed(ctx, opt.TiDBCloudClusterID, changefeedID)
}

func deleteOPChangefeed(ctx context.Context, opt *Option, changefeedID string) error {
	client, err := ticdc.NewClient(opt.TiCDCAddress)
	if err != nil {
		return errors.Trace(err)
	}
	return client.DeleteChangefeed(ctx, changefeedID)
}
