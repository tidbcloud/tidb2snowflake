package cmd

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewCreateCmd builds the `create` subcommand.
func NewCreateCmd() *cobra.Command {
	return newCreateCmdWithRun(run)
}

func newCreateCmdWithRun(run func(context.Context, *Option) error) *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "create --config config.toml",
		Short: "Replicate snapshot and incremental data from TiDB to Snowflake",
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
				log.Error("replication failed", zap.Error(err))
				return err
			}
			log.Info("replication finished")
			return nil
		},
	}

	f := cmd.Flags()
	f.StringVar(&configPath, "config", "", "TOML config file path")

	return cmd
}
