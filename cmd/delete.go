package cmd

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewDeleteCmd builds the `delete` subcommand.
func NewDeleteCmd() *cobra.Command {
	return newDeleteCmdWithRun(runDelete)
}

func newDeleteCmdWithRun(run func(context.Context, *Option) error) *cobra.Command {
	var (
		logFile  string
		logLevel string
	)

	opt := NewOption()
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete the task changefeed recorded in replication state",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := logger.InitLogger(&logger.Config{Level: logLevel, File: logFile}); err != nil {
				return errors.Trace(err)
			}
			ctx := context.Background()
			if err := run(ctx, opt); err != nil {
				log.Error("task deletion failed", zap.Error(err))
				return err
			}
			log.Info("task deletion finished")
			return nil
		},
	}

	f := cmd.Flags()
	f.StringVar(&opt.SourceMode, "source.mode", opt.SourceMode, "source deployment mode: tidbcloud or op")
	f.StringVar(&opt.TiCDCAddress, "ticdc.address", opt.TiCDCAddress, "TiCDC OpenAPI base address for --source.mode=op, e.g. http://127.0.0.1:8300")
	f.StringVarP(&opt.StoragePath, "storage", "s", opt.StoragePath, "object storage path, e.g. s3://<bucket>/<path>")
	f.StringVar(&opt.AWSAccessKey, "aws.access-key", opt.AWSAccessKey, "AWS access key for the storage bucket")
	f.StringVar(&opt.AWSSecretKey, "aws.secret-key", opt.AWSSecretKey, "AWS secret key for the storage bucket")
	f.StringVar(&logFile, "log.file", "", "log file path")
	f.StringVar(&logLevel, "log.level", "info", "log level")

	_ = cmd.MarkFlagRequired("storage")

	return cmd
}
