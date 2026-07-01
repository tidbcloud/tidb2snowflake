package source

import (
	"context"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/op"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"github.com/tidbcloud/tidb2snowflake/source/tidbcloud"
	"go.uber.org/zap"
)

type Request struct {
	PrepareSnapshot   bool
	PrepareChangefeed bool
	UseOPSource       bool
	SnapshotTSO       string

	TiDB                    *tidb.Config
	TiDBCloudClusterID      string
	TiDBCloudPublicKey      string
	TiDBCloudPrivateKey     string
	TiDBCloudHost           string
	TiCDCAddress            string
	SnapshotConcurrency     int
	SnapshotCSVNullValue    string
	Tables                  []string
	ChangefeedFlushInterval time.Duration
	ChangefeedFileSizeMiB   int
	SnapshotCompression     string

	Credential   *aws.Credentials
	SnapshotURI  *url.URL
	IncrementURI *url.URL
}

type runner interface {
	EnsureSnapshot(context.Context) error
	EnsureChangefeed(context.Context) error
}

func Prepare(ctx context.Context, request Request, store *storage.Storage, state state.Manager) error {
	start := time.Now()
	log.Info("starting source prepare phase",
		zap.Bool("prepareSnapshot", request.PrepareSnapshot),
		zap.Bool("prepareChangefeed", request.PrepareChangefeed),
		zap.Int("tableCount", len(request.Tables)))
	if request.UseOPSource {
		if request.PrepareSnapshot && request.SnapshotURI == nil {
			return errors.New("snapshot URI is required to prepare OP snapshot")
		}
		if request.PrepareChangefeed && request.IncrementURI == nil {
			return errors.New("increment URI is required to prepare OP changefeed")
		}
	}
	runner := newRunner(request, store, state)
	if request.PrepareSnapshot {
		if err := runner.EnsureSnapshot(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	if request.PrepareChangefeed {
		if err := runner.EnsureChangefeed(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	log.Info("source prepare phase finished", zap.Duration("duration", time.Since(start)))
	return nil
}

func newRunner(request Request, store *storage.Storage, state state.Manager) runner {
	if request.UseOPSource {
		return op.NewRunner(request.opConfig(), store, state)
	}
	return tidbcloud.NewRunner(request.tidbCloudConfig(), store, state)
}

func (request Request) opConfig() op.Config {
	return op.Config{
		TiDB:                    request.TiDB,
		TiCDCAddress:            request.TiCDCAddress,
		SnapshotConcurrency:     request.SnapshotConcurrency,
		SnapshotCSVNullValue:    request.SnapshotCSVNullValue,
		Tables:                  request.Tables,
		ChangefeedFlushInterval: request.ChangefeedFlushInterval,
		ChangefeedFileSizeMiB:   request.ChangefeedFileSizeMiB,
		SnapshotCompression:     request.SnapshotCompression,
		SnapshotTSO:             request.SnapshotTSO,
		SnapshotURI:             request.SnapshotURI,
		IncrementURI:            request.IncrementURI,
	}
}

func (request Request) tidbCloudConfig() tidbcloud.Config {
	var compression cloudapi.ExportCompression
	switch request.SnapshotCompression {
	case "none":
		compression = cloudapi.ExportCompressionNone
	case "gzip":
		compression = cloudapi.ExportCompressionGzip
	default:
		log.Panic("unknown snapshot compression", zap.String("compression", request.SnapshotCompression))
	}
	return tidbcloud.Config{
		ClusterID:               request.TiDBCloudClusterID,
		PublicKey:               request.TiDBCloudPublicKey,
		PrivateKey:              request.TiDBCloudPrivateKey,
		Host:                    request.TiDBCloudHost,
		Tables:                  request.Tables,
		ChangefeedFlushInterval: request.ChangefeedFlushInterval,
		ChangefeedFileSizeMiB:   request.ChangefeedFileSizeMiB,
		SnapshotCompression:     compression,
		SnapshotTSO:             request.SnapshotTSO,
		Credential:              request.Credential,
	}
}
