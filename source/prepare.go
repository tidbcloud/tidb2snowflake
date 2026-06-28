package source

import (
	"context"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/op"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"github.com/tidbcloud/tidb2snowflake/source/tidbcloud"
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
	Tables                  []string
	ChangefeedFlushInterval time.Duration
	ChangefeedFileSizeMiB   int
	SnapshotCompression     string

	Credential  *credentials.Value
	StoragePath string
	StorageURI  *url.URL
}

type runner interface {
	EnsureSnapshot(context.Context) error
	EnsureChangefeed(context.Context) error
}

func Prepare(ctx context.Context, request Request, store storeapi.Storage, state state.Manager) error {
	if request.PrepareSnapshot || request.PrepareChangefeed {
		if request.StorageURI == nil {
			return errors.New("storage URI is required to prepare source")
		}
	}
	runner := newRunner(request, store, state)
	if request.PrepareSnapshot {
		if err := runner.EnsureSnapshot(ctx); err != nil {
			return errors.Trace(err)
		}
		if state.Snapshot().Snapshot.TSO == 0 {
			return errors.New("snapshot.tso is required after preparing snapshot")
		}
	}
	if request.PrepareChangefeed {
		if err := runner.EnsureChangefeed(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func newRunner(request Request, store storeapi.Storage, state state.Manager) runner {
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
		Tables:                  request.Tables,
		ChangefeedFlushInterval: request.ChangefeedFlushInterval,
		ChangefeedFileSizeMiB:   request.ChangefeedFileSizeMiB,
		SnapshotCompression:     request.SnapshotCompression,
		SnapshotTSO:             request.SnapshotTSO,
		SnapshotURI:             request.StorageURI.JoinPath(storage.SnapshotDirName),
		IncrementURI:            request.StorageURI.JoinPath(storage.IncrementDirName),
	}
}

func (request Request) tidbCloudConfig() tidbcloud.Config {
	var compression cloudapi.ExportCompression
	switch request.SnapshotCompression {
	case "gzip":
		compression = cloudapi.ExportCompressionGzip
	default:
		compression = cloudapi.ExportCompressionNone
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
		StoragePath:             request.StoragePath,
	}
}
