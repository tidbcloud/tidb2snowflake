package tidbcloud

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/snapshot"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

type Config struct {
	ClusterID               string
	PublicKey               string
	PrivateKey              string
	Host                    string
	Tables                  []string
	ChangefeedFlushInterval time.Duration
	ChangefeedFileSizeMiB   int
	SnapshotCompression     string

	Credential  *credentials.Value
	StoragePath string
}

type Runner struct {
	cfg    Config
	client *cloudapi.Client

	store storeapi.Storage
	state state.Manager
}

func NewRunner(cfg Config, store storeapi.Storage, state state.Manager) *Runner {
	return &Runner{
		cfg:   cfg,
		store: store,
		state: state,
	}
}

func (r *Runner) EnsureSnapshot(ctx context.Context) error {
	if r.state.Snapshot().TaskInfo.ExportID == "" {
		if err := snapshot.LoadExistingTSO(ctx, r.store, r.state); err != nil {
			return errors.Trace(err)
		}
	}

	if r.state.Snapshot().Snapshot.Finished {
		if err := snapshot.LoadExistingTSO(ctx, r.store, r.state); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	if err := r.ensureExport(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := snapshot.LoadTSOFromMetadata(ctx, r.store, r.state); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *Runner) ensureExport(ctx context.Context) error {
	if id := r.state.Snapshot().TaskInfo.ExportID; id != "" {
		exportID, snapshotTSO, err := r.waitExport(ctx, id)
		if err != nil {
			return errors.Annotate(err, "wait TiDB Cloud export")
		}
		return errors.Trace(r.state.UpdateExportState(ctx, exportID, snapshotTSO))
	}

	exists, err := storage.DirHasObjects(ctx, r.store, storage.SnapshotDirName)
	if err != nil {
		return errors.Annotatef(err, "check %s directory", storage.SnapshotDirName)
	}
	if exists {
		return nil
	}

	exportID, snapshotTSO, err := r.createExport(ctx)
	if err != nil {
		return errors.Annotate(err, "create TiDB Cloud export")
	}
	if exportID == "" {
		return errors.New("create TiDB Cloud export returned empty id")
	}
	if err := r.state.UpdateExportState(ctx, exportID, snapshotTSO); err != nil {
		return errors.Trace(err)
	}
	waitedExportID, waitedSnapshotTSO, err := r.waitExport(ctx, exportID)
	if err != nil {
		return errors.Annotate(err, "wait TiDB Cloud export")
	}
	return errors.Trace(r.state.UpdateExportState(ctx, waitedExportID, waitedSnapshotTSO))
}

func (r *Runner) EnsureChangefeed(ctx context.Context) error {
	if id := r.state.Snapshot().TaskInfo.ChangefeedID; id != "" {
		log.Info("waiting for existing TiDB Cloud changefeed from state",
			zap.String("changefeedID", id))
		changefeedID, err := r.waitChangefeed(ctx, id)
		if err != nil {
			return errors.Annotate(err, "wait TiDB Cloud changefeed")
		}
		return errors.Trace(r.state.UpdateChangefeedID(ctx, changefeedID))
	}

	exists, err := storage.DirHasObjects(ctx, r.store, storage.IncrementDirName)
	if err != nil {
		return errors.Annotatef(err, "check %s directory", storage.IncrementDirName)
	}
	if exists {
		log.Info("changefeed data already exists in storage, skipping TiDB Cloud changefeed creation",
			zap.String("dir", storage.IncrementDirName))
		return nil
	}

	changefeedID, err := r.createChangefeed(ctx)
	if err != nil {
		return errors.Annotate(err, "create TiDB Cloud changefeed")
	}
	if changefeedID == "" {
		return errors.New("create TiDB Cloud changefeed returned empty id")
	}
	if err := r.state.UpdateChangefeedID(ctx, changefeedID); err != nil {
		return errors.Trace(err)
	}
	log.Info("TiDB Cloud changefeed created",
		zap.String("changefeedID", changefeedID),
		zap.String("snapshotTSO", r.state.Snapshot().Snapshot.TSO))

	waitedChangefeedID, err := r.waitChangefeed(ctx, changefeedID)
	if err != nil {
		return errors.Annotate(err, "wait TiDB Cloud changefeed")
	}
	if err := r.state.UpdateChangefeedID(ctx, waitedChangefeedID); err != nil {
		return errors.Trace(err)
	}
	log.Info("TiDB Cloud changefeed ready", zap.String("changefeedID", changefeedID))
	return nil
}

func (r *Runner) createExport(ctx context.Context) (string, string, error) {
	c, err := r.tidbCloudClient()
	if err != nil {
		return "", "", err
	}
	cleanSnapshotURI, err := storage.CleanSubURI(r.cfg.StoragePath, storage.SnapshotDirName)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	req := buildExportRequest(r.cfg, cleanSnapshotURI, r.cfg.Credential, r.state.Snapshot().Snapshot.TSO)
	export, err := c.CreateExport(ctx, r.cfg.ClusterID, req)
	if err != nil {
		return "", "", err
	}
	return export.ExportID, export.SnapshotTSO, nil
}

func (r *Runner) waitExport(ctx context.Context, exportID string) (string, string, error) {
	c, err := r.tidbCloudClient()
	if err != nil {
		return "", "", err
	}
	export, err := c.WaitExport(ctx, r.cfg.ClusterID, exportID)
	if err != nil {
		return "", "", err
	}
	return export.ExportID, export.SnapshotTSO, nil
}

func (r *Runner) createChangefeed(ctx context.Context) (string, error) {
	c, err := r.tidbCloudClient()
	if err != nil {
		return "", err
	}
	cleanIncrementURI, err := storage.CleanSubURI(r.cfg.StoragePath, storage.IncrementDirName)
	if err != nil {
		return "", errors.Trace(err)
	}
	req := buildChangefeedRequest(r.cfg, cleanIncrementURI, r.cfg.Credential, r.state.Snapshot().Snapshot.TSO)
	cf, err := c.CreateChangefeed(ctx, r.cfg.ClusterID, req)
	if err != nil {
		return "", err
	}
	return cf.ChangefeedID, nil
}

func (r *Runner) waitChangefeed(ctx context.Context, changefeedID string) (string, error) {
	c, err := r.tidbCloudClient()
	if err != nil {
		return "", err
	}
	changefeed, err := c.WaitChangefeed(ctx, r.cfg.ClusterID, changefeedID)
	if err != nil {
		return "", err
	}
	return changefeed.ChangefeedID, nil
}

func (r *Runner) tidbCloudClient() (*cloudapi.Client, error) {
	if r.client != nil {
		return r.client, nil
	}
	if r.cfg.ClusterID == "" {
		return nil, errors.New("--tidbcloud.cluster-id is required to create or wait on an export/changefeed")
	}
	var opts []cloudapi.Option
	if r.cfg.Host != "" {
		opts = append(opts, cloudapi.WithHost(r.cfg.Host))
	}
	c, err := cloudapi.NewClient(r.cfg.PublicKey, r.cfg.PrivateKey, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.client = c
	return c, nil
}

func buildChangefeedRequest(cfg Config, cleanIncrementURI string, cred *credentials.Value, snapshotTSO string) *cloudapi.CreateChangefeedRequest {
	start := &cloudapi.StartPosition{}
	if snapshotTSO != "" {
		start.Mode = cloudapi.StartModeFromTSO
		start.TSO = snapshotTSO
	} else {
		start.Mode = cloudapi.StartModeFromNow
	}
	return &cloudapi.CreateChangefeedRequest{
		DisplayName: "tidb2snowflake-incremental",
		Sink: &cloudapi.Sink{
			Type: cloudapi.ChangefeedTypeCloudStorage,
			CloudStorage: &cloudapi.CloudStorageSink{
				Storage: &cloudapi.CloudStorage{
					Type: cloudapi.CloudStorageTypeS3,
					S3: &cloudapi.S3CloudStorage{
						URI:       cleanIncrementURI,
						AuthType:  cloudapi.S3AuthTypeAccessKey,
						AccessKey: &cloudapi.S3AccessKey{ID: cred.AccessKeyID, Secret: cred.SecretAccessKey},
					},
				},
				DataFormat: &cloudapi.CloudStorageDataFormat{
					Protocol: cloudapi.CloudStorageProtocolCSV,
					CSVConfig: &cloudapi.CSVConfig{
						IncludeCommitTs: true,
						Encoding:        cloudapi.BinaryEncodingHex,
					},
				},
				DateSeparator:     cloudapi.DateSeparatorDay,
				IntervalInSeconds: int(cfg.ChangefeedFlushInterval.Seconds()),
				SizeInMiB:         cfg.ChangefeedFileSizeMiB,
				OutputColumnID:    true,
			},
		},
		Filter:        &cloudapi.ChangefeedFilter{FilterRule: cfg.Tables, Mode: cloudapi.TableModeForceSync},
		StartPosition: start,
	}
}

func buildExportRequest(cfg Config, cleanSnapshotURI string, cred *credentials.Value, snapshotTSO string) *cloudapi.CreateExportRequest {
	req := &cloudapi.CreateExportRequest{
		DisplayName: "tidb2snowflake-snapshot",
		ExportOptions: &cloudapi.ExportOptions{
			FileType:        cloudapi.ExportFileTypeCSV,
			Compression:     exportCompression(cfg.SnapshotCompression),
			EscapeBackslash: util.AddressOf(false),
			Filter:          &cloudapi.ExportFilter{Table: &cloudapi.ExportFilterTable{Patterns: cfg.Tables}},
			CSVFormat: &cloudapi.ExportCSVFormat{
				Separator:  ",",
				Delimiter:  util.AddressOf("\""),
				NullValue:  util.AddressOf("\\N"),
				SkipHeader: true,
				Dialect:    cloudapi.ExportCSVDialectSnowflake,
			},
		},
		Target: &cloudapi.ExportTarget{
			Type: cloudapi.ExportTargetTypeS3,
			S3: &cloudapi.S3Target{
				URI:       cleanSnapshotURI,
				AuthType:  cloudapi.S3AuthTypeAccessKey,
				AccessKey: &cloudapi.S3AccessKey{ID: cred.AccessKeyID, Secret: cred.SecretAccessKey},
			},
		},
	}
	if snapshotTSO != "" {
		req.ExportOptions.SnapshotTSO = snapshotTSO
	}
	return req
}

func exportCompression(compression string) cloudapi.ExportCompression {
	switch compression {
	case "gzip":
		return cloudapi.ExportCompressionGzip
	default:
		return cloudapi.ExportCompressionNone
	}
}
