package tidbcloud

import (
	"context"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tidbcloud/tidb2snowflake/pkg/dumpling"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
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
	SnapshotCompression     tidbcloud.ExportCompression
	SnapshotTSO             string

	Credential *credentials.Value
}

type Runner struct {
	cfg    Config
	client *tidbcloud.Client

	store *storage.Storage
	state state.Manager
}

func NewRunner(cfg Config, store *storage.Storage, state state.Manager) *Runner {
	return &Runner{
		cfg:   cfg,
		store: store,
		state: state,
	}
}

func (r *Runner) EnsureSnapshot(ctx context.Context) error {
	stateSnapshot := r.state.Snapshot()
	if stateSnapshot.CheckpointTS != 0 {
		log.Info("snapshot checkpoint already exists in state, skipping TiDB Cloud export",
			zap.Uint64("checkpointTS", stateSnapshot.CheckpointTS))
		return nil
	}

	metadataPath := path.Join(storage.SnapshotDirName, "metadata")
	metadataExists, err := r.store.FileExists(ctx, metadataPath)
	if err != nil {
		return errors.Annotatef(err, "check snapshot metadata %s", metadataPath)
	}
	if metadataExists {
		snapshotTSO, err := dumpling.LoadTSOFromMetadata(ctx, r.store)
		if err != nil {
			return errors.Trace(err)
		}
		if err := r.state.SetCheckpointTS(ctx, snapshotTSO); err != nil {
			return errors.Trace(err)
		}
		log.Info("snapshot metadata already exists in storage, skipping TiDB Cloud export",
			zap.String("metadata", metadataPath),
			zap.Uint64("snapshotTSO", snapshotTSO))
		return nil
	}

	var snapshotTSO string
	exportID := r.state.Snapshot().TaskInfo.ExportID
	if exportID == "" {
		snapshotExists, err := r.store.DirHasObjects(ctx, storage.SnapshotDirName)
		if err != nil {
			return errors.Annotatef(err, "check %s directory", storage.SnapshotDirName)
		}
		if snapshotExists {
			return errors.Errorf("snapshot directory is not complete, metadata missing: %s", metadataPath)
		}
	}
	if exportID == "" {
		exportID, snapshotTSO, err = r.createExport(ctx)
		if err != nil {
			return errors.Annotate(err, "create TiDB Cloud export")
		}
		log.Info("TiDB Cloud export created", zap.String("exportID", exportID), zap.String("snapshotTSO", snapshotTSO))
	}

	if err := r.state.SetExportID(ctx, exportID); err != nil {
		return errors.Trace(err)
	}

	_, snapshotTSO, err = r.waitExport(ctx, exportID)
	if err != nil {
		return errors.Annotate(err, "wait TiDB Cloud export")
	}
	log.Info("TiDB Cloud export ready", zap.String("exportID", exportID), zap.String("snapshotTSO", snapshotTSO))

	tso, err := dumpling.LoadTSOFromMetadata(ctx, r.store)
	if err != nil {
		return errors.Annotate(err, "load TiDB Cloud export snapshot metadata")
	}
	if err := r.state.SetCheckpointTS(ctx, tso); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *Runner) EnsureChangefeed(ctx context.Context) error {
	if id := r.state.Snapshot().TaskInfo.ChangefeedID; id != "" {
		err := r.getChangefeed(ctx, id)
		if err != nil {
			return errors.Annotate(err, "get TiDB Cloud changefeed")
		}
		log.Info("TiDB Cloud changefeed exists, skipping creation", zap.String("changefeedID", id))
		return nil
	}

	changefeedID, err := r.createChangefeed(ctx)
	if err != nil {
		return errors.Annotate(err, "create TiDB Cloud changefeed")
	}
	if err := r.state.SetChangefeedID(ctx, changefeedID); err != nil {
		return errors.Trace(err)
	}
	log.Info("TiDB Cloud changefeed created",
		zap.String("changefeedID", changefeedID),
		zap.Uint64("checkpointTS", r.state.Snapshot().CheckpointTS))

	return nil
}

func (r *Runner) createExport(ctx context.Context) (string, string, error) {
	client, err := r.tidbCloudClient()
	if err != nil {
		return "", "", err
	}
	cleanSnapshotURI := r.store.CleanSubURI(storage.SnapshotDirName)
	req := buildExportRequest(r.cfg, cleanSnapshotURI, r.cfg.Credential, r.cfg.SnapshotTSO)
	export, err := client.CreateExport(ctx, r.cfg.ClusterID, req)
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
	cleanIncrementURI := r.store.CleanSubURI(storage.IncrementDirName)
	snapshotTSO := r.state.Snapshot().CheckpointTS
	if snapshotTSO == 0 {
		return "", errors.New("checkpoint_ts is required to create TiDB Cloud changefeed")
	}
	req := buildChangefeedRequest(r.cfg, cleanIncrementURI, r.cfg.Credential, strconv.FormatUint(snapshotTSO, 10))
	cf, err := c.CreateChangefeed(ctx, r.cfg.ClusterID, req)
	if err != nil {
		return "", err
	}
	return cf.ChangefeedID, nil
}

func (r *Runner) getChangefeed(ctx context.Context, changefeedID string) error {
	c, err := r.tidbCloudClient()
	if err != nil {
		return err
	}
	_, err = c.GetChangefeed(ctx, r.cfg.ClusterID, changefeedID)
	if err != nil {
		return err
	}
	return nil
}

func (r *Runner) tidbCloudClient() (*tidbcloud.Client, error) {
	if r.client != nil {
		return r.client, nil
	}
	if r.cfg.ClusterID == "" {
		return nil, errors.New("--tidbcloud.cluster-id is required to create or wait on an export/changefeed")
	}
	var opts []tidbcloud.Option
	if r.cfg.Host != "" {
		opts = append(opts, tidbcloud.WithHost(r.cfg.Host))
	}
	c, err := tidbcloud.NewClient(r.cfg.PublicKey, r.cfg.PrivateKey, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.client = c
	return c, nil
}

func buildChangefeedRequest(cfg Config, cleanIncrementURI string, cred *credentials.Value, snapshotTSO string) *tidbcloud.CreateChangefeedRequest {
	return &tidbcloud.CreateChangefeedRequest{
		DisplayName: "tidb2snowflake-incremental",
		Sink: &tidbcloud.Sink{
			Type: tidbcloud.ChangefeedTypeCloudStorage,
			CloudStorage: &tidbcloud.CloudStorageSink{
				Storage: &tidbcloud.CloudStorage{
					Type: tidbcloud.CloudStorageTypeS3,
					S3: &tidbcloud.S3CloudStorage{
						URI:       cleanIncrementURI,
						AuthType:  tidbcloud.S3AuthTypeAccessKey,
						AccessKey: &tidbcloud.S3AccessKey{ID: cred.AccessKeyID, Secret: cred.SecretAccessKey},
					},
				},
				DataFormat: &tidbcloud.CloudStorageDataFormat{
					Protocol: tidbcloud.CloudStorageProtocolCSV,
					CSVConfig: &tidbcloud.CSVConfig{
						IncludeCommitTs: true,
						Encoding:        tidbcloud.BinaryEncodingHex,
					},
				},
				DateSeparator:     tidbcloud.DateSeparatorDay,
				IntervalInSeconds: int(cfg.ChangefeedFlushInterval.Seconds()),
				SizeInMiB:         cfg.ChangefeedFileSizeMiB,
			},
		},
		Filter:        &tidbcloud.ChangefeedFilter{FilterRule: cfg.Tables, Mode: tidbcloud.TableModeForceSync},
		StartPosition: &tidbcloud.StartPosition{Mode: tidbcloud.StartModeFromTSO, TSO: snapshotTSO},
	}
}

func buildExportRequest(cfg Config, cleanSnapshotURI string, cred *credentials.Value, snapshotTSO string) *tidbcloud.CreateExportRequest {
	req := &tidbcloud.CreateExportRequest{
		DisplayName: "tidb2snowflake-snapshot",
		ExportOptions: &tidbcloud.ExportOptions{
			FileType:        tidbcloud.ExportFileTypeCSV,
			Compression:     cfg.SnapshotCompression,
			EscapeBackslash: util.AddressOf(false),
			Filter:          &tidbcloud.ExportFilter{Table: &tidbcloud.ExportFilterTable{Patterns: cfg.Tables}},
			CSVFormat: &tidbcloud.ExportCSVFormat{
				Separator:  ",",
				Delimiter:  util.AddressOf("\""),
				NullValue:  util.AddressOf("\\N"),
				SkipHeader: true,
				Dialect:    tidbcloud.ExportCSVDialectSnowflake,
			},
		},
		Target: &tidbcloud.ExportTarget{
			Type: tidbcloud.ExportTargetTypeS3,
			S3: &tidbcloud.S3Target{
				URI:       cleanSnapshotURI,
				AuthType:  tidbcloud.S3AuthTypeAccessKey,
				AccessKey: &tidbcloud.S3AccessKey{ID: cred.AccessKeyID, Secret: cred.SecretAccessKey},
			},
		},
	}
	if snapshotTSO != "" {
		req.ExportOptions.SnapshotTSO = snapshotTSO
	}
	return req
}
