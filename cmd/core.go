package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2snowflake/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2snowflake/pkg/metrics"
	"github.com/pingcap-inc/tidb2snowflake/pkg/snowsql"
	"github.com/pingcap-inc/tidb2snowflake/pkg/tidbcloud"
	"github.com/pingcap-inc/tidb2snowflake/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2snowflake/pkg/utils"
	"github.com/pingcap-inc/tidb2snowflake/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/thediveo/enumflag"
	"go.uber.org/zap"
)

// RunMode selects which phases of replication to run.
type RunMode enumflag.Flag

const (
	RunModeFull RunMode = iota
	RunModeSnapshotOnly
	RunModeIncrementalOnly
)

var RunModeIds = map[RunMode][]string{
	RunModeFull:            {"full"},
	RunModeSnapshotOnly:    {"snapshot-only"},
	RunModeIncrementalOnly: {"incremental-only"},
}

// stateFileName holds the export/changefeed identifiers so a re-run can resume
// instead of creating duplicates.
const stateFileName = "tidb2snowflake.state.json"

// Config is the full configuration for one replication run.
type Config struct {
	TiDB      *tidbsql.TiDBConfig
	Snowflake *snowsql.SnowflakeConfig
	TiDBCloud TiDBCloudConfig

	Tables       []string
	StoragePath  string
	AWSAccessKey string
	AWSSecretKey string

	// SnapshotTSO, when set, pins the export snapshot (and the changefeed start
	// position) to this exact TiDB TSO. When empty, the server picks the
	// snapshot at export-creation time and the tool reads the effective TSO back.
	SnapshotTSO string

	ChangefeedFlushInterval time.Duration
	ChangefeedFileSizeMiB   int

	PollInterval time.Duration
	Mode         RunMode
}

// TiDBCloudConfig is the TiDB Cloud OpenAPI access configuration.
type TiDBCloudConfig struct {
	ClusterID  string
	PublicKey  string
	PrivateKey string
	Host       string
}

// runState is persisted to object storage between runs for idempotent resume.
type runState struct {
	ExportID     string `json:"exportId,omitempty"`
	ChangefeedID string `json:"changefeedId,omitempty"`
	SnapshotTSO  string `json:"snapshotTso,omitempty"`
}

// Replicate runs one full orchestration: drive a snapshot export and an
// incremental changefeed through the TiDB Cloud OpenAPI, then load the resulting
// object-storage files into Snowflake.
func Replicate(ctx context.Context, cfg *Config) error {
	if len(cfg.Tables) == 0 {
		return errors.New("no tables specified")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 10 * time.Second
	}

	cred := &credentials.Value{
		AccessKeyID:     cfg.AWSAccessKey,
		SecretAccessKey: cfg.AWSSecretKey,
	}

	storageURI, err := getS3URIWithCredentials(cfg.StoragePath, cred)
	if err != nil {
		return errors.Trace(err)
	}
	snapshotURI, incrementURI, err := genSnapshotAndIncrementURIs(storageURI)
	if err != nil {
		return errors.Trace(err)
	}
	cleanSnapshotURI, cleanIncrementURI, err := genCleanSubURIs(cfg.StoragePath)
	if err != nil {
		return errors.Trace(err)
	}

	store, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return errors.Annotate(err, "open storage")
	}
	state, err := loadState(ctx, store)
	if err != nil {
		return errors.Trace(err)
	}

	var clientOpts []tidbcloud.Option
	if cfg.TiDBCloud.Host != "" {
		clientOpts = append(clientOpts, tidbcloud.WithHost(cfg.TiDBCloud.Host))
	}
	client, err := tidbcloud.NewClient(cfg.TiDBCloud.PublicKey, cfg.TiDBCloud.PrivateKey, clientOpts...)
	if err != nil {
		return errors.Trace(err)
	}

	// ---- Export: snapshot via OpenAPI (anchors the consistency TSO) ----
	if cfg.Mode != RunModeIncrementalOnly {
		if state.ExportID == "" {
			req := buildExportRequest(cfg, cleanSnapshotURI, cred)
			export, err := client.CreateExport(ctx, cfg.TiDBCloud.ClusterID, req)
			if err != nil {
				return errors.Annotate(err, "create export")
			}
			state.ExportID = export.ExportID
			state.SnapshotTSO = export.SnapshotTSO
			if err := saveState(ctx, store, state); err != nil {
				return errors.Trace(err)
			}
			log.Info("export created",
				zap.String("exportID", export.ExportID),
				zap.String("snapshotTSO", export.SnapshotTSO))
		} else {
			export, err := client.GetExport(ctx, cfg.TiDBCloud.ClusterID, state.ExportID)
			if err != nil {
				return errors.Annotate(err, "resume export")
			}
			if state.SnapshotTSO == "" {
				state.SnapshotTSO = export.SnapshotTSO
			}
			log.Info("resuming existing export", zap.String("exportID", export.ExportID))
		}
	}

	// ---- Changefeed: incremental via OpenAPI, started at the snapshot TSO ----
	if cfg.Mode != RunModeSnapshotOnly {
		if state.ChangefeedID == "" {
			req := buildChangefeedRequest(cfg, cleanIncrementURI, cred, state.SnapshotTSO)
			cf, err := client.CreateChangefeed(ctx, cfg.TiDBCloud.ClusterID, req)
			if err != nil {
				return errors.Annotate(err, "create changefeed")
			}
			state.ChangefeedID = cf.ChangefeedID
			if err := saveState(ctx, store, state); err != nil {
				return errors.Trace(err)
			}
			log.Info("changefeed created",
				zap.String("changefeedID", cf.ChangefeedID),
				zap.String("startTSO", state.SnapshotTSO))
		} else {
			log.Info("resuming existing changefeed", zap.String("changefeedID", state.ChangefeedID))
		}
	}

	// ---- Wait until the managed jobs are ready ----
	if cfg.Mode != RunModeIncrementalOnly {
		log.Info("waiting for export to finish", zap.String("exportID", state.ExportID))
		if _, err := client.WaitExport(ctx, cfg.TiDBCloud.ClusterID, state.ExportID, cfg.PollInterval); err != nil {
			return errors.Annotate(err, "wait export")
		}
		log.Info("export finished")
	}
	if cfg.Mode != RunModeSnapshotOnly {
		log.Info("waiting for changefeed to be running", zap.String("changefeedID", state.ChangefeedID))
		if _, err := client.WaitChangefeed(ctx, cfg.TiDBCloud.ClusterID, state.ChangefeedID, cfg.PollInterval); err != nil {
			return errors.Annotate(err, "wait changefeed")
		}
		log.Info("changefeed running")
	}

	// ---- Load object-storage files into Snowflake ----
	return loadIntoSnowflake(ctx, cfg, cred, snapshotURI, incrementURI)
}

func loadIntoSnowflake(
	ctx context.Context,
	cfg *Config,
	cred *credentials.Value,
	snapshotURI, incrementURI *url.URL,
) error {
	metrics.TableNumGauge.Add(float64(len(cfg.Tables)))

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		errList []error
	)
	for _, table := range cfg.Tables {
		wg.Add(1)
		go func(tableFQN string) {
			defer wg.Done()
			if err := replicateTable(ctx, cfg, cred, tableFQN, snapshotURI, incrementURI); err != nil {
				metrics.AddCounter(metrics.ErrorCounter, 1, tableFQN)
				mu.Lock()
				errList = append(errList, errors.Annotatef(err, "table %s", tableFQN))
				mu.Unlock()
			}
		}(table)
	}
	wg.Wait()

	if len(errList) > 0 {
		return errList[0]
	}
	return nil
}

func replicateTable(
	ctx context.Context,
	cfg *Config,
	cred *credentials.Value,
	tableFQN string,
	snapshotURI, incrementURI *url.URL,
) error {
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)

	if cfg.Mode != RunModeIncrementalOnly {
		conn, err := snowsql.NewSnowflakeConnector(
			cfg.Snowflake,
			fmt.Sprintf("snapshot_external_%s_%s", sourceDatabase, sourceTable),
			snapshotURI,
			cred,
		)
		if err != nil {
			return errors.Trace(err)
		}
		err = replicate.StartReplicateSnapshot(ctx, conn, tableFQN, cfg.TiDB, snapshotURI, true)
		conn.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}

	if cfg.Mode != RunModeSnapshotOnly {
		conn, err := snowsql.NewSnowflakeConnector(
			cfg.Snowflake,
			fmt.Sprintf("increment_external_%s_%s", sourceDatabase, sourceTable),
			incrementURI,
			cred,
		)
		if err != nil {
			return errors.Trace(err)
		}
		var connIface coreinterfaces.Connector = conn
		err = replicate.StartReplicateIncrement(ctx, connIface, tableFQN, incrementURI, cfg.ChangefeedFlushInterval/5)
		conn.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func buildExportRequest(cfg *Config, cleanSnapshotURI string, cred *credentials.Value) *tidbcloud.CreateExportRequest {
	req := &tidbcloud.CreateExportRequest{
		DisplayName: "tidb2snowflake-snapshot",
		ExportOptions: &tidbcloud.ExportOptions{
			FileType:    tidbcloud.ExportFileTypeCSV,
			Compression: tidbcloud.ExportCompressionNone,
			Filter:      &tidbcloud.ExportFilter{Table: &tidbcloud.ExportFilterTable{Patterns: cfg.Tables}},
			CSVFormat: &tidbcloud.ExportCSVFormat{
				Separator: ",",
				Delimiter: strPtr("\""),
				// The Snowflake COPY loader treats `\N` as NULL
				// (snowsql: NULL_IF=('\N')), so the export must emit `\N`.
				NullValue:  strPtr("\\N"),
				SkipHeader: true,
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
	if cfg.SnapshotTSO != "" {
		req.ExportOptions.SnapshotTSO = cfg.SnapshotTSO
	}
	return req
}

func buildChangefeedRequest(cfg *Config, cleanIncrementURI string, cred *credentials.Value, snapshotTSO string) *tidbcloud.CreateChangefeedRequest {
	start := &tidbcloud.StartPosition{}
	if snapshotTSO != "" {
		start.Mode = tidbcloud.StartModeFromTSO
		start.TSO = snapshotTSO
	} else {
		start.Mode = tidbcloud.StartModeFromNow
	}
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
				// The increment loader maps columns by ID across DDL changes.
				OutputColumnID: true,
			},
		},
		Filter:        &tidbcloud.ChangefeedFilter{FilterRule: cfg.Tables},
		StartPosition: start,
	}
}

// ---- storage helpers ----

func getS3URIWithCredentials(storagePath string, cred *credentials.Value) (*url.URL, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Annotate(err, "parse storage path")
	}
	if uri.Scheme != "s3" {
		return nil, errors.New("only s3 storage is supported")
	}
	values := url.Values{}
	values.Add("access-key", cred.AccessKeyID)
	values.Add("secret-access-key", cred.SecretAccessKey)
	if cred.SessionToken != "" {
		values.Add("session-token", cred.SessionToken)
	}
	uri.RawQuery = values.Encode()
	return uri, nil
}

func genSnapshotAndIncrementURIs(storageURI *url.URL) (*url.URL, *url.URL, error) {
	snapshotURI := *storageURI
	incrementURI := *storageURI
	var err error
	snapshotURI.Path, err = url.JoinPath(storageURI.Path, "snapshot")
	if err != nil {
		return nil, nil, errors.Annotate(err, "join snapshot path")
	}
	incrementURI.Path, err = url.JoinPath(storageURI.Path, "increment")
	if err != nil {
		return nil, nil, errors.Annotate(err, "join increment path")
	}
	return &snapshotURI, &incrementURI, nil
}

// genCleanSubURIs returns the snapshot/increment S3 URIs without credentials in
// the query string, suitable for the OpenAPI target.s3.uri field (credentials
// are passed separately via the access key).
func genCleanSubURIs(storagePath string) (string, string, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return "", "", errors.Annotate(err, "parse storage path")
	}
	snapshot := *uri
	increment := *uri
	snapshot.Path, err = url.JoinPath(uri.Path, "snapshot")
	if err != nil {
		return "", "", errors.Trace(err)
	}
	increment.Path, err = url.JoinPath(uri.Path, "increment")
	if err != nil {
		return "", "", errors.Trace(err)
	}
	return snapshot.String(), increment.String(), nil
}

func loadState(ctx context.Context, store storage.ExternalStorage) (*runState, error) {
	exists, err := store.FileExists(ctx, stateFileName)
	if err != nil {
		return nil, errors.Annotate(err, "check state file")
	}
	if !exists {
		return &runState{}, nil
	}
	data, err := store.ReadFile(ctx, stateFileName)
	if err != nil {
		return nil, errors.Annotate(err, "read state file")
	}
	var s runState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, errors.Annotate(err, "decode state file")
	}
	return &s, nil
}

func saveState(ctx context.Context, store storage.ExternalStorage, state *runState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return errors.Trace(err)
	}
	if err := store.WriteFile(ctx, stateFileName, data); err != nil {
		return errors.Annotate(err, "write state file")
	}
	return nil
}

func strPtr(s string) *string { return &s }
