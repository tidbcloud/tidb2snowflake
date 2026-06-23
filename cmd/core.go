package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	putil "github.com/pingcap/ticdc/pkg/util"
	storage "github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/thediveo/enumflag"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/pkg/utils"
	"github.com/tidbcloud/tidb2snowflake/replicate"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

// snapshotDirName / incrementDirName are the object-storage sub-directories the
// export and changefeed write into (also where the loaders read from). When they
// already contain data, the export/changefeed are assumed to exist (created by a
// previous run or by the user directly) and are not created again.
const (
	snapshotDirName  = "snapshot"
	incrementDirName = "increment"
)

const (
	SnapshotLoadModeBulk    = "bulk"
	SnapshotLoadModePerFile = "per-file"
)

const (
	SnapshotCompressionNone = "none"
	SnapshotCompressionGzip = "gzip"
)

// Config is the full configuration for one replication run.
type Config struct {
	TiDB      *tidb.Config
	Snowflake *snowflake.Config
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
	SnapshotLoadMode        string
	SnapshotCompression     string

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
	log.Info("replication run started",
		zap.Strings("tables", cfg.Tables),
		zap.String("mode", runModeString(cfg.Mode)),
		zap.String("storage", redactURLRawQuery(cfg.StoragePath)),
		zap.String("snapshotLoadMode", snapshotLoadMode(cfg)),
		zap.String("snapshotCompression", snapshotCompression(cfg)),
		zap.Duration("changefeedFlushInterval", cfg.ChangefeedFlushInterval),
		zap.Int("changefeedFileSizeMiB", cfg.ChangefeedFileSizeMiB),
		zap.Duration("pollInterval", cfg.PollInterval),
		zap.Bool("tidbCloudConfigured", cfg.TiDBCloud.ClusterID != ""),
		zap.String("snowflakeDatabase", cfg.Snowflake.Database),
		zap.String("snowflakeSchema", cfg.Snowflake.Schema))

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
	log.Info("replication state loaded",
		zap.String("stateFile", stateFileName),
		zap.Bool("hasExportID", state.ExportID != ""),
		zap.Bool("hasChangefeedID", state.ChangefeedID != ""),
		zap.String("snapshotTSO", state.SnapshotTSO))

	// The TiDB Cloud client is created lazily: when the snapshot/increment data
	// already exists (created by a previous run or by the user directly), no
	// export/changefeed is created or waited on, so no API credentials are needed.
	var cdcClient *tidbcloud.Client
	getClient := func() (*tidbcloud.Client, error) {
		if cdcClient != nil {
			return cdcClient, nil
		}
		if cfg.TiDBCloud.ClusterID == "" {
			return nil, errors.New("--tidbcloud.cluster-id is required to create or wait on an export/changefeed")
		}
		var opts []tidbcloud.Option
		if cfg.TiDBCloud.Host != "" {
			opts = append(opts, tidbcloud.WithHost(cfg.TiDBCloud.Host))
		}
		c, err := tidbcloud.NewClient(cfg.TiDBCloud.PublicKey, cfg.TiDBCloud.PrivateKey, opts...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cdcClient = c
		return c, nil
	}

	// ---- Export: snapshot via OpenAPI (anchors the consistency TSO) ----
	// waitForExport/waitForChangefeed are only set for jobs this run created or
	// resumed; jobs whose data already exists (a previous run, or created by the
	// user directly) are loaded as-is without waiting.
	waitForExport := false
	if cfg.Mode != RunModeIncrementalOnly {
		switch {
		case state.ExportID != "":
			c, err := getClient()
			if err != nil {
				return err
			}
			export, err := c.GetExport(ctx, cfg.TiDBCloud.ClusterID, state.ExportID)
			if err != nil {
				return errors.Annotate(err, "resume export")
			}
			if state.SnapshotTSO == "" {
				state.SnapshotTSO = export.SnapshotTSO
			}
			waitForExport = true
			log.Info("resuming existing export",
				zap.String("exportID", export.ExportID),
				zap.String("snapshotTSO", state.SnapshotTSO))
		default:
			exists, err := dirHasObjects(ctx, store, snapshotDirName)
			if err != nil {
				return errors.Annotate(err, "check snapshot directory")
			}
			if exists {
				log.Info("snapshot data already exists in storage, skipping export creation",
					zap.String("dir", snapshotDirName))
			} else {
				c, err := getClient()
				if err != nil {
					return err
				}
				req := buildExportRequest(cfg, cleanSnapshotURI, cred)
				export, err := c.CreateExport(ctx, cfg.TiDBCloud.ClusterID, req)
				if err != nil {
					return errors.Annotate(err, "create export")
				}
				state.ExportID = export.ExportID
				state.SnapshotTSO = export.SnapshotTSO
				if err := saveState(ctx, store, state); err != nil {
					return errors.Trace(err)
				}
				waitForExport = true
				log.Info("export created",
					zap.String("exportID", export.ExportID),
					zap.String("snapshotTSO", export.SnapshotTSO),
					zap.String("target", redactURLRawQuery(cleanSnapshotURI)))
			}
		}
	}

	// ---- Changefeed: incremental via OpenAPI, started at the snapshot TSO ----
	waitForChangefeed := false
	if cfg.Mode != RunModeSnapshotOnly {
		switch {
		case state.ChangefeedID != "":
			waitForChangefeed = true
			log.Info("resuming existing changefeed",
				zap.String("changefeedID", state.ChangefeedID),
				zap.String("startTSO", state.SnapshotTSO))
		default:
			exists, err := dirHasObjects(ctx, store, incrementDirName)
			if err != nil {
				return errors.Annotate(err, "check increment directory")
			}
			if exists {
				log.Info("increment data already exists in storage, skipping changefeed creation",
					zap.String("dir", incrementDirName))
			} else {
				c, err := getClient()
				if err != nil {
					return err
				}
				req := buildChangefeedRequest(cfg, cleanIncrementURI, cred, state.SnapshotTSO)
				cf, err := c.CreateChangefeed(ctx, cfg.TiDBCloud.ClusterID, req)
				if err != nil {
					return errors.Annotate(err, "create changefeed")
				}
				state.ChangefeedID = cf.ChangefeedID
				if err := saveState(ctx, store, state); err != nil {
					return errors.Trace(err)
				}
				waitForChangefeed = true
				log.Info("changefeed created",
					zap.String("changefeedID", cf.ChangefeedID),
					zap.String("startTSO", state.SnapshotTSO),
					zap.String("target", redactURLRawQuery(cleanIncrementURI)),
					zap.Duration("flushInterval", cfg.ChangefeedFlushInterval),
					zap.Int("fileSizeMiB", cfg.ChangefeedFileSizeMiB))
			}
		}
	}

	// ---- Wait until the jobs this run manages are ready ----
	if waitForExport {
		c, err := getClient()
		if err != nil {
			return err
		}
		log.Info("waiting for export to finish",
			zap.String("exportID", state.ExportID),
			zap.Duration("pollInterval", cfg.PollInterval))
		if _, err := c.WaitExport(ctx, cfg.TiDBCloud.ClusterID, state.ExportID, cfg.PollInterval); err != nil {
			return errors.Annotate(err, "wait export")
		}
		log.Info("export finished", zap.String("exportID", state.ExportID))
	}
	if waitForChangefeed {
		c, err := getClient()
		if err != nil {
			return err
		}
		log.Info("waiting for changefeed to be running",
			zap.String("changefeedID", state.ChangefeedID),
			zap.Duration("pollInterval", cfg.PollInterval))
		if _, err := c.WaitChangefeed(ctx, cfg.TiDBCloud.ClusterID, state.ChangefeedID, cfg.PollInterval); err != nil {
			return errors.Annotate(err, "wait changefeed")
		}
		log.Info("changefeed running", zap.String("changefeedID", state.ChangefeedID))
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
	log.Info("starting Snowflake load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.String("snapshotStorage", safeURLForLog(snapshotURI)),
		zap.String("incrementStorage", safeURLForLog(incrementURI)))

	g, ctx := errgroup.WithContext(ctx)
	for _, table := range cfg.Tables {
		tableFQN := table
		g.Go(func() error {
			log.Info("starting table replication", zap.String("table", tableFQN))
			if err := replicateTable(ctx, cfg, cred, tableFQN, snapshotURI, incrementURI); err != nil {
				metrics.AddCounter(metrics.ErrorCounter, 1, tableFQN)
				log.Error("table replication failed", zap.String("table", tableFQN), zap.Error(err))
				return err
			}
			log.Info("table replication finished", zap.String("table", tableFQN))
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		return err
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
		log.Info("starting snapshot load for table",
			zap.String("table", tableFQN),
			zap.String("stage", fmt.Sprintf("snapshot_external_%s_%s", sourceDatabase, sourceTable)))
		conn, err := snowflake.NewConnector(
			cfg.Snowflake,
			fmt.Sprintf("snapshot_external_%s_%s", sourceDatabase, sourceTable),
			snapshotURI,
			cred,
			snowflake.WithStageFileCompression(snapshotCompression(cfg)),
		)
		if err != nil {
			return errors.Trace(err)
		}
		err = replicate.Snapshot(ctx, conn, tableFQN, snapshotURI, snapshotLoadMode(cfg) == SnapshotLoadModePerFile)
		conn.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}

	if cfg.Mode != RunModeSnapshotOnly {
		log.Info("starting incremental load for table",
			zap.String("table", tableFQN),
			zap.String("stage", fmt.Sprintf("increment_external_%s_%s", sourceDatabase, sourceTable)),
			zap.Duration("scanInterval", cfg.ChangefeedFlushInterval/5))
		conn, err := snowflake.NewConnector(
			cfg.Snowflake,
			fmt.Sprintf("increment_external_%s_%s", sourceDatabase, sourceTable),
			incrementURI,
			cred,
		)
		if err != nil {
			return errors.Trace(err)
		}
		err = replicate.StartReplicateIncrement(ctx, conn, tableFQN, incrementURI, cfg.ChangefeedFlushInterval/5)
		conn.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func snapshotLoadMode(cfg *Config) string {
	if cfg.SnapshotLoadMode == "" {
		return SnapshotLoadModeBulk
	}
	return cfg.SnapshotLoadMode
}

func snapshotCompression(cfg *Config) string {
	if cfg.SnapshotCompression == "" {
		return SnapshotCompressionNone
	}
	return cfg.SnapshotCompression
}

func runModeString(mode RunMode) string {
	if ids, ok := RunModeIds[mode]; ok && len(ids) > 0 {
		return ids[0]
	}
	return fmt.Sprintf("unknown(%d)", mode)
}

func redactURLRawQuery(raw string) string {
	uri, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	uri.RawQuery = ""
	return uri.String()
}

func safeURLForLog(uri *url.URL) string {
	if uri == nil {
		return ""
	}
	clone := *uri
	clone.RawQuery = ""
	return clone.String()
}

func buildExportRequest(cfg *Config, cleanSnapshotURI string, cred *credentials.Value) *tidbcloud.CreateExportRequest {
	req := &tidbcloud.CreateExportRequest{
		DisplayName: "tidb2snowflake-snapshot",
		ExportOptions: &tidbcloud.ExportOptions{
			FileType:    tidbcloud.ExportFileTypeCSV,
			Compression: exportCompression(snapshotCompression(cfg)),
			// Disable backslash escaping to match the Snowflake-dialect CSV the
			// loader's COPY expects.
			EscapeBackslash: boolPtr(false),
			Filter:          &tidbcloud.ExportFilter{Table: &tidbcloud.ExportFilterTable{Patterns: cfg.Tables}},
			CSVFormat: &tidbcloud.ExportCSVFormat{
				Separator: ",",
				Delimiter: strPtr("\""),
				// The Snowflake COPY loader treats `\N` as NULL
				// (snowsql: NULL_IF=('\N')), so the export must emit `\N`.
				NullValue:  strPtr("\\N"),
				SkipHeader: true,
				// Write Snowflake-dialect CSV so the COPY parses special
				// characters and binary columns losslessly.
				Dialect: tidbcloud.ExportCSVDialectSnowflake,
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

func exportCompression(compression string) tidbcloud.ExportCompression {
	switch compression {
	case SnapshotCompressionGzip:
		return tidbcloud.ExportCompressionGzip
	default:
		return tidbcloud.ExportCompressionNone
	}
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
		Filter:        &tidbcloud.ChangefeedFilter{FilterRule: cfg.Tables, Mode: tidbcloud.TableModeForceSync},
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
	return s3URIWithTrailingSlash(&snapshot), s3URIWithTrailingSlash(&increment), nil
}

func s3URIWithTrailingSlash(uri *url.URL) string {
	out := uri.String()
	if !strings.HasSuffix(out, "/") {
		out += "/"
	}
	return out
}

// errWalkStop is a sentinel used to stop a WalkDir early once a match is found.
var errWalkStop = errors.New("stop walk")

// dirHasObjects reports whether the given sub-directory of the storage root
// contains at least one object. It is used to detect a snapshot/increment
// produced by a previous run or created directly by the user, so it is not
// re-created.
func dirHasObjects(ctx context.Context, store storage.Storage, subDir string) (bool, error) {
	found := false
	err := store.WalkDir(ctx, &storage.WalkOption{SubDir: subDir, ListCount: 1}, func(string, int64) error {
		found = true
		return errWalkStop
	})
	if found {
		return true, nil
	}
	if err != nil && errors.Cause(err) != errWalkStop {
		return false, errors.Trace(err)
	}
	return false, nil
}

func loadState(ctx context.Context, store storage.Storage) (*runState, error) {
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

func saveState(ctx context.Context, store storage.Storage, state *runState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return errors.Trace(err)
	}
	if err := store.WriteFile(ctx, stateFileName, data); err != nil {
		return errors.Annotate(err, "write state file")
	}
	log.Info("replication state saved",
		zap.String("stateFile", stateFileName),
		zap.Bool("hasExportID", state.ExportID != ""),
		zap.Bool("hasChangefeedID", state.ChangefeedID != ""),
		zap.String("snapshotTSO", state.SnapshotTSO))
	return nil
}

func strPtr(s string) *string { return &s }

func boolPtr(b bool) *bool { return &b }
