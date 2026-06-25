package cmd

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/thediveo/enumflag"
	"github.com/tidbcloud/tidb2snowflake/incremental"
	"github.com/tidbcloud/tidb2snowflake/pkg/dumpling"
	"github.com/tidbcloud/tidb2snowflake/pkg/metrics"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/ticdc"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/snapshot"
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

// SourceMode selects how source snapshot and incremental streams are created.
type SourceMode enumflag.Flag

const (
	SourceModeTiDBCloud SourceMode = iota
	SourceModeOP
)

var SourceModeIds = map[SourceMode][]string{
	SourceModeTiDBCloud: {"tidbcloud"},
	SourceModeOP:        {"op"},
}

// snapshotDirName / incrementDirName are the object-storage sub-directories the
// export and changefeed write into (also where the loaders read from). When they
// already contain data, the export/changefeed are assumed to exist (created by a
// previous run or by the user directly) and are not created again.
const (
	snapshotDirName  = "snapshot"
	incrementDirName = "increment"
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
	OP        OPConfig

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
	SnapshotCompression     string

	PollInterval time.Duration
	Mode         RunMode
	SourceMode   SourceMode
}

// TiDBCloudConfig is the TiDB Cloud OpenAPI access configuration.
type TiDBCloudConfig struct {
	ClusterID  string
	PublicKey  string
	PrivateKey string
	Host       string
}

type OPConfig struct {
	TiCDCAddress        string
	SnapshotConcurrency int
}

type managedSourceJobResult struct {
	ID          string
	SnapshotTSO string
}

type sourceJobType string

const (
	sourceJobExport     sourceJobType = "export"
	sourceJobChangefeed sourceJobType = "changefeed"
)

type sourcePrepareContext struct {
	store             storeapi.Storage
	state             *state.Manager
	cred              *credentials.Value
	snapshotURI       *url.URL
	incrementURI      *url.URL
	cleanSnapshotURI  string
	cleanIncrementURI string
}

type sourceRunner interface {
	prepare(context.Context, sourcePrepareContext) error
	sourceJobName(sourceJobType) string
	sourceJobStorageDir(sourceJobType) string
	createSourceJob(context.Context, sourcePrepareContext, sourceJobType) (*managedSourceJobResult, error)
	waitSourceJob(context.Context, sourcePrepareContext, sourceJobType, string) (*managedSourceJobResult, error)
}

type tidbCloudSourceRunner struct {
	cfg       *Config
	cdcClient *tidbcloud.Client
}

type opSourceRunner struct {
	cfg       *Config
	cdcClient *ticdc.Client
}

func newSourceRunner(cfg *Config) (sourceRunner, error) {
	switch sourceMode(cfg) {
	case SourceModeTiDBCloud:
		return &tidbCloudSourceRunner{cfg: cfg}, nil
	case SourceModeOP:
		return &opSourceRunner{cfg: cfg}, nil
	default:
		return nil, errors.Errorf("unknown source mode %s", sourceModeString(sourceMode(cfg)))
	}
}

func (r *tidbCloudSourceRunner) sourceJobName(jobType sourceJobType) string {
	switch jobType {
	case sourceJobExport:
		return "TiDB Cloud export"
	case sourceJobChangefeed:
		return "TiDB Cloud changefeed"
	default:
		return string(jobType)
	}
}

func (r *tidbCloudSourceRunner) sourceJobStorageDir(jobType sourceJobType) string {
	return managedSourceJobStorageDir(jobType)
}

func (r *tidbCloudSourceRunner) createSourceJob(
	ctx context.Context,
	prepareCtx sourcePrepareContext,
	jobType sourceJobType,
) (*managedSourceJobResult, error) {
	c, err := r.tidbCloudClient()
	if err != nil {
		return nil, err
	}
	switch jobType {
	case sourceJobExport:
		req := buildExportRequest(r.cfg, prepareCtx.cleanSnapshotURI, prepareCtx.cred)
		if tso := prepareCtx.state.Snapshot().Snapshot.TSO; tso != "" {
			req.ExportOptions.SnapshotTSO = tso
		}
		export, err := c.CreateExport(ctx, r.cfg.TiDBCloud.ClusterID, req)
		if err != nil {
			return nil, err
		}
		return &managedSourceJobResult{ID: export.ExportID, SnapshotTSO: export.SnapshotTSO}, nil
	case sourceJobChangefeed:
		req := buildChangefeedRequest(r.cfg, prepareCtx.cleanIncrementURI, prepareCtx.cred, prepareCtx.state.Snapshot().Snapshot.TSO)
		cf, err := c.CreateChangefeed(ctx, r.cfg.TiDBCloud.ClusterID, req)
		if err != nil {
			return nil, err
		}
		return &managedSourceJobResult{ID: cf.ChangefeedID}, nil
	default:
		return nil, errors.Errorf("unsupported source job %s", jobType)
	}
}

func (r *tidbCloudSourceRunner) waitSourceJob(
	ctx context.Context,
	_ sourcePrepareContext,
	jobType sourceJobType,
	id string,
) (*managedSourceJobResult, error) {
	c, err := r.tidbCloudClient()
	if err != nil {
		return nil, err
	}
	switch jobType {
	case sourceJobExport:
		export, err := c.WaitExport(ctx, r.cfg.TiDBCloud.ClusterID, id, r.cfg.PollInterval)
		if err != nil {
			return nil, err
		}
		return &managedSourceJobResult{ID: export.ExportID, SnapshotTSO: export.SnapshotTSO}, nil
	case sourceJobChangefeed:
		changefeed, err := c.WaitChangefeed(ctx, r.cfg.TiDBCloud.ClusterID, id, r.cfg.PollInterval)
		if err != nil {
			return nil, err
		}
		return &managedSourceJobResult{ID: changefeed.ChangefeedID}, nil
	default:
		return nil, errors.Errorf("unsupported source job %s", jobType)
	}
}

func (r *tidbCloudSourceRunner) tidbCloudClient() (*tidbcloud.Client, error) {
	if r.cdcClient != nil {
		return r.cdcClient, nil
	}
	if r.cfg.TiDBCloud.ClusterID == "" {
		return nil, errors.New("--tidbcloud.cluster-id is required to create or wait on an export/changefeed")
	}
	var opts []tidbcloud.Option
	if r.cfg.TiDBCloud.Host != "" {
		opts = append(opts, tidbcloud.WithHost(r.cfg.TiDBCloud.Host))
	}
	c, err := tidbcloud.NewClient(r.cfg.TiDBCloud.PublicKey, r.cfg.TiDBCloud.PrivateKey, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.cdcClient = c
	return c, nil
}

func (r *opSourceRunner) sourceJobName(jobType sourceJobType) string {
	switch jobType {
	case sourceJobChangefeed:
		return "OP TiCDC changefeed"
	default:
		return string(jobType)
	}
}

func (r *opSourceRunner) sourceJobStorageDir(jobType sourceJobType) string {
	return managedSourceJobStorageDir(jobType)
}

func (r *opSourceRunner) createSourceJob(
	ctx context.Context,
	prepareCtx sourcePrepareContext,
	jobType sourceJobType,
) (*managedSourceJobResult, error) {
	if jobType != sourceJobChangefeed {
		return nil, errors.Errorf("unsupported source job %s", jobType)
	}
	client, err := r.ticdcClient()
	if err != nil {
		return nil, errors.Trace(err)
	}
	startTSO, err := parseOptionalTSO(prepareCtx.state.Snapshot().Snapshot.TSO)
	if err != nil {
		return nil, errors.Trace(err)
	}
	req, err := ticdc.BuildChangefeedConfig(ticdc.ChangefeedConfigOptions{
		Tables:        r.cfg.Tables,
		StorageURI:    prepareCtx.incrementURI,
		StartTSO:      startTSO,
		FlushInterval: r.cfg.ChangefeedFlushInterval,
		FileSizeMiB:   r.cfg.ChangefeedFileSizeMiB,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	cf, err := client.CreateChangefeed(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &managedSourceJobResult{ID: ticdc.ChangefeedID(cf)}, nil
}

func (r *opSourceRunner) waitSourceJob(
	ctx context.Context,
	_ sourcePrepareContext,
	jobType sourceJobType,
	id string,
) (*managedSourceJobResult, error) {
	if jobType != sourceJobChangefeed {
		return nil, errors.Errorf("unsupported source job %s", jobType)
	}
	client, err := r.ticdcClient()
	if err != nil {
		return nil, err
	}
	cf, err := client.WaitChangefeed(ctx, id, r.cfg.PollInterval)
	if err != nil {
		return nil, err
	}
	return &managedSourceJobResult{ID: ticdc.ChangefeedID(cf)}, nil
}

func (r *opSourceRunner) ticdcClient() (*ticdc.Client, error) {
	if r.cdcClient != nil {
		return r.cdcClient, nil
	}
	c, err := ticdc.NewClient(r.cfg.OP.TiCDCAddress)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.cdcClient = c
	return c, nil
}

func managedSourceJobStorageDir(jobType sourceJobType) string {
	switch jobType {
	case sourceJobExport:
		return snapshotDirName
	case sourceJobChangefeed:
		return incrementDirName
	default:
		return ""
	}
}

// Replicate runs one full orchestration: prepare snapshot and incremental data
// from the selected source deployment, then load the resulting object-storage
// files into Snowflake.
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
		zap.String("sourceMode", sourceModeString(sourceMode(cfg))),
		zap.String("storage", redactURLRawQuery(cfg.StoragePath)),
		zap.String("snapshotCompression", snapshotCompression(cfg)),
		zap.Duration("changefeedFlushInterval", cfg.ChangefeedFlushInterval),
		zap.Int("changefeedFileSizeMiB", cfg.ChangefeedFileSizeMiB),
		zap.Duration("pollInterval", cfg.PollInterval),
		zap.Bool("tidbCloudConfigured", cfg.TiDBCloud.ClusterID != ""),
		zap.String("ticdcAddress", cfg.OP.TiCDCAddress),
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

	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
	if err != nil {
		return errors.Annotate(err, "open storage")
	}
	defer store.Close()

	stateManager, err := state.Open(ctx, store, cfg.Tables)
	if err != nil {
		return errors.Trace(err)
	}
	if err := setSnapshotTSO(ctx, stateManager, cfg.SnapshotTSO); err != nil {
		return errors.Trace(err)
	}

	source, err := newSourceRunner(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	if err := source.prepare(ctx, sourcePrepareContext{
		store:             store,
		state:             stateManager,
		cred:              cred,
		snapshotURI:       snapshotURI,
		incrementURI:      incrementURI,
		cleanSnapshotURI:  cleanSnapshotURI,
		cleanIncrementURI: cleanIncrementURI,
	}); err != nil {
		return errors.Trace(err)
	}

	// ---- Load object-storage files into Snowflake ----
	return loadIntoSnowflake(ctx, cfg, cred, storageURI, store, stateManager)
}

func (r *tidbCloudSourceRunner) prepare(
	ctx context.Context,
	prepareCtx sourcePrepareContext,
) error {
	cfg := r.cfg

	if cfg.Mode != RunModeIncrementalOnly && sourceJobID(prepareCtx.state.Snapshot(), sourceJobExport) == "" {
		if err := loadExistingSnapshotTSO(ctx, prepareCtx.store, prepareCtx.state); err != nil {
			return errors.Trace(err)
		}
	}

	// ---- Export: snapshot via OpenAPI (anchors the consistency TSO) ----
	if cfg.Mode != RunModeIncrementalOnly && !prepareCtx.state.Snapshot().Snapshot.Finished {
		if err := ensureManagedSourceJob(ctx, prepareCtx, r, sourceJobExport); err != nil {
			return errors.Trace(err)
		}
	}
	if cfg.Mode != RunModeIncrementalOnly {
		if prepareCtx.state.Snapshot().Snapshot.Finished {
			if err := loadExistingSnapshotTSO(ctx, prepareCtx.store, prepareCtx.state); err != nil {
				return errors.Trace(err)
			}
		} else if err := loadSnapshotTSOFromMetadata(ctx, prepareCtx.store, prepareCtx.state); err != nil {
			return errors.Trace(err)
		}
	}
	if cfg.Mode == RunModeFull && prepareCtx.state.Snapshot().Snapshot.TSO == "" {
		return errors.New("snapshot.tso is required before creating a changefeed for full replication")
	}

	// ---- Changefeed: incremental via OpenAPI, started at the snapshot TSO ----
	if cfg.Mode != RunModeSnapshotOnly {
		if err := ensureManagedSourceJob(ctx, prepareCtx, r, sourceJobChangefeed); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func ensureManagedSourceJob(
	ctx context.Context,
	prepareCtx sourcePrepareContext,
	source sourceRunner,
	jobType sourceJobType,
) error {
	store := prepareCtx.store
	jobName := source.sourceJobName(jobType)

	storageDir := source.sourceJobStorageDir(jobType)
	if storageDir == "" {
		return errors.Errorf("unsupported source job %s", jobType)
	}
	if id := sourceJobID(prepareCtx.state.Snapshot(), jobType); id != "" {
		log.Info("waiting for existing source job from state",
			zap.String("job", jobName),
			zap.String("jobID", id))
		res, err := source.waitSourceJob(ctx, prepareCtx, jobType, id)
		if err != nil {
			return errors.Annotatef(err, "wait %s", jobName)
		}
		return errors.Trace(updateManagedSourceJobState(ctx, prepareCtx.state, jobType, res))
	}
	exists, err := dirHasObjects(ctx, store, storageDir)
	if err != nil {
		return errors.Annotatef(err, "check %s directory", storageDir)
	}
	if exists {
		log.Info("source data already exists in storage, skipping source job creation",
			zap.String("job", jobName),
			zap.String("dir", storageDir))
		return nil
	}

	res, err := source.createSourceJob(ctx, prepareCtx, jobType)
	if err != nil {
		return errors.Annotatef(err, "create %s", jobName)
	}
	if res == nil || res.ID == "" {
		return errors.Errorf("create %s returned empty id", jobName)
	}
	if err := updateManagedSourceJobState(ctx, prepareCtx.state, jobType, res); err != nil {
		return errors.Trace(err)
	}
	snapshotTSO := prepareCtx.state.Snapshot().Snapshot.TSO
	log.Info("source job created",
		zap.String("job", jobName),
		zap.String("jobID", res.ID),
		zap.String("snapshotTSO", snapshotTSO))

	log.Info("waiting for source job",
		zap.String("job", jobName),
		zap.String("jobID", res.ID))
	waitResult, err := source.waitSourceJob(ctx, prepareCtx, jobType, res.ID)
	if err != nil {
		return errors.Annotatef(err, "wait %s", jobName)
	}
	if err := updateManagedSourceJobState(ctx, prepareCtx.state, jobType, waitResult); err != nil {
		return errors.Trace(err)
	}
	log.Info("source job ready",
		zap.String("job", jobName),
		zap.String("jobID", res.ID))
	return nil
}

func sourceJobID(st state.State, jobType sourceJobType) string {
	switch jobType {
	case sourceJobExport:
		return st.TaskInfo.ExportID
	case sourceJobChangefeed:
		return st.TaskInfo.ChangefeedID
	default:
		return ""
	}
}

func updateManagedSourceJobState(ctx context.Context, manager *state.Manager, jobType sourceJobType, res *managedSourceJobResult) error {
	if res == nil {
		return nil
	}
	return manager.Update(ctx, func(st *state.State) error {
		switch jobType {
		case sourceJobExport:
			if res.ID != "" {
				st.TaskInfo.ExportID = res.ID
			}
		case sourceJobChangefeed:
			if res.ID != "" {
				st.TaskInfo.ChangefeedID = res.ID
			}
		default:
			return errors.Errorf("unsupported source job %s", jobType)
		}
		if res.SnapshotTSO != "" {
			if st.Snapshot.TSO != "" && st.Snapshot.TSO != res.SnapshotTSO {
				return errors.Errorf("snapshot.tso mismatch: state %s, source job %s", st.Snapshot.TSO, res.SnapshotTSO)
			}
			st.Snapshot.TSO = res.SnapshotTSO
		}
		return nil
	})
}

func loadExistingSnapshotTSO(ctx context.Context, store storeapi.Storage, manager *state.Manager) error {
	exists, err := dirHasObjects(ctx, store, snapshotDirName)
	if err != nil {
		return errors.Annotate(err, "check snapshot directory")
	}
	if !exists {
		return nil
	}
	return loadSnapshotTSOFromMetadata(ctx, store, manager)
}

func loadSnapshotTSOFromMetadata(ctx context.Context, store storeapi.Storage, manager *state.Manager) error {
	metadataPath := path.Join(snapshotDirName, "metadata")
	exists, err := store.FileExists(ctx, metadataPath)
	if err != nil {
		return errors.Annotatef(err, "check snapshot metadata %s", metadataPath)
	}
	if !exists {
		return errors.Errorf("snapshot data exists but %s is missing", metadataPath)
	}
	data, err := store.ReadFile(ctx, metadataPath)
	if err != nil {
		return errors.Annotatef(err, "read snapshot metadata %s", metadataPath)
	}
	tso, err := snapshotTSOFromMetadata(data)
	if err != nil {
		return errors.Annotatef(err, "parse snapshot metadata %s", metadataPath)
	}
	return setSnapshotTSO(ctx, manager, tso)
}

func snapshotTSOFromMetadata(data []byte) (string, error) {
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Pos:") {
			continue
		}
		tso := strings.TrimSpace(strings.TrimPrefix(line, "Pos:"))
		if tso == "" {
			return "", errors.New("metadata Pos is empty")
		}
		if _, err := strconv.ParseUint(tso, 10, 64); err != nil {
			return "", errors.Annotatef(err, "parse metadata Pos %q", tso)
		}
		return tso, nil
	}
	return "", errors.New("metadata Pos is missing")
}

func setSnapshotTSO(ctx context.Context, manager *state.Manager, tso string) error {
	if tso == "" {
		return nil
	}
	if _, err := strconv.ParseUint(tso, 10, 64); err != nil {
		return errors.Annotatef(err, "parse snapshot.tso %q", tso)
	}
	return manager.Update(ctx, func(st *state.State) error {
		if st.Snapshot.TSO != "" && st.Snapshot.TSO != tso {
			return errors.Errorf("snapshot.tso mismatch: state %s, new %s", st.Snapshot.TSO, tso)
		}
		st.Snapshot.TSO = tso
		return nil
	})
}

func markSnapshotFinished(ctx context.Context, manager *state.Manager) error {
	return manager.Update(ctx, func(st *state.State) error {
		if st.Snapshot.TSO == "" {
			log.Panic("snapshot.tso is empty after snapshot load")
		}
		tso, err := strconv.ParseUint(st.Snapshot.TSO, 10, 64)
		if err != nil {
			log.Panic("invalid snapshot.tso after snapshot load",
				zap.String("snapshotTSO", st.Snapshot.TSO),
				zap.Error(err))
		}
		st.Snapshot.Finished = true
		if st.Incremental.CheckpointTS == 0 {
			st.Incremental.CheckpointTS = tso
		}
		return nil
	})
}

func (r *opSourceRunner) prepare(
	ctx context.Context,
	prepareCtx sourcePrepareContext,
) error {
	cfg := r.cfg
	store := prepareCtx.store

	var snapshotExists bool
	if cfg.Mode != RunModeIncrementalOnly {
		exists, err := dirHasObjects(ctx, store, snapshotDirName)
		if err != nil {
			return errors.Annotate(err, "check snapshot directory")
		}
		snapshotExists = exists
		if snapshotExists {
			if err := loadSnapshotTSOFromMetadata(ctx, store, prepareCtx.state); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if cfg.Mode == RunModeFull && !snapshotExists && prepareCtx.state.Snapshot().Snapshot.TSO == "" {
		tso, err := tidb.GetCurrentTSO(cfg.TiDB)
		if err != nil {
			return errors.Annotate(err, "get current TiDB TSO")
		}
		if err := setSnapshotTSO(ctx, prepareCtx.state, strconv.FormatUint(tso, 10)); err != nil {
			return errors.Trace(err)
		}
	}

	if cfg.Mode != RunModeSnapshotOnly {
		if err := ensureManagedSourceJob(ctx, prepareCtx, r, sourceJobChangefeed); err != nil {
			return errors.Trace(err)
		}
	}

	if cfg.Mode != RunModeIncrementalOnly {
		if prepareCtx.state.Snapshot().Snapshot.Finished {
			log.Info("snapshot already marked finished in state, skipping OP Dumpling snapshot dump")
			return nil
		}
		if snapshotExists {
			log.Info("snapshot data already exists in storage, skipping OP Dumpling snapshot dump",
				zap.String("dir", snapshotDirName))
			return nil
		}
		log.Info("dumping OP TiDB snapshot with Dumpling",
			zap.String("snapshotTSO", prepareCtx.state.Snapshot().Snapshot.TSO),
			zap.String("target", safeURLForLog(prepareCtx.snapshotURI)),
			zap.Int("concurrency", opSnapshotConcurrency(cfg)))
		if err := dumpling.Run(ctx, cfg.TiDB, dumpling.Config{
			Concurrency:  opSnapshotConcurrency(cfg),
			StorageURI:   prepareCtx.snapshotURI,
			SnapshotTSO:  prepareCtx.state.Snapshot().Snapshot.TSO,
			Tables:       cfg.Tables,
			Compression:  snapshotCompression(cfg),
			CSVNullValue: "\\N",
			OnProgress: func(dumpedRows, totalRows int64) {
				log.Info("OP Dumpling snapshot dump progress",
					zap.Int64("dumpedRows", dumpedRows),
					zap.Int64("estimatedTotalRows", totalRows))
			},
		}); err != nil {
			return errors.Trace(err)
		}
		if err := loadSnapshotTSOFromMetadata(ctx, store, prepareCtx.state); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func loadIntoSnowflake(
	ctx context.Context,
	cfg *Config,
	cred *credentials.Value,
	storageURI *url.URL,
	store storeapi.Storage,
	stateManager *state.Manager,
) error {
	metrics.TableNumGauge.Add(float64(len(cfg.Tables)))
	log.Info("starting Snowflake load phase",
		zap.Int("tableCount", len(cfg.Tables)),
		zap.String("storage", safeURLForLog(storageURI)))

	if cfg.Mode != RunModeIncrementalOnly {
		if stateManager.Snapshot().Snapshot.Finished {
			log.Info("snapshot already marked finished in state, skipping Snowflake snapshot load")
		} else {
			if err := snapshot.Load(ctx, snapshot.Config{
				Snowflake:   cfg.Snowflake,
				Credential:  cred,
				Tables:      cfg.Tables,
				StorageURI:  storageURI,
				StorageDir:  snapshotDirName,
				Compression: snapshotCompression(cfg),
			}, store); err != nil {
				return errors.Trace(err)
			}
			if err := markSnapshotFinished(ctx, stateManager); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if cfg.Mode != RunModeSnapshotOnly {
		if err := incremental.Load(ctx, incremental.Config{
			Snowflake:    cfg.Snowflake,
			Credential:   cred,
			Tables:       cfg.Tables,
			StorageURI:   storageURI,
			StorageDir:   incrementDirName,
			ScanInterval: cfg.ChangefeedFlushInterval / 5,
			State:        stateManager,
		}, store); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func snapshotCompression(cfg *Config) string {
	if cfg.SnapshotCompression == "" {
		return SnapshotCompressionNone
	}
	return cfg.SnapshotCompression
}

func sourceMode(cfg *Config) SourceMode {
	return cfg.SourceMode
}

func runModeString(mode RunMode) string {
	if ids, ok := RunModeIds[mode]; ok && len(ids) > 0 {
		return ids[0]
	}
	return fmt.Sprintf("unknown(%d)", mode)
}

func sourceModeString(mode SourceMode) string {
	if ids, ok := SourceModeIds[mode]; ok && len(ids) > 0 {
		return ids[0]
	}
	return fmt.Sprintf("unknown(%d)", mode)
}

func opSnapshotConcurrency(cfg *Config) int {
	if cfg.OP.SnapshotConcurrency <= 0 {
		return 8
	}
	return cfg.OP.SnapshotConcurrency
}

func parseOptionalTSO(tso string) (uint64, error) {
	if tso == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(tso, 10, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "parse snapshot TSO %q", tso)
	}
	return parsed, nil
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
			EscapeBackslash: util.AddressOf(false),
			Filter:          &tidbcloud.ExportFilter{Table: &tidbcloud.ExportFilterTable{Patterns: cfg.Tables}},
			CSVFormat: &tidbcloud.ExportCSVFormat{
				Separator: ",",
				Delimiter: util.AddressOf("\""),
				// The Snowflake COPY loader treats `\N` as NULL
				// (snowsql: NULL_IF=('\N')), so the export must emit `\N`.
				NullValue:  util.AddressOf("\\N"),
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
func dirHasObjects(ctx context.Context, store storeapi.Storage, subDir string) (bool, error) {
	found := false
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: subDir, ListCount: 1}, func(string, int64) error {
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
