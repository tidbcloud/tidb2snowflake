package cmd

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
)

func testCred() *credentials.Value {
	return &credentials.Value{AccessKeyID: "AKIA", SecretAccessKey: "secret"}
}

func baseConfig() *Config {
	return &Config{
		Tables:                  []string{"db1.t1", "db2.t2"},
		StoragePath:             "s3://bucket/path",
		AWSAccessKey:            "AKIA",
		AWSSecretKey:            "secret",
		ChangefeedFlushInterval: 60 * time.Second,
		ChangefeedFileSizeMiB:   64,
	}
}

func TestBuildExportRequest(t *testing.T) {
	cfg := baseConfig()
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred())

	require.Equal(t, tidbcloud.ExportFileTypeCSV, req.ExportOptions.FileType)
	require.Equal(t, tidbcloud.ExportCompressionNone, req.ExportOptions.Compression)
	require.Equal(t, []string{"db1.t1", "db2.t2"}, req.ExportOptions.Filter.Table.Patterns)
	require.Equal(t, ",", req.ExportOptions.CSVFormat.Separator)
	require.Equal(t, "\"", *req.ExportOptions.CSVFormat.Delimiter)
	require.Equal(t, "\\N", *req.ExportOptions.CSVFormat.NullValue)
	require.True(t, req.ExportOptions.CSVFormat.SkipHeader)
	require.Equal(t, tidbcloud.ExportCSVDialectSnowflake, req.ExportOptions.CSVFormat.Dialect)
	require.NotNil(t, req.ExportOptions.EscapeBackslash)
	require.False(t, *req.ExportOptions.EscapeBackslash)
	require.Empty(t, req.ExportOptions.SnapshotTSO)

	require.Equal(t, tidbcloud.ExportTargetTypeS3, req.Target.Type)
	require.Equal(t, "s3://bucket/path/snapshot/", req.Target.S3.URI)
	require.Equal(t, tidbcloud.S3AuthTypeAccessKey, req.Target.S3.AuthType)
	require.Equal(t, "AKIA", req.Target.S3.AccessKey.ID)
	require.Equal(t, "secret", req.Target.S3.AccessKey.Secret)
}

func TestBuildExportRequest_PinnedSnapshotTSO(t *testing.T) {
	cfg := baseConfig()
	cfg.SnapshotTSO = "449023000000000000"
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred())
	require.Equal(t, "449023000000000000", req.ExportOptions.SnapshotTSO)
}

func TestBuildExportRequest_GzipCompression(t *testing.T) {
	cfg := baseConfig()
	cfg.SnapshotCompression = SnapshotCompressionGzip
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred())
	require.Equal(t, tidbcloud.ExportCompressionGzip, req.ExportOptions.Compression)
}

func TestSnapshotCompressionDefault(t *testing.T) {
	require.Equal(t, SnapshotCompressionNone, snapshotCompression(&Config{}))
	require.Equal(t, SnapshotCompressionGzip, snapshotCompression(&Config{SnapshotCompression: SnapshotCompressionGzip}))
}

func TestSourceModeDefault(t *testing.T) {
	require.Equal(t, SourceModeTiDBCloud, sourceMode(&Config{}))
	require.Equal(t, SourceModeOP, sourceMode(&Config{SourceMode: SourceModeOP}))
	require.Equal(t, "tidbcloud", sourceModeString(SourceModeTiDBCloud))
	require.Equal(t, "op", sourceModeString(SourceModeOP))
}

func TestNewSourceRunnerSelectsImplementation(t *testing.T) {
	runner, err := newSourceRunner(&Config{})
	require.NoError(t, err)
	require.IsType(t, &tidbCloudSourceRunner{}, runner)

	runner, err = newSourceRunner(&Config{SourceMode: SourceModeOP})
	require.NoError(t, err)
	require.IsType(t, &opSourceRunner{}, runner)
}

func TestBuildChangefeedRequest_FromTSO(t *testing.T) {
	cfg := baseConfig()
	req := buildChangefeedRequest(cfg, "s3://bucket/path/increment/", testCred(), "449023000000000000")

	require.Equal(t, tidbcloud.ChangefeedTypeCloudStorage, req.Sink.Type)
	cs := req.Sink.CloudStorage
	require.Equal(t, tidbcloud.CloudStorageTypeS3, cs.Storage.Type)
	require.Equal(t, "s3://bucket/path/increment/", cs.Storage.S3.URI)
	require.Equal(t, tidbcloud.S3AuthTypeAccessKey, cs.Storage.S3.AuthType)
	require.Equal(t, "AKIA", cs.Storage.S3.AccessKey.ID)
	require.Equal(t, tidbcloud.CloudStorageProtocolCSV, cs.DataFormat.Protocol)
	require.True(t, cs.DataFormat.CSVConfig.IncludeCommitTs)
	require.Equal(t, tidbcloud.BinaryEncodingHex, cs.DataFormat.CSVConfig.Encoding)
	require.Equal(t, tidbcloud.DateSeparatorDay, cs.DateSeparator)
	require.Equal(t, 60, cs.IntervalInSeconds)
	require.Equal(t, 64, cs.SizeInMiB)
	require.True(t, cs.OutputColumnID)

	require.Equal(t, []string{"db1.t1", "db2.t2"}, req.Filter.FilterRule)
	require.Equal(t, tidbcloud.TableModeForceSync, req.Filter.Mode)
	require.Equal(t, tidbcloud.StartModeFromTSO, req.StartPosition.Mode)
	require.Equal(t, "449023000000000000", req.StartPosition.TSO)
}

func TestBuildChangefeedRequest_FromNowWhenNoTSO(t *testing.T) {
	cfg := baseConfig()
	req := buildChangefeedRequest(cfg, "s3://bucket/path/increment", testCred(), "")
	require.Equal(t, tidbcloud.StartModeFromNow, req.StartPosition.Mode)
	require.Empty(t, req.StartPosition.TSO)
}

func TestGenCleanSubURIs(t *testing.T) {
	snap, incr, err := genCleanSubURIs("s3://bucket/path")
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/path/snapshot/", snap)
	require.Equal(t, "s3://bucket/path/increment/", incr)
	// no credentials leaked into the API URI
	require.NotContains(t, snap, "access-key")
	require.NotContains(t, incr, "secret")
}

func TestGetS3URIWithCredentials(t *testing.T) {
	uri, err := getS3URIWithCredentials("s3://bucket/path", testCred())
	require.NoError(t, err)
	require.Equal(t, "s3", uri.Scheme)
	require.Contains(t, uri.RawQuery, "access-key=AKIA")
	require.Contains(t, uri.RawQuery, "secret-access-key=secret")

	_, err = getS3URIWithCredentials("gcs://bucket/path", testCred())
	require.Error(t, err)
}

func TestGenSnapshotAndIncrementURIs(t *testing.T) {
	storageURI, err := getS3URIWithCredentials("s3://bucket/path", testCred())
	require.NoError(t, err)
	snap, incr, err := genSnapshotAndIncrementURIs(storageURI)
	require.NoError(t, err)
	require.Equal(t, "/path/snapshot", snap.Path)
	require.Equal(t, "/path/increment", incr.Path)
	// credentials preserved for the loader
	require.Contains(t, snap.RawQuery, "access-key=AKIA")
}

func TestDirHasObjects(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)

	// empty dirs -> no objects
	has, err := dirHasObjects(ctx, store, snapshotDirName)
	require.NoError(t, err)
	require.False(t, has)

	// write a file under snapshot/ -> detected; increment/ still empty
	require.NoError(t, store.WriteFile(ctx, snapshotDirName+"/db1.t1.000000.csv", []byte("a,b\n")))

	has, err = dirHasObjects(ctx, store, snapshotDirName)
	require.NoError(t, err)
	require.True(t, has)

	has, err = dirHasObjects(ctx, store, incrementDirName)
	require.NoError(t, err)
	require.False(t, has)

	// a root-level metadata file must not count as snapshot/increment content
	require.NoError(t, store.WriteFile(ctx, "metadata.json", []byte("{}")))
	has, err = dirHasObjects(ctx, store, incrementDirName)
	require.NoError(t, err)
	require.False(t, has)
}

func TestEnsureManagedSourceJobCreatesAndWaits(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)

	manager := newCmdTestStateManager(t, ctx, store)

	var created bool
	var waitedID string
	err = ensureManagedSourceJob(ctx, sourcePrepareContext{
		store: store,
		state: manager,
	}, fakeSourceRunner{
		jobName: "test changefeed",
		dir:     incrementDirName,
		createFn: func(context.Context, sourcePrepareContext) (*managedSourceJobResult, error) {
			created = true
			return &managedSourceJobResult{ID: "cf-1", SnapshotTSO: "449"}, nil
		},
		waitFn: func(_ context.Context, _ sourcePrepareContext, _ sourceJobType, id string) (*managedSourceJobResult, error) {
			waitedID = id
			return &managedSourceJobResult{ID: id}, nil
		},
	}, sourceJobChangefeed)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "cf-1", waitedID)

	st := manager.Snapshot()
	require.Equal(t, "449", st.Snapshot.TSO)
	require.Equal(t, "cf-1", st.TaskInfo.ChangefeedID)
}

func TestEnsureManagedSourceJobSkipsWhenStorageDataExists(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)

	require.NoError(t, store.WriteFile(ctx, incrementDirName+"/metadata", []byte("ready")))

	manager := newCmdTestStateManager(t, ctx, store)
	err = ensureManagedSourceJob(ctx, sourcePrepareContext{
		store: store,
		state: manager,
	}, fakeSourceRunner{
		jobName: "test changefeed",
		dir:     incrementDirName,
		createFn: func(context.Context, sourcePrepareContext) (*managedSourceJobResult, error) {
			t.Fatal("create should not be called when storage data exists")
			return nil, nil
		},
		waitFn: func(context.Context, sourcePrepareContext, sourceJobType, string) (*managedSourceJobResult, error) {
			t.Fatal("wait should not be called when storage data exists")
			return nil, nil
		},
	}, sourceJobChangefeed)
	require.NoError(t, err)
	require.Empty(t, manager.Snapshot().Snapshot.TSO)
}

func TestEnsureManagedSourceJobWaitsExistingJobFromState(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)
	require.NoError(t, manager.Update(ctx, func(st *state.State) error {
		st.TaskInfo.ExportID = "exp-1"
		return nil
	}))
	require.NoError(t, store.WriteFile(ctx, snapshotDirName+"/db1.t1.000000.csv", []byte("partial")))

	var waitedID string
	err = ensureManagedSourceJob(ctx, sourcePrepareContext{
		store: store,
		state: manager,
	}, fakeSourceRunner{
		jobName: "test export",
		dir:     snapshotDirName,
		createFn: func(context.Context, sourcePrepareContext) (*managedSourceJobResult, error) {
			t.Fatal("create should not be called when state has the source job id")
			return nil, nil
		},
		waitFn: func(_ context.Context, _ sourcePrepareContext, _ sourceJobType, id string) (*managedSourceJobResult, error) {
			waitedID = id
			return &managedSourceJobResult{ID: id, SnapshotTSO: "466924115091783691"}, nil
		},
	}, sourceJobExport)
	require.NoError(t, err)
	require.Equal(t, "exp-1", waitedID)
	require.Equal(t, "466924115091783691", manager.Snapshot().Snapshot.TSO)
}

func TestLoadExistingSnapshotTSOFromMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)
	require.NoError(t, store.WriteFile(ctx, snapshotDirName+"/metadata", []byte(`Started dump at: 2026-06-11 10:35:45
SHOW MASTER STATUS:
	Log: tidb-binlog
	Pos: 466924115091783691
	GTID:

Finished dump at: 2026-06-11 10:35:48
`)))

	require.NoError(t, loadExistingSnapshotTSO(ctx, store, manager))
	require.Equal(t, "466924115091783691", manager.Snapshot().Snapshot.TSO)
}

func TestLoadSnapshotTSOFromMetadataRequiresMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)
	require.NoError(t, store.WriteFile(ctx, snapshotDirName+"/db1.t1.000001.csv", []byte("1\n")))

	err = loadSnapshotTSOFromMetadata(ctx, store, manager)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot/metadata is missing")
}

func TestLoadExistingSnapshotTSORejectsMismatch(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)
	require.NoError(t, setSnapshotTSO(ctx, manager, "466924115091783691"))
	require.NoError(t, store.WriteFile(ctx, snapshotDirName+"/metadata", []byte("Pos: 466924115091783692\n")))

	err = loadExistingSnapshotTSO(ctx, store, manager)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso mismatch")
}

func TestSnapshotTSOFromMetadata(t *testing.T) {
	tso, err := snapshotTSOFromMetadata([]byte("Started dump at: x\n\tPos: 466924115091783691\n"))
	require.NoError(t, err)
	require.Equal(t, "466924115091783691", tso)

	_, err = snapshotTSOFromMetadata([]byte("Started dump at: x\n"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Pos is missing")
}

func TestMarkSnapshotFinishedInitializesIncrementalCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)
	require.NoError(t, setSnapshotTSO(ctx, manager, "466924115091783691"))

	require.NoError(t, markSnapshotFinished(ctx, manager))
	st := manager.Snapshot()
	require.True(t, st.Snapshot.Finished)
	require.Equal(t, uint64(466924115091783691), st.Incremental.CheckpointTS)
}

func TestSetSnapshotTSORejectsMismatch(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newCmdTestStateManager(t, ctx, store)
	require.NoError(t, setSnapshotTSO(ctx, manager, "466924115091783691"))

	err = setSnapshotTSO(ctx, manager, "466924115091783692")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso mismatch")
}

type fakeSourceRunner struct {
	jobName  string
	dir      string
	createFn func(context.Context, sourcePrepareContext) (*managedSourceJobResult, error)
	waitFn   func(context.Context, sourcePrepareContext, sourceJobType, string) (*managedSourceJobResult, error)
}

func (r fakeSourceRunner) prepare(context.Context, sourcePrepareContext) error { return nil }

func (r fakeSourceRunner) sourceJobName(sourceJobType) string { return r.jobName }

func (r fakeSourceRunner) sourceJobStorageDir(sourceJobType) string { return r.dir }

func (r fakeSourceRunner) createSourceJob(
	ctx context.Context,
	prepareCtx sourcePrepareContext,
	jobType sourceJobType,
) (*managedSourceJobResult, error) {
	return r.createFn(ctx, prepareCtx)
}

func (r fakeSourceRunner) waitSourceJob(
	ctx context.Context,
	prepareCtx sourcePrepareContext,
	jobType sourceJobType,
	id string,
) (*managedSourceJobResult, error) {
	if r.waitFn == nil {
		return &managedSourceJobResult{ID: id}, nil
	}
	return r.waitFn(ctx, prepareCtx, jobType, id)
}

func newCmdTestStateManager(t *testing.T, ctx context.Context, store storeapi.Storage) *state.Manager {
	t.Helper()
	manager, err := state.Open(ctx, store, []string{"db1.t1", "db2.t2"})
	require.NoError(t, err)
	return manager
}

func validationConfig() *Config {
	cfg := baseConfig()
	cfg.TiDB = &tidb.Config{Host: "127.0.0.1", Port: 4000, User: "root"}
	cfg.Snowflake = &snowflake.Config{Database: "SNOW", Schema: "PUBLIC"}
	return cfg
}

func TestValidateConfig_DefaultSourceModeDoesNotRequireTiCDC(t *testing.T) {
	cfg := validationConfig()
	require.NoError(t, validateConfig(cfg))
	require.Equal(t, SourceModeTiDBCloud, sourceMode(cfg))
}

func TestValidateConfig_OPModeRequiresTiCDCAddress(t *testing.T) {
	cfg := validationConfig()
	cfg.SourceMode = SourceModeOP
	err := validateConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--ticdc.address is required when --source.mode=op")

	cfg.OP.TiCDCAddress = "http://127.0.0.1:8300"
	require.NoError(t, validateConfig(cfg))
}

func TestValidateConfig_RejectsUnknownSourceMode(t *testing.T) {
	cfg := validationConfig()
	cfg.SourceMode = SourceMode(99)
	err := validateConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--source.mode must be")
}
