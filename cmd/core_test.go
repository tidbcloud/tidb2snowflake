package cmd

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
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

func TestSnapshotLoadModeDefault(t *testing.T) {
	require.Equal(t, SnapshotLoadModeBulk, snapshotLoadMode(&Config{}))
	require.Equal(t, SnapshotLoadModePerFile, snapshotLoadMode(&Config{SnapshotLoadMode: SnapshotLoadModePerFile}))
}

func TestSnapshotCompressionDefault(t *testing.T) {
	require.Equal(t, SnapshotCompressionNone, snapshotCompression(&Config{}))
	require.Equal(t, SnapshotCompressionGzip, snapshotCompression(&Config{SnapshotCompression: SnapshotCompressionGzip}))
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

func TestStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, err := putil.GetExternalStorageFromURI(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)

	// no file yet -> empty state
	s, err := loadState(ctx, store)
	require.NoError(t, err)
	require.Empty(t, s.ExportID)

	// save then reload
	require.NoError(t, saveState(ctx, store, &runState{
		ExportID:     "exp-1",
		ChangefeedID: "cf-1",
		SnapshotTSO:  "449",
	}))
	got, err := loadState(ctx, store)
	require.NoError(t, err)
	require.Equal(t, "exp-1", got.ExportID)
	require.Equal(t, "cf-1", got.ChangefeedID)
	require.Equal(t, "449", got.SnapshotTSO)
}

func TestDirHasObjects(t *testing.T) {
	ctx := context.Background()
	store, err := putil.GetExternalStorageFromURI(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
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

	// a state file at the root must not count as snapshot/increment content
	require.NoError(t, store.WriteFile(ctx, stateFileName, []byte("{}")))
	has, err = dirHasObjects(ctx, store, incrementDirName)
	require.NoError(t, err)
	require.False(t, has)
}
