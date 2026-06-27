package tidbcloud

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stretchr/testify/require"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
)

func testCred() *credentials.Value {
	return &credentials.Value{AccessKeyID: "AKIA", SecretAccessKey: "secret"}
}

func baseConfig() Config {
	return Config{
		Tables:                  []string{"db1.t1", "db2.t2"},
		ChangefeedFlushInterval: 60 * time.Second,
		ChangefeedFileSizeMiB:   64,
		SnapshotCompression:     "none",
	}
}

func TestBuildExportRequest(t *testing.T) {
	cfg := baseConfig()
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred(), "")

	require.Equal(t, cloudapi.ExportFileTypeCSV, req.ExportOptions.FileType)
	require.Equal(t, cloudapi.ExportCompressionNone, req.ExportOptions.Compression)
	require.Equal(t, []string{"db1.t1", "db2.t2"}, req.ExportOptions.Filter.Table.Patterns)
	require.Equal(t, ",", req.ExportOptions.CSVFormat.Separator)
	require.Equal(t, "\"", *req.ExportOptions.CSVFormat.Delimiter)
	require.Equal(t, "\\N", *req.ExportOptions.CSVFormat.NullValue)
	require.True(t, req.ExportOptions.CSVFormat.SkipHeader)
	require.Equal(t, cloudapi.ExportCSVDialectSnowflake, req.ExportOptions.CSVFormat.Dialect)
	require.NotNil(t, req.ExportOptions.EscapeBackslash)
	require.False(t, *req.ExportOptions.EscapeBackslash)
	require.Empty(t, req.ExportOptions.SnapshotTSO)

	require.Equal(t, cloudapi.ExportTargetTypeS3, req.Target.Type)
	require.Equal(t, "s3://bucket/path/snapshot/", req.Target.S3.URI)
	require.Equal(t, cloudapi.S3AuthTypeAccessKey, req.Target.S3.AuthType)
	require.Equal(t, "AKIA", req.Target.S3.AccessKey.ID)
	require.Equal(t, "secret", req.Target.S3.AccessKey.Secret)
}

func TestBuildExportRequestPinnedSnapshotTSO(t *testing.T) {
	cfg := baseConfig()
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred(), "449023000000000000")
	require.Equal(t, "449023000000000000", req.ExportOptions.SnapshotTSO)
}

func TestBuildExportRequestGzipCompression(t *testing.T) {
	cfg := baseConfig()
	cfg.SnapshotCompression = "gzip"
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred(), "")
	require.Equal(t, cloudapi.ExportCompressionGzip, req.ExportOptions.Compression)
}

func TestBuildChangefeedRequestFromTSO(t *testing.T) {
	cfg := baseConfig()
	req := buildChangefeedRequest(cfg, "s3://bucket/path/increment/", testCred(), "449023000000000000")

	require.Equal(t, cloudapi.ChangefeedTypeCloudStorage, req.Sink.Type)
	cs := req.Sink.CloudStorage
	require.Equal(t, cloudapi.CloudStorageTypeS3, cs.Storage.Type)
	require.Equal(t, "s3://bucket/path/increment/", cs.Storage.S3.URI)
	require.Equal(t, cloudapi.S3AuthTypeAccessKey, cs.Storage.S3.AuthType)
	require.Equal(t, "AKIA", cs.Storage.S3.AccessKey.ID)
	require.Equal(t, cloudapi.CloudStorageProtocolCSV, cs.DataFormat.Protocol)
	require.True(t, cs.DataFormat.CSVConfig.IncludeCommitTs)
	require.Equal(t, cloudapi.BinaryEncodingHex, cs.DataFormat.CSVConfig.Encoding)
	require.Equal(t, cloudapi.DateSeparatorDay, cs.DateSeparator)
	require.Equal(t, 60, cs.IntervalInSeconds)
	require.Equal(t, 64, cs.SizeInMiB)
	require.True(t, cs.OutputColumnID)

	require.Equal(t, []string{"db1.t1", "db2.t2"}, req.Filter.FilterRule)
	require.Equal(t, cloudapi.TableModeForceSync, req.Filter.Mode)
	require.Equal(t, cloudapi.StartModeFromTSO, req.StartPosition.Mode)
	require.Equal(t, "449023000000000000", req.StartPosition.TSO)
}

func TestBuildChangefeedRequestFromNowWhenNoTSO(t *testing.T) {
	cfg := baseConfig()
	req := buildChangefeedRequest(cfg, "s3://bucket/path/increment", testCred(), "")
	require.Equal(t, cloudapi.StartModeFromNow, req.StartPosition.Mode)
	require.Empty(t, req.StartPosition.TSO)
}
