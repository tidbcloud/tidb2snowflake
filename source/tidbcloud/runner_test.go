package tidbcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func testCred() *aws.Credentials {
	return &aws.Credentials{AccessKeyID: "AKIA", SecretAccessKey: "secret"}
}

func baseConfig() Config {
	return Config{
		Tables:                  []string{"db1.t1", "db2.t2"},
		ChangefeedFlushInterval: 60 * time.Second,
		ChangefeedFileSizeMiB:   64,
		SnapshotCompression:     cloudapi.ExportCompressionNone,
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

func TestEnsureSnapshotLoadsExistingMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/metadata", []byte("Pos: 466924115091783691\n")))

	runner := NewRunner(baseConfig(), store, manager)

	require.NoError(t, runner.EnsureSnapshot(ctx))
	require.Zero(t, manager.Snapshot().CheckpointTS)
}

func TestEnsureSnapshotSkipsWhenCheckpointExists(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, manager.MarkSnapshotFinished(ctx, 466924115091783691))

	runner := NewRunner(Config{}, store, manager)

	require.NoError(t, runner.EnsureSnapshot(ctx))
}

func TestBuildExportRequestPinnedSnapshotTSO(t *testing.T) {
	cfg := baseConfig()
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred(), "449023000000000000")
	require.Equal(t, "449023000000000000", req.ExportOptions.SnapshotTSO)
}

func TestBuildExportRequestGzipCompression(t *testing.T) {
	cfg := baseConfig()
	cfg.SnapshotCompression = cloudapi.ExportCompressionGzip
	req := buildExportRequest(cfg, "s3://bucket/path/snapshot/", testCred(), "")
	require.Equal(t, cloudapi.ExportCompressionGzip, req.ExportOptions.Compression)
}

func TestBuildChangefeedRequestFromTSO(t *testing.T) {
	cfg := baseConfig()
	cfg.ChangefeedRCU = 8
	before := time.Now().UnixMilli()
	req := buildChangefeedRequest(cfg, "s3://bucket/path/increment/", testCred(), "449023000000000000")
	after := time.Now().UnixMilli()

	require.Regexp(t, `^tidb2snowflake-\d+$`, req.DisplayName)
	displayNameTS, err := strconv.ParseInt(strings.TrimPrefix(req.DisplayName, "tidb2snowflake-"), 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, displayNameTS, before)
	require.LessOrEqual(t, displayNameTS, after)
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

	require.Equal(t, []string{"db1.t1", "db2.t2"}, req.Filter.FilterRule)
	require.Equal(t, cloudapi.TableModeForceSync, req.Filter.Mode)
	require.Equal(t, cloudapi.StartModeFromTSO, req.StartPosition.Mode)
	require.Equal(t, "449023000000000000", req.StartPosition.TSO)
	require.Equal(t, 8, req.RCU)
}

func TestCreateChangefeedValidatesConfiguredRCUAgainstSpecifications(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)

	postCalled := false
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case http.MethodGet + " /v1beta1/clusters/10/changefeeds:listSpecifications":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[{"name":"3rcu","rcu":3},{"name":"8rcu","rcu":8}],"total":2}`))
		case http.MethodPost + " /v1beta1/clusters/10/changefeeds":
			postCalled = true
			http.Error(w, "create should not be called for invalid rcu", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := NewRunner(Config{
		ClusterID:               "10",
		Tables:                  []string{"db1.t1"},
		ChangefeedFlushInterval: time.Minute,
		ChangefeedFileSizeMiB:   64,
		ChangefeedRCU:           7,
		Credential:              testCred(),
	}, store, manager)
	runner.client = newCloudAPITestClient(t, srv)

	_, _, err = runner.createChangefeed(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "changefeed.rcu 7 is not available")
	require.Contains(t, err.Error(), "available values: 3, 8")
	require.False(t, postCalled)
}

func TestCreateChangefeedSendsConfiguredRCUAfterValidation(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)

	var events []string
	var createReq struct {
		RCU int `json:"rcu"`
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case http.MethodGet + " /v1beta1/clusters/10/changefeeds:listSpecifications":
			events = append(events, "list")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[{"name":"3rcu","rcu":3},{"name":"8rcu","rcu":8}],"total":2}`))
		case http.MethodPost + " /v1beta1/clusters/10/changefeeds":
			events = append(events, "create")
			require.NoError(t, json.NewDecoder(r.Body).Decode(&createReq))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"CREATING"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := NewRunner(Config{
		ClusterID:               "10",
		Tables:                  []string{"db1.t1"},
		ChangefeedFlushInterval: time.Minute,
		ChangefeedFileSizeMiB:   64,
		ChangefeedRCU:           8,
		Credential:              testCred(),
	}, store, manager)
	runner.client = newCloudAPITestClient(t, srv)

	changefeedID, _, err := runner.createChangefeed(ctx)
	require.NoError(t, err)
	require.Equal(t, "cf-1", changefeedID)
	require.Equal(t, []string{"list", "create"}, events)
	require.Equal(t, 8, createReq.RCU)
}

func TestValidateTiDBCloudConfigReportsConfigKeys(t *testing.T) {
	err := validateTiDBCloudConfig(Config{})

	require.Error(t, err)
	require.Contains(t, err.Error(), "tidbcloud.cluster-id")
	require.Contains(t, err.Error(), "tidbcloud.public-key")
	require.Contains(t, err.Error(), "tidbcloud.private-key")
	require.NotContains(t, err.Error(), "TIDBCLOUD_CLUSTER_ID")
	require.NotContains(t, err.Error(), "environment variable")
}

func newTestStateManager(t *testing.T, ctx context.Context, store storeapi.Storage) state.Manager {
	t.Helper()
	manager, err := state.Open(ctx, store, []string{"db1.t1", "db2.t2"}, false)
	require.NoError(t, err)
	return manager
}

func newCloudAPITestClient(t *testing.T, srv *httptest.Server) *cloudapi.Client {
	t.Helper()
	host := strings.TrimPrefix(srv.URL, "https://")
	client, err := cloudapi.NewClient("public-key", "private-key",
		cloudapi.WithHost(host),
		cloudapi.WithBaseTransport(srv.Client().Transport),
		cloudapi.WithMaxRetries(0),
	)
	require.NoError(t, err)
	return client
}
