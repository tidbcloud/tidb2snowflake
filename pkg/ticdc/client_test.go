package ticdc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestBuildChangefeedConfigCloudStorageCSV(t *testing.T) {
	storageURI, err := url.Parse("s3://bucket/path/increment?access-key=AKIA&secret-access-key=secret")
	require.NoError(t, err)

	cfg, err := BuildChangefeedConfig(ChangefeedConfigOptions{
		Tables:        []string{"db1.t1", "db2.t2"},
		StorageURI:    storageURI,
		StartTSO:      449023000000000000,
		FlushInterval: 60 * time.Second,
		FileSizeMiB:   64,
	})
	require.NoError(t, err)

	sinkURI, err := url.Parse(cfg.SinkURI)
	require.NoError(t, err)
	require.Equal(t, "s3", sinkURI.Scheme)
	require.Equal(t, "bucket", sinkURI.Host)
	require.Equal(t, "/path/increment", sinkURI.Path)
	require.Equal(t, "AKIA", sinkURI.Query().Get("access-key"))
	require.Equal(t, "secret", sinkURI.Query().Get("secret-access-key"))
	require.Equal(t, "csv", sinkURI.Query().Get("protocol"))
	require.Equal(t, "1m0s", sinkURI.Query().Get("flush-interval"))
	require.Equal(t, "67108864", sinkURI.Query().Get("file-size"))

	require.Equal(t, uint64(449023000000000000), cfg.StartTs)
	require.Equal(t, []string{"db1.t1", "db2.t2"}, cfg.ReplicaConfig.Filter.Rules)
	require.True(t, cfg.ReplicaConfig.Sink.CSVConfig.IncludeCommitTs)
	require.Equal(t, "hex", cfg.ReplicaConfig.Sink.CSVConfig.BinaryEncodingMethod)
	require.NotNil(t, cfg.ReplicaConfig.Sink.CloudStorageConfig)
	require.Equal(t, "1m0s", *cfg.ReplicaConfig.Sink.CloudStorageConfig.FlushInterval)
	require.Equal(t, 64*1024*1024, *cfg.ReplicaConfig.Sink.CloudStorageConfig.FileSize)
	require.True(t, *cfg.ReplicaConfig.Sink.CloudStorageConfig.OutputColumnID)
	require.NotSame(t, storageURI, sinkURI)
	require.Empty(t, storageURI.Query().Get("protocol"))
}

func TestClientCreateAndWaitChangefeed(t *testing.T) {
	var createPath string
	var createBody map[string]any
	getCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case http.MethodPost + " /api/v2/changefeeds":
			createPath = r.URL.Path
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))
			require.NoError(t, json.NewDecoder(r.Body).Decode(&createBody))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"cf-1","state":"pending"}`))
		case http.MethodGet + " /api/v2/changefeeds/cf-1":
			require.Equal(t, "default", r.URL.Query().Get("namespace"))
			getCount++
			_, _ = w.Write([]byte(`{"id":"cf-1","state":"normal"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL)
	require.NoError(t, err)

	cf, err := client.CreateChangefeed(context.Background(), &ChangefeedConfig{SinkURI: "s3://bucket/increment?protocol=csv"})
	require.NoError(t, err)
	require.Equal(t, "cf-1", cf.ID)
	require.Equal(t, "/api/v2/changefeeds", createPath)
	require.Equal(t, "s3://bucket/increment?protocol=csv", createBody["sink_uri"])

	cf, err = client.WaitChangefeed(context.Background(), "cf-1")
	require.NoError(t, err)
	require.Equal(t, config.StateNormal, cf.State)
	require.Equal(t, 1, getCount)
}

func TestClientWaitChangefeedFailsOnTerminalState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		_, _ = w.Write([]byte(`{"id":"cf-1","state":"failed"}`))
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL)
	require.NoError(t, err)

	_, err = client.WaitChangefeed(context.Background(), "cf-1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended in state failed")
}
