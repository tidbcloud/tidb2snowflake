package tidbcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCreateChangefeed_CloudStorageBody(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"CREATING"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	req := &CreateChangefeedRequest{
		DisplayName: "incremental",
		Sink: &Sink{
			Type: ChangefeedTypeCloudStorage,
			CloudStorage: &CloudStorageSink{
				Storage: &CloudStorage{
					Type: CloudStorageTypeS3,
					S3: &S3CloudStorage{
						URI:       "s3://bucket/increment",
						AuthType:  S3AuthTypeAccessKey,
						AccessKey: &S3AccessKey{ID: "AKIA", Secret: "secret"},
					},
				},
				DataFormat: &CloudStorageDataFormat{
					Protocol: CloudStorageProtocolCSV,
					CSVConfig: &CSVConfig{
						IncludeCommitTs: true,
						Encoding:        BinaryEncodingHex,
					},
				},
				DateSeparator:     DateSeparatorDay,
				IntervalInSeconds: 60,
				SizeInMiB:         64,
			},
		},
		Filter: &ChangefeedFilter{FilterRule: []string{"db1.t1"}, Mode: TableModeForceSync},
		StartPosition: &StartPosition{
			Mode: StartModeFromTSO,
			TSO:  "449023000000000000",
		},
	}
	cf, err := c.CreateChangefeed(context.Background(), "10", req)
	require.NoError(t, err)

	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/v1beta1/clusters/10/changefeeds", gotPath)
	require.Equal(t, "incremental", gotBody["displayName"])
	sink := gotBody["sink"].(map[string]any)
	require.Equal(t, string(ChangefeedTypeCloudStorage), sink["type"])
	cloudStorage := sink["cloudStorage"].(map[string]any)
	storage := cloudStorage["storage"].(map[string]any)
	s3 := storage["s3"].(map[string]any)
	require.Equal(t, "s3://bucket/increment", s3["uri"])
	dataFormat := cloudStorage["dataFormat"].(map[string]any)
	require.Equal(t, string(CloudStorageProtocolCSV), dataFormat["protocol"])
	csvConfig := dataFormat["csvConfig"].(map[string]any)
	require.True(t, csvConfig["includeCommitTs"].(bool))
	require.Equal(t, string(BinaryEncodingHex), csvConfig["encoding"])
	filter := gotBody["filter"].(map[string]any)
	require.Equal(t, []any{"db1.t1"}, filter["filterRule"])
	require.Equal(t, string(TableModeForceSync), filter["mode"])
	startPosition := gotBody["startPosition"].(map[string]any)
	require.Equal(t, string(StartModeFromTSO), startPosition["mode"])
	require.Equal(t, "449023000000000000", startPosition["tso"])

	require.Equal(t, "cf-1", cf.ChangefeedID)
}

func TestWaitChangefeed_RunningAfterCreating(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if atomic.AddInt32(&calls, 1) < 2 {
			_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"CREATING"}`))
			return
		}
		_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"RUNNING"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	cf, err := c.WaitChangefeed(context.Background(), "10", "cf-1", 10*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, ChangefeedStateRunning, cf.State)
}

func TestWaitChangefeed_FailsOnCreateFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"CREATE_FAILED"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	cf, err := c.WaitChangefeed(context.Background(), "10", "cf-1", 10*time.Millisecond)
	require.Error(t, err)
	require.Equal(t, ChangefeedStateCreateFailed, cf.State)
}

func TestChangefeedVerbPaths(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"RUNNING"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)

	_, err := c.StartChangefeed(context.Background(), "10", "cf-1")
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/v1beta1/clusters/10/changefeeds/cf-1:start", gotPath)

	_, err = c.StopChangefeed(context.Background(), "10", "cf-1")
	require.NoError(t, err)
	require.Equal(t, "/v1beta1/clusters/10/changefeeds/cf-1:stop", gotPath)

	err = c.DeleteChangefeed(context.Background(), "10", "cf-1")
	require.NoError(t, err)
	require.Equal(t, http.MethodDelete, gotMethod)
	require.Equal(t, "/v1beta1/clusters/10/changefeeds/cf-1", gotPath)
}
