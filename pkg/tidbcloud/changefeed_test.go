package tidbcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateChangefeedCloudStorageBody(t *testing.T) {
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
		RCU: 8,
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
	require.Equal(t, float64(8), gotBody["rcu"])
	require.NotContains(t, cloudStorage, "columnSelectors")

	require.Equal(t, "cf-1", cf.ChangefeedID)
}

func TestCreateChangefeedCloudStorageColumnSelectorsBody(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"CREATING"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	_, err := c.CreateChangefeed(context.Background(), "10", &CreateChangefeedRequest{Sink: &Sink{
		Type: ChangefeedTypeCloudStorage,
		CloudStorage: &CloudStorageSink{ColumnSelectors: []ColumnSelector{{
			Matcher: []string{"db1.t1"},
			Columns: []string{"*", "!customer_email"},
		}}},
	}})
	require.NoError(t, err)

	cloudStorage := gotBody["sink"].(map[string]any)["cloudStorage"].(map[string]any)
	require.Equal(t, []any{map[string]any{
		"matcher": []any{"db1.t1"},
		"columns": []any{"*", "!customer_email"},
	}}, cloudStorage["columnSelectors"])
}

func TestListChangefeedSpecifications(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "items": [
    {"name": "3rcu", "rcu": 3, "rpsLimit": 5000},
    {"name": "8rcu", "rcu": 8, "rpsLimit": 20000}
  ],
  "total": 2
}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	specs, err := c.ListChangefeedSpecifications(context.Background(), "10")
	require.NoError(t, err)

	require.Equal(t, http.MethodGet, gotMethod)
	require.Equal(t, "/v1beta1/clusters/10/changefeeds:listSpecifications", gotPath)
	require.Equal(t, []ChangefeedSpecification{
		{Name: "3rcu", RCU: 3, RPSLimit: 5000},
		{Name: "8rcu", RCU: 8, RPSLimit: 20000},
	}, specs.Items)
	require.Equal(t, 2, specs.Total)
}

func TestWaitChangefeedReturnsRunningChangefeed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"RUNNING"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	err := c.WaitChangefeed(context.Background(), "10", "cf-1")
	require.NoError(t, err)
}

func TestWaitChangefeedFailsOnCreateFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changefeedId":"cf-1","state":"CREATE_FAILED"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	err := c.WaitChangefeed(context.Background(), "10", "cf-1")
	require.Error(t, err)
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
