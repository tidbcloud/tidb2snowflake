package op

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestPrepareFullUsesSnapshotMetadataForChangefeedStart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: root})
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/metadata", []byte("Pos: 466924115091783691\n")))

	incrementURI, err := url.Parse("s3://bucket/path/increment?access-key=AKIA&secret-access-key=secret")
	require.NoError(t, err)

	var events []string
	var createReq struct {
		ChangefeedID string `json:"changefeed_id"`
		StartTS      uint64 `json:"start_ts"`
		SinkURI      string `json:"sink_uri"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case http.MethodPost + " /api/v2/changefeeds":
			events = append(events, "changefeed")
			require.NoError(t, json.NewDecoder(r.Body).Decode(&createReq))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"cf-1","state":"pending"}`))
		case http.MethodGet + " /api/v2/changefeeds/cf-1":
			require.Equal(t, "default", r.URL.Query().Get("namespace"))
			_, _ = w.Write([]byte(`{"id":"cf-1","state":"normal"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := NewRunner(Config{
		TiDB:                    &tidb.Config{},
		TiCDCAddress:            srv.URL,
		SnapshotConcurrency:     8,
		Tables:                  []string{"db1.t1", "db2.t2"},
		ChangefeedFlushInterval: time.Minute,
		ChangefeedFileSizeMiB:   64,
		SnapshotCompression:     "none",
		IncrementURI:            incrementURI,
	}, store, manager)

	err = runner.EnsureSnapshot(ctx)
	require.NoError(t, err)
	err = runner.EnsureChangefeed(ctx)
	require.NoError(t, err)

	require.Equal(t, []string{"changefeed"}, events)
	require.Zero(t, manager.Snapshot().CheckpointTS)
	require.Equal(t, "cf-1", manager.Snapshot().TaskInfo.ChangefeedID)
	require.Regexp(t, `^tidb2snowflake-\d+$`, createReq.ChangefeedID)
	require.Equal(t, uint64(466924115091783691), createReq.StartTS)
	sinkURI, err := url.Parse(createReq.SinkURI)
	require.NoError(t, err)
	require.Equal(t, "s3", sinkURI.Scheme)
	require.Equal(t, "bucket", sinkURI.Host)
	require.Equal(t, "/path/increment", sinkURI.Path)
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

func TestChangefeedStartTSOUsesConfiguredSnapshotTSO(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)

	runner := NewRunner(Config{SnapshotTSO: "466924115091783691"}, store, manager)
	startTSO, err := runner.changefeedStartTSO(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(466924115091783691), startTSO)
}

func TestEnsureChangefeedOnlyChecksExistingID(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, manager.SetChangefeedID(ctx, "cf-1"))

	getCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet+" /api/v2/changefeeds/cf-1", r.Method+" "+r.URL.Path)
		getCount++
		_, _ = w.Write([]byte(`{"id":"cf-1","state":"stopped"}`))
	}))
	defer srv.Close()

	runner := NewRunner(Config{TiCDCAddress: srv.URL}, store, manager)

	require.NoError(t, runner.EnsureChangefeed(ctx))
	require.Equal(t, 1, getCount)
}

func newTestStateManager(t *testing.T, ctx context.Context, store storeapi.Storage) state.Manager {
	t.Helper()
	manager, err := state.Open(ctx, store, []string{"db1.t1", "db2.t2"}, false)
	require.NoError(t, err)
	return manager
}
