package op

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestPrepareFullUsesSnapshotMetadataForChangefeedStart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: root}).String())
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/metadata", []byte("Pos: 466924115091783691\n")))

	incrementURI, err := url.Parse("s3://bucket/path/increment?access-key=AKIA&secret-access-key=secret")
	require.NoError(t, err)

	var events []string
	var createReq struct {
		StartTS uint64 `json:"start_ts"`
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
	require.Equal(t, uint64(466924115091783691), manager.Snapshot().Snapshot.TSO)
	require.Equal(t, "cf-1", manager.Snapshot().TaskInfo.ChangefeedID)
	require.Equal(t, uint64(466924115091783691), createReq.StartTS)
}

func TestEnsureSnapshotSkipsWhenSnapshotTSOExists(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, manager.SetSnapshotTSO(ctx, 466924115091783691))

	runner := NewRunner(Config{}, store, manager)

	require.NoError(t, runner.EnsureSnapshot(ctx))
}

func TestCreateChangefeedRequiresSnapshotTSO(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	defer store.Close()
	manager := newTestStateManager(t, ctx, store)

	runner := NewRunner(Config{}, store, manager)
	_, err = runner.createChangefeed(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso is required")
}

func newTestStateManager(t *testing.T, ctx context.Context, store storeapi.Storage) state.Manager {
	t.Helper()
	manager, err := state.Open(ctx, store, []string{"db1.t1", "db2.t2"})
	require.NoError(t, err)
	return manager
}
