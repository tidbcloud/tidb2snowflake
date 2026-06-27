package snapshot

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func TestLoadExistingTSOFromMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/metadata", []byte(`Started dump at: 2026-06-11 10:35:45
SHOW MASTER STATUS:
	Log: tidb-binlog
	Pos: 466924115091783691
	GTID:

Finished dump at: 2026-06-11 10:35:48
`)))

	require.NoError(t, LoadExistingTSO(ctx, store, manager))
	require.Equal(t, "466924115091783691", manager.Snapshot().Snapshot.TSO)
}

func TestLoadTSOFromMetadataRequiresMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/db1.t1.000001.csv", []byte("1\n")))

	err = LoadTSOFromMetadata(ctx, store, manager)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot/metadata is missing")
}

func TestLoadExistingTSORejectsMismatch(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)
	manager := newTestStateManager(t, ctx, store)
	require.NoError(t, manager.SetSnapshotTSO(ctx, "466924115091783691"))
	require.NoError(t, store.WriteFile(ctx, storage.SnapshotDirName+"/metadata", []byte("Pos: 466924115091783692\n")))

	err = LoadExistingTSO(ctx, store, manager)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso mismatch")
}

func TestTSOFromMetadata(t *testing.T) {
	tso, err := TSOFromMetadata([]byte("Started dump at: x\n\tPos: 466924115091783691\n"))
	require.NoError(t, err)
	require.Equal(t, "466924115091783691", tso)

	_, err = TSOFromMetadata([]byte("Started dump at: x\n"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Pos is missing")
}

func newTestStateManager(t *testing.T, ctx context.Context, store storeapi.Storage) state.Manager {
	t.Helper()
	manager, err := state.Open(ctx, store, []string{"db1.t1", "db2.t2"})
	require.NoError(t, err)
	return manager
}
