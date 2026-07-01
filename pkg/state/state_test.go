package state

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestOpenCreatesStateFile(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	manager, err := Open(ctx, store, []string{"db1.t1", "db2.t2"}, false)
	require.NoError(t, err)

	st := manager.Snapshot()
	require.Equal(t, Version, st.Version)
	require.Zero(t, st.CheckpointTS)
	require.False(t, st.SnapshotFinished)
	require.Empty(t, st.TaskInfo.ExportID)
	require.Empty(t, st.TaskInfo.ChangefeedID)
	require.Equal(t, TableState{
		DDLTableVersionWatermark: 0,
		DMLCursors:               map[string]DMLCursor{},
	}, st.Tables["db1.t1"])
	require.Equal(t, TableState{
		DDLTableVersionWatermark: 0,
		DMLCursors:               map[string]DMLCursor{},
	}, st.Tables["db2.t2"])

	data, err := store.ReadFile(ctx, stateFileName)
	require.NoError(t, err)
	require.True(t, json.Valid(data))
}

func TestOpenRejectsMissingRequiredField(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.WriteFile(ctx, stateFileName, []byte(`{
  "version": 1,
  "checkpoint_ts": 0,
  "snapshot_finished": false,
  "task_info": {
    "export_id": "",
    "changefeed_id": ""
  }
}`)))

	_, err := Open(ctx, store, []string{"db.t"}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tables")
}

func TestOpenRejectsUnknownField(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.WriteFile(ctx, stateFileName, []byte(`{
  "version": 1,
  "checkpoint_ts": 0,
  "snapshot_finished": false,
  "task_info": {
    "export_id": "",
    "changefeed_id": "",
    "extra": true
  },
  "tables": {}
}`)))

	_, err := Open(ctx, store, []string{"db.t"}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown field")
}

func TestOpenRejectsConfiguredTableSetChange(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.WriteFile(ctx, stateFileName, []byte(`{
  "version": 1,
  "checkpoint_ts": 449,
  "snapshot_finished": true,
  "task_info": {
    "export_id": "exp-1",
    "changefeed_id": ""
  },
  "tables": {
    "db1.t1": {
      "ddl_table_version_watermark": 42,
      "dml_cursors": {}
    }
  }
}`)))

	_, err := Open(ctx, store, []string{"db1.t1", "db2.t2"}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "state table set does not match configured tables")
}

func TestStateMutationsPersist(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)

	require.NoError(t, manager.SetExportID(ctx, "exp-1"))
	require.NoError(t, manager.SetDDLTableVersionWatermark(ctx, "db.t", 42))
	require.NoError(t, manager.SetDMLCursors(ctx, "db.t", map[string]DMLCursor{
		"449/0/CDC": {Date: "2026-06-30", FileIndex: 12},
	}))
	require.NoError(t, manager.MarkSnapshotFinished(ctx, 449))

	reopened, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)
	st := reopened.Snapshot()
	require.Equal(t, "exp-1", st.TaskInfo.ExportID)
	require.Equal(t, uint64(449), st.CheckpointTS)
	require.True(t, st.SnapshotFinished)
	require.Equal(t, uint64(42), st.Tables["db.t"].DDLTableVersionWatermark)
	require.Equal(t, DMLCursor{Date: "2026-06-30", FileIndex: 12}, st.Tables["db.t"].DMLCursors["449/0/CDC"])
}

func TestClearChangefeedIDPersists(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)
	require.NoError(t, manager.SetChangefeedID(ctx, "cf-1"))

	require.NoError(t, manager.ClearChangefeedID(ctx))

	reopened, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)
	require.Empty(t, reopened.Snapshot().TaskInfo.ChangefeedID)
}

func TestSnapshotReturnsDeepCopy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)

	snapshot := manager.Snapshot()
	snapshot.SnapshotFinished = true
	tableState := snapshot.Tables["db.t"]
	tableState.DDLTableVersionWatermark = 42
	tableState.DMLCursors["449/0/CDC"] = DMLCursor{Date: "2026-06-30", FileIndex: 12}
	snapshot.Tables["db.t"] = tableState

	st := manager.Snapshot()
	require.False(t, st.SnapshotFinished)
	require.Zero(t, st.Tables["db.t"].DDLTableVersionWatermark)
	require.Empty(t, st.Tables["db.t"].DMLCursors)
}

func TestSetCheckpointTSRejectsBackwardMove(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)
	require.NoError(t, manager.SetCheckpointTS(ctx, 466924115091783691))

	err = manager.SetCheckpointTS(ctx, 466924115091783690)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checkpoint_ts cannot move backward")
}

func TestSetDMLCursorsRejectsBackwardMove(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)
	require.NoError(t, manager.SetDMLCursors(ctx, "db.t", map[string]DMLCursor{
		"449/0/CDC": {Date: "2026-06-30", FileIndex: 12},
	}))

	err = manager.SetDMLCursors(ctx, "db.t", map[string]DMLCursor{
		"449/0/CDC": {Date: "2026-06-30", FileIndex: 11},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "dml cursor cannot move backward")
}

func TestWrittenStateContainsOnlyV1Fields(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	_, err := Open(ctx, store, []string{"db.t"}, false)
	require.NoError(t, err)

	data, err := store.ReadFile(ctx, stateFileName)
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc))
	require.ElementsMatch(t, []string{"version", "checkpoint_ts", "snapshot_finished", "task_info", "tables"}, mapKeys(doc))
	require.ElementsMatch(t, []string{"export_id", "changefeed_id"}, mapKeys(doc["task_info"].(map[string]any)))
	tables := doc["tables"].(map[string]any)
	tableState := tables["db.t"].(map[string]any)
	require.ElementsMatch(t, []string{"ddl_table_version_watermark", "dml_cursors"}, mapKeys(tableState))
}

func newTestStore(t *testing.T) storeapi.Storage {
	t.Helper()
	uri := (&url.URL{Scheme: "file", Path: t.TempDir()}).String()
	store, err := util.GetExternalStorageWithDefaultTimeout(context.Background(), uri)
	require.NoError(t, err)
	return store
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
