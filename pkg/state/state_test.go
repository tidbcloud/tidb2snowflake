package state

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestOpenCreatesStateFile(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	manager, err := Open(ctx, store, []string{"db1.t1", "db2.t2"})
	require.NoError(t, err)

	st := manager.Snapshot()
	require.Equal(t, Version, st.Version)
	require.Empty(t, st.TaskInfo.ExportID)
	require.Empty(t, st.TaskInfo.ChangefeedID)
	require.Empty(t, st.Snapshot.TSO)
	require.False(t, st.Snapshot.Finished)
	require.Zero(t, st.Incremental.CheckpointTS)
	require.Nil(t, st.Incremental.Scan)
	require.Equal(t, TableState{DMLFileWatermarks: map[string]uint64{}}, st.Incremental.Tables["db1.t1"])
	require.Equal(t, TableState{DMLFileWatermarks: map[string]uint64{}}, st.Incremental.Tables["db2.t2"])

	data, err := store.ReadFile(ctx, FileName)
	require.NoError(t, err)
	require.True(t, json.Valid(data))
}

func TestOpenRejectsMissingRequiredField(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.WriteFile(ctx, FileName, []byte(`{
  "version": 1,
  "task_info": {
    "export_id": "",
    "changefeed_id": ""
  },
  "snapshot": {
    "finished": false
  },
  "incremental": {
    "checkpoint_ts": 0,
    "tables": {}
  }
}`)))

	_, err := Open(ctx, store, []string{"db.t"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso")
}

func TestOpenRejectsUnknownField(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.WriteFile(ctx, FileName, []byte(`{
  "version": 1,
  "task_info": {
    "export_id": "",
    "changefeed_id": ""
  },
  "snapshot": {
    "tso": "",
    "finished": false,
    "extra": true
  },
  "incremental": {
    "checkpoint_ts": 0,
    "tables": {}
  }
}`)))

	_, err := Open(ctx, store, []string{"db.t"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown field")
}

func TestOpenAddsConfiguredTableEntries(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.WriteFile(ctx, FileName, []byte(`{
  "version": 1,
  "task_info": {
    "export_id": "exp-1",
    "changefeed_id": ""
  },
  "snapshot": {
    "tso": "449",
    "finished": true
  },
  "incremental": {
    "checkpoint_ts": 0,
    "tables": {
      "db1.t1": {
        "dml_file_watermarks": {
          "42/0/2026-06-25/": 9
        },
        "ddl_table_version_watermark": 42
      }
    }
  }
}`)))

	manager, err := Open(ctx, store, []string{"db1.t1", "db2.t2"})
	require.NoError(t, err)

	st := manager.Snapshot()
	require.Equal(t, uint64(9), st.Incremental.Tables["db1.t1"].DMLFileWatermarks["42/0/2026-06-25/"])
	require.Equal(t, uint64(42), st.Incremental.Tables["db1.t1"].DDLTableVersionWatermark)
	require.Equal(t, TableState{DMLFileWatermarks: map[string]uint64{}}, st.Incremental.Tables["db2.t2"])
}

func TestUpdatePersistsState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)

	err = manager.Update(ctx, func(st *State) error {
		st.TaskInfo.ExportID = "exp-1"
		st.Snapshot.TSO = "449"
		st.Snapshot.Finished = true
		st.Incremental.CheckpointTS = 450
		st.Incremental.Scan = &ScanState{HighWatermark: 500}
		tableState := st.Incremental.Tables["db.t"]
		tableState.DMLFileWatermarks["42/0/2026-06-25/"] = 9
		tableState.DDLTableVersionWatermark = 42
		st.Incremental.Tables["db.t"] = tableState
		return nil
	})
	require.NoError(t, err)

	reopened, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)
	st := reopened.Snapshot()
	require.Equal(t, "exp-1", st.TaskInfo.ExportID)
	require.Equal(t, "449", st.Snapshot.TSO)
	require.True(t, st.Snapshot.Finished)
	require.Equal(t, uint64(450), st.Incremental.CheckpointTS)
	require.Equal(t, &ScanState{HighWatermark: 500}, st.Incremental.Scan)
	require.Equal(t, uint64(9), st.Incremental.Tables["db.t"].DMLFileWatermarks["42/0/2026-06-25/"])
	require.Equal(t, uint64(42), st.Incremental.Tables["db.t"].DDLTableVersionWatermark)
}

func TestUpdateDoesNotMutateStateOnError(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)

	err = manager.Update(ctx, func(st *State) error {
		st.Snapshot.Finished = true
		return errors.New("boom")
	})
	require.Error(t, err)

	require.False(t, manager.Snapshot().Snapshot.Finished)
}

func TestSnapshotReturnsDeepCopy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)

	snapshot := manager.Snapshot()
	snapshot.Snapshot.Finished = true
	snapshot.Incremental.Scan = &ScanState{HighWatermark: 500}
	tableState := snapshot.Incremental.Tables["db.t"]
	tableState.DMLFileWatermarks["42/0/2026-06-25/"] = 9
	snapshot.Incremental.Tables["db.t"] = tableState

	st := manager.Snapshot()
	require.False(t, st.Snapshot.Finished)
	require.Nil(t, st.Incremental.Scan)
	require.Empty(t, st.Incremental.Tables["db.t"].DMLFileWatermarks)
}

func TestUpdateRejectsInvalidState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)

	err = manager.Update(ctx, func(st *State) error {
		st.Version = 2
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported state version")
	require.Equal(t, Version, manager.Snapshot().Version)
}

func TestSetSnapshotTSORejectsMismatch(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)
	require.NoError(t, manager.SetSnapshotTSO(ctx, "466924115091783691"))

	err = manager.SetSnapshotTSO(ctx, "466924115091783692")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso mismatch")
}

func TestUpdateExportStateRejectsSnapshotTSOMismatch(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	manager, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)
	require.NoError(t, manager.UpdateExportState(ctx, "exp-1", "466924115091783691"))

	err = manager.UpdateExportState(ctx, "exp-2", "466924115091783692")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot.tso mismatch")
	require.Equal(t, "exp-1", manager.Snapshot().TaskInfo.ExportID)
}

func TestWrittenStateContainsOnlyV1Fields(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	_, err := Open(ctx, store, []string{"db.t"})
	require.NoError(t, err)

	data, err := store.ReadFile(ctx, FileName)
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc))
	require.ElementsMatch(t, []string{"version", "task_info", "snapshot", "incremental"}, mapKeys(doc))
	require.ElementsMatch(t, []string{"export_id", "changefeed_id"}, mapKeys(doc["task_info"].(map[string]any)))
	require.ElementsMatch(t, []string{"tso", "finished"}, mapKeys(doc["snapshot"].(map[string]any)))
	incremental := doc["incremental"].(map[string]any)
	require.ElementsMatch(t, []string{"checkpoint_ts", "tables"}, mapKeys(incremental))
	tables := incremental["tables"].(map[string]any)
	tableState := tables["db.t"].(map[string]any)
	require.ElementsMatch(t, []string{"dml_file_watermarks", "ddl_table_version_watermark"}, mapKeys(tableState))
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
