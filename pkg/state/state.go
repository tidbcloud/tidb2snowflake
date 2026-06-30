package state

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/pingcap/errors"
)

const (
	Version       = 1
	stateFileName = "replication-state.json"
)

type State struct {
	// Version is the on-disk state schema version.
	Version int `json:"version"`
	// CheckpointTS is the latest upstream checkpoint confirmed in downstream Snowflake.
	CheckpointTS uint64 `json:"checkpoint_ts"`
	// SnapshotFinished means snapshot data has been loaded into downstream Snowflake.
	SnapshotFinished bool `json:"snapshot_finished"`
	// TaskInfo records source-side jobs created or reused by the prepare phase.
	TaskInfo TaskInfo `json:"task_info"`
	// Tables records per-table incremental load state.
	Tables map[string]TableState `json:"tables"`
}

type TaskInfo struct {
	// ExportID is the TiDB Cloud export job to wait for when snapshot export is resumed.
	ExportID string `json:"export_id"`
	// ChangefeedID is the source changefeed job to wait for when incremental export is resumed.
	ChangefeedID string `json:"changefeed_id"`
}

type TableState struct {
	// DDLTableVersionWatermark records the highest applied schema table version.
	DDLTableVersionWatermark uint64 `json:"ddl_table_version_watermark"`
	// DMLCursors record the last consumed date and file index for each DML stream.
	DMLCursors map[string]DMLCursor `json:"dml_cursors"`
}

type DMLCursor struct {
	// Date is the last consumed date directory for a DML stream.
	Date string `json:"date"`
	// FileIndex is the last consumed file index within Date.
	FileIndex uint64 `json:"file_index"`
}

func newState(tables []string, snapshotFinished bool) State {
	st := State{
		Version:          Version,
		SnapshotFinished: snapshotFinished,
		Tables:           make(map[string]TableState, len(tables)),
	}
	ensureConfiguredTables(&st, tables)
	return st
}

func ensureConfiguredTables(st *State, tables []string) bool {
	changed := false
	if st.Tables == nil {
		st.Tables = make(map[string]TableState, len(tables))
		changed = true
	}
	for _, table := range tables {
		if _, ok := st.Tables[table]; !ok {
			st.Tables[table] = TableState{
				DDLTableVersionWatermark: 0,
				DMLCursors:               map[string]DMLCursor{},
			}
			changed = true
		}
	}
	return changed
}

func validateState(st State, tables []string) error {
	if st.Version != Version {
		return errors.Errorf("unsupported state version %d", st.Version)
	}
	if st.Tables == nil {
		return errors.New("state missing required field tables")
	}
	if err := validateConfiguredTables(st, tables); err != nil {
		return err
	}
	for _, table := range tables {
		tableState, ok := st.Tables[table]
		if !ok {
			return errors.Errorf("state missing table entry %q", table)
		}
		if err := validateTableState(table, tableState); err != nil {
			return err
		}
	}
	for table, tableState := range st.Tables {
		if err := validateTableState(table, tableState); err != nil {
			return err
		}
	}
	return nil
}

func validateConfiguredTables(st State, tables []string) error {
	if tables == nil {
		return nil
	}
	configured := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		configured[table] = struct{}{}
	}
	if len(st.Tables) != len(configured) {
		return errors.Errorf("state table set does not match configured tables")
	}
	for table := range st.Tables {
		if _, ok := configured[table]; !ok {
			return errors.Errorf("state contains unconfigured table entry %q", table)
		}
	}
	return nil
}

func validateTableState(table string, tableState TableState) error {
	if tableState.DMLCursors == nil {
		return errors.Errorf("state missing dml_cursors for table %q", table)
	}
	return nil
}

func cloneState(st State) State {
	out := st
	out.Tables = make(map[string]TableState, len(st.Tables))
	for tableName, tableState := range st.Tables {
		cloned := tableState
		cloned.DMLCursors = make(map[string]DMLCursor, len(tableState.DMLCursors))
		for key, cursor := range tableState.DMLCursors {
			cloned.DMLCursors[key] = cursor
		}
		out.Tables[tableName] = cloned
	}
	return out
}

func decodeState(data []byte) (State, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var st State
	if err := dec.Decode(&st); err != nil {
		return State{}, errors.Trace(err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return State{}, errors.New("state file contains trailing JSON data")
	}
	if err := validateState(st, nil); err != nil {
		return State{}, err
	}
	return st, nil
}
