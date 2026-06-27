package state

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

const (
	Version  = 1
	FileName = "replication-state.json"
)

type State struct {
	Version     int              `json:"version"`
	TaskInfo    TaskInfo         `json:"task_info"`
	Snapshot    SnapshotState    `json:"snapshot"`
	Incremental IncrementalState `json:"incremental"`
}

type TaskInfo struct {
	ExportID     string `json:"export_id"`
	ChangefeedID string `json:"changefeed_id"`
}

type SnapshotState struct {
	TSO      string `json:"tso"`
	Finished bool   `json:"finished"`
}

type IncrementalState struct {
	CheckpointTS uint64                `json:"checkpoint_ts"`
	Scan         *ScanState            `json:"scan,omitempty"`
	Tables       map[string]TableState `json:"tables"`
}

type ScanState struct {
	HighWatermark uint64 `json:"high_watermark"`
}

type TableState struct {
	DMLFileWatermarks        map[string]uint64 `json:"dml_file_watermarks"`
	DDLTableVersionWatermark uint64            `json:"ddl_table_version_watermark"`
}

func newState(tables []string) State {
	st := State{
		Version: Version,
		Incremental: IncrementalState{
			Tables: make(map[string]TableState, len(tables)),
		},
	}
	ensureConfiguredTables(&st, tables)
	return st
}

func ensureConfiguredTables(st *State, tables []string) bool {
	changed := false
	if st.Incremental.Tables == nil {
		st.Incremental.Tables = make(map[string]TableState, len(tables))
		changed = true
	}
	for _, table := range tables {
		if _, ok := st.Incremental.Tables[table]; !ok {
			st.Incremental.Tables[table] = TableState{
				DMLFileWatermarks: make(map[string]uint64),
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
	if st.Incremental.Tables == nil {
		return errors.New("state missing required field incremental.tables")
	}
	for _, table := range tables {
		if _, ok := st.Incremental.Tables[table]; !ok {
			return errors.Errorf("state missing incremental table entry %q", table)
		}
	}
	for table, tableState := range st.Incremental.Tables {
		if tableState.DMLFileWatermarks == nil {
			return errors.Errorf("state missing required field incremental.tables.%s.dml_file_watermarks", table)
		}
		for scope := range tableState.DMLFileWatermarks {
			if err := validateDMLFileWatermarkScope(scope); err != nil {
				return errors.Annotatef(err, "invalid dml_file_watermarks scope for table %s", table)
			}
		}
	}
	return nil
}

func validateDMLFileWatermarkScope(scope string) error {
	parts := strings.Split(scope, "/")
	if len(parts) != 4 {
		return errors.Errorf("invalid scope %q", scope)
	}
	if _, err := strconv.ParseUint(parts[0], 10, 64); err != nil {
		return errors.Trace(err)
	}
	if _, err := strconv.ParseInt(parts[1], 10, 64); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func cloneState(st State) State {
	out := st
	if st.Incremental.Scan != nil {
		scan := *st.Incremental.Scan
		out.Incremental.Scan = &scan
	}
	out.Incremental.Tables = make(map[string]TableState, len(st.Incremental.Tables))
	for table, tableState := range st.Incremental.Tables {
		copiedTable := tableState
		copiedTable.DMLFileWatermarks = make(map[string]uint64, len(tableState.DMLFileWatermarks))
		for scope, idx := range tableState.DMLFileWatermarks {
			copiedTable.DMLFileWatermarks[scope] = idx
		}
		out.Incremental.Tables[table] = copiedTable
	}
	return out
}

type rawState struct {
	Version     *int            `json:"version"`
	TaskInfo    *rawTaskInfo    `json:"task_info"`
	Snapshot    *rawSnapshot    `json:"snapshot"`
	Incremental *rawIncremental `json:"incremental"`
}

type rawTaskInfo struct {
	ExportID     *string `json:"export_id"`
	ChangefeedID *string `json:"changefeed_id"`
}

type rawSnapshot struct {
	TSO      *string `json:"tso"`
	Finished *bool   `json:"finished"`
}

type rawIncremental struct {
	CheckpointTS *uint64                   `json:"checkpoint_ts"`
	Scan         *rawScan                  `json:"scan"`
	Tables       *map[string]rawTableState `json:"tables"`
}

type rawScan struct {
	HighWatermark *uint64 `json:"high_watermark"`
}

type rawTableState struct {
	DMLFileWatermarks        *map[string]uint64 `json:"dml_file_watermarks"`
	DDLTableVersionWatermark *uint64            `json:"ddl_table_version_watermark"`
}

func decodeState(data []byte) (State, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var raw rawState
	if err := dec.Decode(&raw); err != nil {
		return State{}, errors.Trace(err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return State{}, errors.New("state file contains trailing JSON data")
	}

	if raw.Version == nil {
		return State{}, errors.New("state missing required field version")
	}
	if raw.TaskInfo == nil {
		return State{}, errors.New("state missing required field task_info")
	}
	if raw.TaskInfo.ExportID == nil {
		return State{}, errors.New("state missing required field task_info.export_id")
	}
	if raw.TaskInfo.ChangefeedID == nil {
		return State{}, errors.New("state missing required field task_info.changefeed_id")
	}
	if raw.Snapshot == nil {
		return State{}, errors.New("state missing required field snapshot")
	}
	if raw.Snapshot.TSO == nil {
		return State{}, errors.New("state missing required field snapshot.tso")
	}
	if raw.Snapshot.Finished == nil {
		return State{}, errors.New("state missing required field snapshot.finished")
	}
	if raw.Incremental == nil {
		return State{}, errors.New("state missing required field incremental")
	}
	if raw.Incremental.CheckpointTS == nil {
		return State{}, errors.New("state missing required field incremental.checkpoint_ts")
	}
	if raw.Incremental.Scan != nil && raw.Incremental.Scan.HighWatermark == nil {
		return State{}, errors.New("state missing required field incremental.scan.high_watermark")
	}
	if raw.Incremental.Tables == nil {
		return State{}, errors.New("state missing required field incremental.tables")
	}

	st := State{
		Version: *raw.Version,
		TaskInfo: TaskInfo{
			ExportID:     *raw.TaskInfo.ExportID,
			ChangefeedID: *raw.TaskInfo.ChangefeedID,
		},
		Snapshot: SnapshotState{
			TSO:      *raw.Snapshot.TSO,
			Finished: *raw.Snapshot.Finished,
		},
		Incremental: IncrementalState{
			CheckpointTS: *raw.Incremental.CheckpointTS,
			Tables:       make(map[string]TableState, len(*raw.Incremental.Tables)),
		},
	}
	if raw.Incremental.Scan != nil {
		st.Incremental.Scan = &ScanState{HighWatermark: *raw.Incremental.Scan.HighWatermark}
	}
	for table, tableState := range *raw.Incremental.Tables {
		if tableState.DMLFileWatermarks == nil {
			return State{}, errors.Errorf("state missing required field incremental.tables.%s.dml_file_watermarks", table)
		}
		if tableState.DDLTableVersionWatermark == nil {
			return State{}, errors.Errorf("state missing required field incremental.tables.%s.ddl_table_version_watermark", table)
		}
		watermarks := make(map[string]uint64, len(*tableState.DMLFileWatermarks))
		for scope, idx := range *tableState.DMLFileWatermarks {
			watermarks[scope] = idx
		}
		st.Incremental.Tables[table] = TableState{
			DMLFileWatermarks:        watermarks,
			DDLTableVersionWatermark: *tableState.DDLTableVersionWatermark,
		}
	}
	if err := validateState(st, nil); err != nil {
		return State{}, err
	}
	return st, nil
}
