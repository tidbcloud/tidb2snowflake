package state

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/tidbcloud/tidb2snowflake/pkg/common/strconv"
)

const (
	Version  = 1
	FileName = "replication-state.json"
)

type State struct {
	// Version is the on-disk state schema version.
	Version int `json:"version"`
	// TaskInfo records source-side jobs created or reused by the prepare phase.
	TaskInfo TaskInfo `json:"task_info"`
	// Snapshot records the source snapshot artifact and downstream load progress.
	Snapshot SnapshotState `json:"snapshot"`
	// Incremental records the current TiCDC-to-Snowflake load progress.
	Incremental IncrementalState `json:"incremental"`
}

type TaskInfo struct {
	// ExportID is the TiDB Cloud export job to wait for when snapshot export is resumed.
	ExportID string `json:"export_id"`
	// ChangefeedID is the source changefeed job to wait for when incremental export is resumed.
	ChangefeedID string `json:"changefeed_id"`
}

type SnapshotState struct {
	// TSO is the snapshot artifact TSO confirmed after the source snapshot is ready.
	TSO uint64 `json:"tso"`
	// Finished means the snapshot data has been loaded into downstream Snowflake.
	Finished bool `json:"finished"`
}

type IncrementalState struct {
	// CheckpointTS is the latest commit timestamp fully loaded into Snowflake, stored as a decimal string.
	CheckpointTS uint64 `json:"checkpoint_ts"`
	// Scan records an in-progress incremental scan, if the process stopped mid-scan.
	Scan *ScanState `json:"scan,omitempty"`
	// Tables records per-table incremental load watermarks.
	Tables map[string]TableState `json:"tables"`
}

type ScanState struct {
	// HighWatermark is the upper commit timestamp bound selected for the current scan, stored as a decimal string.
	HighWatermark uint64 `json:"high_watermark"`
}

type TableState struct {
	// DMLFileWatermarks records the highest loaded DML file index per table-version scope.
	DMLFileWatermarks map[string]uint64 `json:"dml_file_watermarks"`
	// DDLTableVersionWatermark records the highest applied schema table version, stored as a decimal string.
	DDLTableVersionWatermark uint64 `json:"ddl_table_version_watermark"`
}

func newState(tables []string) State {
	st := State{
		Version: Version,
		Incremental: IncrementalState{
			CheckpointTS: 0,
			Tables:       make(map[string]TableState, len(tables)),
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
				DMLFileWatermarks:        make(map[string]uint64),
				DDLTableVersionWatermark: 0,
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
	if err := validateUint64String("incremental.checkpoint_ts", st.Incremental.CheckpointTS); err != nil {
		return err
	}
	if st.Incremental.Scan != nil {
		if err := validateUint64String("incremental.scan.high_watermark", st.Incremental.Scan.HighWatermark); err != nil {
			return err
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
		if err := validateUint64String(
			"incremental.tables."+table+".ddl_table_version_watermark",
			tableState.DDLTableVersionWatermark,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateUint64String(field string, value uint64) error {
	if value == 0 {
		return errors.Errorf("state missing required field %s", field)
	}
	return nil
}

func validateDMLFileWatermarkScope(scope string) error {
	parts := strings.Split(scope, "/")
	if len(parts) != 4 {
		return errors.Errorf("invalid scope %q", scope)
	}
	strconv.MustParseUint(parts[0], 10, 64)
	strconv.ParseInt(parts[1], 10, 64)
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
